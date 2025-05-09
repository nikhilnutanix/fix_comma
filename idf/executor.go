package idf

import (
	"github.com/golang/glog"
	"github.com/nutanix-core/go-cache/insights/insights_interface"
	"github.com/nutanix-core/go-cache/util-go/net"
	"fix_comma/idf/query"
	"fix_comma/types/idf"
	"fix_comma/util"
)

// IDF Query Executor equivalent
// Wrapper methods with retries and backoff for create, update
// delete, batch IDF calls

type IdfExecutor struct {
	RPC *insights_interface.InsightsRpcClient
}

func NewIdfExecutor() idf.IIdfExecutor {
	return &IdfExecutor{RPC: &insights_interface.InsightsRpcClient{Impl: net.NewProtobufRPCClient(
		*insights_interface.DefaultInsightAddr, uint16(*insights_interface.DefaultInsightPort))}}
}

func (e *IdfExecutor) GetEntitiesWithMetrics(arg *insights_interface.GetEntitiesWithMetricsArg) (*insights_interface.GetEntitiesWithMetricsRet, error) {
	return util.ExecuteQueryWithRetry(arg, e.RPC.GetEntitiesWithMetrics)
}

func (e *IdfExecutor) GetEntities(args *insights_interface.GetEntitiesArg) (*insights_interface.GetEntitiesRet, error) {
	return util.ExecuteQueryWithRetry(args, e.RPC.GetEntities)
}

func (e *IdfExecutor) UpdateEntity(arg *insights_interface.UpdateEntityArg) (*insights_interface.UpdateEntityRet, error) {
	return util.ExecuteQueryWithRetry(arg, e.RPC.UpdateEntity)
}

func (e *IdfExecutor) DeleteEntity(arg *insights_interface.DeleteEntityArg) (*insights_interface.DeleteEntityRet, error) {
	return util.ExecuteQueryWithRetry(arg, e.RPC.DeleteEntity)
}

func (e *IdfExecutor) BatchUpdateEntities(arg *insights_interface.BatchUpdateEntitiesArg) (*insights_interface.BatchUpdateEntitiesRet, error) {
	batchArgs := arg
	return util.ExecuteQueryWithRetry(batchArgs, func(i *insights_interface.BatchUpdateEntitiesArg) (*insights_interface.BatchUpdateEntitiesRet, error) {
		response, err := e.RPC.BatchUpdateEntities(batchArgs)
		if err == nil {
			return response, nil
		}
		batchArgs = getRetriableBatchArgs(batchArgs, response)
		return response, err
	})
}

func (e *IdfExecutor) BatchGetEntities(arg *insights_interface.BatchGetEntitiesWithMetricsArg) (*insights_interface.BatchGetEntitiesWithMetricsRet, error) {
	batchArgs := arg
	return util.ExecuteQueryWithRetry(batchArgs, func(i *insights_interface.BatchGetEntitiesWithMetricsArg) (*insights_interface.BatchGetEntitiesWithMetricsRet, error) {
		response, err := e.RPC.BatchGetEntitiesWithMetrics(batchArgs)
		if err == nil {
			return response, nil
		}
		batchArgs = getRetriableBatchArgs(batchArgs, response)
		return response, err
	})
}

func (e *IdfExecutor) BatchDeleteEntities(arg *insights_interface.BatchDeleteEntitiesArg) (*insights_interface.BatchDeleteEntitiesRet, error) {
	batchArgs := arg
	return util.ExecuteQueryWithRetry(batchArgs, func(i *insights_interface.BatchDeleteEntitiesArg) (*insights_interface.BatchDeleteEntitiesRet, error) {
		response, err := e.RPC.BatchDeleteEntities(batchArgs)
		if err == nil {
			return response, nil
		}
		batchArgs = getRetriableBatchArgs(batchArgs, response)
		return response, err
	})
}

func (e *IdfExecutor) ExecuteWithCursor(args *insights_interface.GetEntitiesWithMetricsArg) ([]*insights_interface.Entity, error) {
	initialCursor := true
	notInitialCursor := false
	readSize := uint32(util.BatchQueryItemsLimit)
	args.GetQuery().CursorQueryInfo = &insights_interface.Query_CursorQueryInfo{IsInitialCursorQuery: &initialCursor, BatchSize: &readSize}

	ret, err := e.GetEntitiesWithMetrics(args)

	if err != nil {
		glog.Errorf("Error in GetEntitiesWithMetrics while fetching entities:%v", err)
		return nil, err
	}
	retList := query.EntitiesWithMetricsRetToEntityList(ret)

	args.Query.CursorQueryInfo.IsInitialCursorQuery = &notInitialCursor
	for ret.GetNextCursor() != nil && ret.GetNextCursor().GetChunksRemaining() {
		args.GetQuery().GetCursorQueryInfo().NextCursor = ret.GetNextCursor()
		ret, err = e.GetEntitiesWithMetrics(args)

		if err != nil {
			glog.Errorf("Error in GetEntitiesWithMetrics while fetching entities:%v", err)
			return nil, err
		}
		retList = append(retList, query.EntitiesWithMetricsRetToEntityList(ret)...)
	}
	return retList, nil
}

//	 getRetriableBatchArgs processes the response to get the indices of the failed args and
//	 then generates new retriable batch arguments
//	 for .
//	 Type Parameters:
//
//		T - The type of the argument.
//		S - The type of the response.
//
//	 Parameters:
//
//		arg - A pointer to the initial argument of type T.
//		response - A pointer to the response of type S.
//
//	 Returns:
//
//		A pointer to a new argument of type T that contains the retriable batch arguments.
func getRetriableBatchArgs[T any, S any](arg *T, response *S) *T {
	retryEntitiesIndex := getBatchRetryIndices(response)
	glog.V(1).Infof("retry entities index %v", retryEntitiesIndex)
	arg = getNewBatchArgs(arg, retryEntitiesIndex)
	glog.V(1).Infof("retriable batch args %v", arg)
	return arg
}

//	 getNewBatchArgs processes a batch argument and filters its entities based on the provided retry indices.
//	 It supports different types of batch arguments, including BatchUpdateEntitiesArg, BatchDeleteEntitiesArg,
//	 and BatchGetEntitiesWithMetricsArg. The function returns a new batch argument with only the entities
//	 specified by the retry indices.
//
//	 Type Parameters:
//
//		T: The type of the batch argument.
//
//	 Parameters:
//
//		arg: A pointer to the batch argument to be processed.
//		retryEntitiesIndex: A slice of indices specifying which entities to retry.
//
//	 Returns:
//
//		A pointer to the new batch argument with filtered entities.
func getNewBatchArgs[T any](arg *T, retryEntitiesIndex []int) *T {
	switch v := any(arg).(type) {
	case *insights_interface.BatchUpdateEntitiesArg:
		v.EntityList = getEntitiesInIndex(v.EntityList, retryEntitiesIndex)
	case *insights_interface.BatchDeleteEntitiesArg:
		v.EntityList = getEntitiesInIndex(v.EntityList, retryEntitiesIndex)
	case *insights_interface.BatchGetEntitiesWithMetricsArg:
		v.QueryList = getEntitiesInIndex(v.QueryList, retryEntitiesIndex)
	}
	return arg
}

// getEntitiesInIndex processes a slice of entities and returns a new slice containing only the entities
// specified by the provided indices. The function supports different types of entities, including pointers
// to Entity, EntityWithMetrics, and any other type.
//
// Type Parameters:
//
// 	T: The type of the entities in the slice.
//
// Parameters:
//
// 	entities: A slice of entities to be processed.
// 	retryEntitiesIndex: A slice of indices specifying which entities to return.
//
// Returns:
//
// 	A new slice of entities containing only the entities specified by the retry indices.

func getEntitiesInIndex[T any](entities []*T, retryEntitiesIndex []int) []*T {
	var retryEntities []*T
	for i := range entities {
		if len(retryEntitiesIndex) > 0 && i == retryEntitiesIndex[0] {
			retryEntities = append(retryEntities, entities[i])
			retryEntitiesIndex = retryEntitiesIndex[1:]
		}
	}
	return retryEntities
}

//	 getBatchRetryIndices processes a response of a generic type and returns a slice of indices
//	 where the status of the entities in the response indicates an error. The function supports
//	 three types of responses: BatchUpdateEntitiesRet, BatchDeleteEntitiesRet, and
//	 BatchGetEntitiesWithMetricsRet.
//
//	 Type Parameters:
//
//		T: The type of the response, which can be any type.
//
//	 Parameters:
//
//		response (T): The response object containing the entities to be checked for errors.
//
//	 Returns:
//
//		[]int: A slice of indices where the status of the entities in the response indicates an error.
func getBatchRetryIndices[T any](response T) []int {
	var retryEntitiesIndex []int
	switch resp := any(response).(type) {
	case *insights_interface.BatchUpdateEntitiesRet:
		retryEntitiesIndex = getRetryEntityIndices(resp.RetList)
	case *insights_interface.BatchDeleteEntitiesRet:
		retryEntitiesIndex = getRetryEntityIndices(resp.RetList)
	case *insights_interface.BatchGetEntitiesWithMetricsRet:
		retryEntitiesIndex = getRetryEntityIndices(resp.QueryRetList)
	}
	return retryEntitiesIndex
}

func getRetryEntityIndices[T interface {
	GetStatus() *insights_interface.InsightsErrorProto
}](entities []T) []int {
	var retryEntitiesIndex []int
	for i, elem := range entities {
		if isStatusError(elem.GetStatus()) {
			retryEntitiesIndex = append(retryEntitiesIndex, i)
		}
	}
	return retryEntitiesIndex
}

// isStatusError checks if the given status represents an error.
// It returns true if the status is not nil, the ErrorType is not nil,
// and the ErrorType is not equal to InsightsErrorProto_kNoError.
func isStatusError(status *insights_interface.InsightsErrorProto) bool {
	return status != nil && status.ErrorType != nil && status.ErrorType != insights_interface.InsightsErrorProto_kNoError.Enum()
}
