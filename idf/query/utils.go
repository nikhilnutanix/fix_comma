package query

// All utility functions required for data-sync service
// Corresponds to IdfQueryUtils, IDFRPCUtils

// Use functions, types, or variables from the imported package

import (
	"fmt"
	"reflect"
	"strings"

	"runtime"

	"fix_comma/rpc/categoriesproto"
	idf "fix_comma/types/idf"
	"fix_comma/util"
	mapset "github.com/deckarep/golang-set"
	"github.com/golang/glog"
	"github.com/nutanix-core/go-cache/insights/insights_interface"
	"github.com/nutanix-core/go-cache/util-go/uuid4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	idListReadBatchSize     int64 = 10000
	createdBySortColumnName       = "_created_timestamp_usecs_"
)

// ReduceBooleanExpressions reduces the list of boolean expressions to a single boolean expression
//
// Parameters:
//   - expressions: []insights_interface.BooleanExpression
//   - operator: insights_interface.BooleanExpression_Operator
//
// Returns:
//   - insights_interface.BooleanExpression
func ReduceBooleanExpressions(expressions []insights_interface.BooleanExpression, operator insights_interface.BooleanExpression_Operator) (insights_interface.BooleanExpression, error) {
	// create an empty list of boolean expressions
	var temp []insights_interface.BooleanExpression
	// loop until size of expressions is greater than 1
	for len(expressions) > 1 {
		size := len(expressions)
		// clear the temp list
		temp = temp[:0]
		for i := 0; i < size-1; i += 2 {
			boolExpr := BuildBooleanExpression(expressions[i], expressions[i+1], operator)
			// append the new boolean expression to temp
			temp = append(temp, boolExpr)
		}
		// if size is odd, append the last element to temp
		if size%2 == 1 {
			temp = append(temp, expressions[size-1])
		}
		// set expressions to temp
		expressions = temp
	}
	if len(expressions) == 0 {
		glog.Errorf("No expressions found")
		return insights_interface.BooleanExpression{}, fmt.Errorf("no expressions found")
	}
	// return the first element of expressions
	return expressions[0], nil
}

// BuildBooleanExpression builds a boolean expression from the given lhs, rhs and operator
//
// Parameters:
//   - lhs: insights_interface.BooleanExpression
//   - rhs: insights_interface.BooleanExpression
//   - operator: insights_interface.BooleanExpression_Operator
//
// Returns:
//   - insights_interface.BooleanExpression
func BuildBooleanExpression(lhs insights_interface.BooleanExpression, rhs insights_interface.BooleanExpression, operator insights_interface.BooleanExpression_Operator) insights_interface.BooleanExpression {
	return insights_interface.BooleanExpression{
		Operator: &operator,
		Lhs:      &lhs,
		Rhs:      &rhs,
	}
}

/**
 * Filter expression for registering watches based on specific attributes.
 * Whenever there is change in any attr value we want the watch to fire so here
 * we are creating filter with kChanged operator for individual attr and
 * kOr for indicating any/all of attr change should be watched.
 *
 */
// BuildFilterExpression builds a filter expression from the given attributes
//
// Parameters:
//   - attrs: []string
//
// Returns:
//   - insights_interface.BooleanExpression
func BuildFilterExpression(attrs []string) (insights_interface.BooleanExpression, error) {
	// if attrs is empty, return nil
	if len(attrs) == 0 {
		glog.Warning("No attributes provided for watch")
		return insights_interface.BooleanExpression{}, nil
	}
	var booleanExpressionList = make([]insights_interface.BooleanExpression, len(attrs))
	for i, attr := range attrs {
		CompExpWithChangedOp := GetCompExpWithChangedOp(attr)
		booleanExpr := insights_interface.BooleanExpression{
			ComparisonExpr: &CompExpWithChangedOp,
		}
		booleanExpressionList[i] = booleanExpr
	}
	ReduceBooleanExpressions, err := ReduceBooleanExpressions(booleanExpressionList, insights_interface.BooleanExpression_kOr)
	if err != nil {
		glog.Errorf("Error while reducing boolean expressions: %v", err)
		return insights_interface.BooleanExpression{}, err
	}
	return ReduceBooleanExpressions, nil
}

// GetCompExpWithChangedOp returns a comparison expression with kChanged operator for the given attribute
//
// Parameters:
//   - attr: string
//
// Returns:
//   - insights_interface.ComparisonExpression
func GetCompExpWithChangedOp(attr string) insights_interface.ComparisonExpression {
	temp := insights_interface.ComparisonExpression_kChanged
	compExp := insights_interface.ComparisonExpression{
		Lhs: &insights_interface.Expression{
			Leaf: &insights_interface.LeafExpression{
				Column: &attr,
			},
		},
		Operator: &temp,
	}

	return compExp
}

// GetDataValue returns the data value for the given value
//
// Parameters:
//   - value: interface{}
//
// Returns:
func GetDataValue(value interface{}) *insights_interface.DataValue {
	return CheckValueTypeAndSet(value)
}

// CheckValueTypeAndSet checks the type of value and sets the value to the builder
//
// Parameters:
//   - filterValue: interface{}
//
// Returns:
//   - insights_interface.DataValue
func CheckValueTypeAndSet(filterValue interface{}) *insights_interface.DataValue {
	builder := &insights_interface.DataValue{}
	// check the type of value
	// get simple name of the type
	var filterValueType = reflect.TypeOf(filterValue).String()
	switch filterValueType {
	case "int64":
		builder.ValueType = &insights_interface.DataValue_Int64Value{
			Int64Value: filterValue.(int64),
		}
		return builder
	case "bool":
		builder.ValueType = &insights_interface.DataValue_BoolValue{
			BoolValue: filterValue.(bool),
		}
		return builder
	case "uint64":
		builder.ValueType = &insights_interface.DataValue_Uint64Value{
			Uint64Value: filterValue.(uint64),
		}
		return builder
	case "string":
		value := filterValue.(string)
		builder.ValueType = &insights_interface.DataValue_StrValue{
			StrValue: value,
		}
		// singleton list
	case "[]string":
		arrayList := filterValue.([]string)
		// if arraylist is non-empty
		if len(arrayList) > 0 {
			builder.ValueType = &insights_interface.DataValue_StrList_{
				StrList: &insights_interface.DataValue_StrList{
					ValueList: arrayList,
				},
			}
		}
	case "[]int64":
		arrayList := filterValue.([]int64)
		// if arraylist is non-empty
		if len(arrayList) > 0 {
			builder.ValueType = &insights_interface.DataValue_Int64List_{
				Int64List: &insights_interface.DataValue_Int64List{
					ValueList: arrayList,
				},
			}
		}
	case "[]byte", "[]uint8":
		builder.ValueType = &insights_interface.DataValue_BytesValue{
			BytesValue: filterValue.([]byte),
		}

	default:
		glog.Warning("Unsupported type for filter value")
	}
	return builder
}

// GetBuilderForValue returns the attribute data arg for the given value and attribute
//
// Parameters:
//   - value: insights_interface.DataValue
//   - attr: string
//
// Returns:
//   - insights_interface.AttributeDataArg
func GetBuilderForValue(value *insights_interface.DataValue, attr string) insights_interface.AttributeDataArg {
	builder := insights_interface.AttributeDataArg{
		AttributeData: &insights_interface.AttributeData{
			Name:  &attr,
			Value: value,
		},
	}
	return builder
}

// GetEntityGuid returns the entity guid for the given entity type and entity id
//
// Parameters:
//   - entityType: string
//   - entityID: string
//
// Returns:
//   - insights_interface.EntityGuid
func GetEntityGUID(entityType string, entityID string) *insights_interface.EntityGuid {
	return &insights_interface.EntityGuid{
		EntityTypeName: &entityType,
		EntityId:       &entityID,
	}
}

// GetAttrToDataMap returns the map of attribute to data value for the given entity
//
// Parameters:
//   - current: insights_interface.Entity
//
// Returns:
//   - map[string]insights_interface.DataValue
func GetAttrToDataMap(current *insights_interface.Entity) map[string]insights_interface.DataValue {
	attrToDataMap := make(map[string]insights_interface.DataValue)
	for _, attrData := range current.GetAttributeDataMap() {
		attrToDataMap[attrData.GetName()] = *attrData.GetValue()
	}
	return attrToDataMap
}

// GetStringFieldValue returns the string field value for the given field name and fields
//
// Parameters:
//   - fieldName: string
//   - fields: []insights_interface.NameTimeValuePair
//
// Returns:
//   - string
func GetStringFieldValue(fieldName string, fields []insights_interface.NameTimeValuePair) string {
	for _, pair := range fields {
		if pair.GetName() == fieldName {
			return pair.GetValue().GetStrValue()
		}
	}
	glog.V(2).Infof("Field %v not found in fields", fieldName)
	return ""
}

func GetStringListFieldValueFromEntity(entity *insights_interface.Entity, fieldName string) []string {
	attrsMap := GetAttrToDataMap(entity)
	if dataVal, ok := attrsMap[fieldName]; ok {
		return dataVal.GetStrList().GetValueList()
	}
	glog.V(2).Infof("Field %v not found in entities", fieldName)
	return make([]string, 0)
}

// GetQueryGroupByBuilder returns the query group by builder for the given projection attributes
//
// Parameters:
//   - projectionAttrs: []string
//
// Returns:
//   - insights_interface.QueryGroupBy
func GetQueryGroupByBuilder(projectionAttrs []string) insights_interface.QueryGroupBy {
	queryGroupByBuilder := insights_interface.QueryGroupBy{}
	for _, attr := range projectionAttrs {
		val := attr
		queryRawColumn := insights_interface.QueryRawColumn{
			Column: &val,
		}
		queryGroupByBuilder.RawColumns = append(queryGroupByBuilder.RawColumns, &queryRawColumn)
	}
	return queryGroupByBuilder
}

// GetGroupBy returns the group by for the given kind id
//
// Parameters:
//   - kindID: int32
//
// Returns:
//   - insights_interface.QueryGroupBy
func GetGroupBy() insights_interface.QueryGroupBy {
	sortOrder := insights_interface.QueryOrderBy_kDescending
	return insights_interface.QueryGroupBy{
		RawColumns: []*insights_interface.QueryRawColumn{
			{
				Column: &createdBySortColumnName,
			},
		},
		RawSortOrder: &insights_interface.QueryOrderBy{
			SortColumn: &createdBySortColumnName,
			SortOrder:  &sortOrder,
		},
		RawLimit: &insights_interface.QueryLimit{
			Limit: &idListReadBatchSize,
		},
	}
}

// EntitiesWithMetricsRetToEntityList returns the entity list for the given entities
//
// Parameters:
//   - GetEntitiesWithMetricsRet: insights_interface.GetEntitiesWithMetricsRet
//
// Returns:
//   - []insights_interface.Entity
func EntitiesWithMetricsRetToEntityList(ret *insights_interface.GetEntitiesWithMetricsRet) []*insights_interface.Entity {
	entityList := make([]*insights_interface.Entity, 0)
	if len(ret.GetGroupResultsList()) != 0 {
		queryGroupResultList := ret.GetGroupResultsList()[0]
		rawResultsList := queryGroupResultList.GetRawResults()
		for _, entityWithMetric := range rawResultsList {
			entity := &insights_interface.Entity{}
			entity.EntityGuid = entityWithMetric.GetEntityGuid()
			metricDataList := entityWithMetric.GetMetricDataList()
			for _, metricData := range metricDataList {
				if len(metricData.GetValueList()) != 0 {
					ntvpBuilder := &insights_interface.NameTimeValuePair{}
					name := metricData.GetName()
					ntvpBuilder.Name = &name
					value := metricData.GetValueList()[0].GetValue()
					ntvpBuilder.Value = value
					entity.AttributeDataMap = append(entity.AttributeDataMap, ntvpBuilder)
				}
			}
			entityList = append(entityList, entity)
		}
	} else {
		glog.V(2).Infof("GetGroupResultsList is empty")
	}
	return entityList
}

// GetKindID returns the kind id for the given entity
//
// Parameters:
//   - entity: insights_interface.Entity
//   - entityType: string
//
// Returns:
//   - string
func GetKindID(entity *insights_interface.Entity, entityType string) string {
	var attrMap []insights_interface.NameTimeValuePair
	for _, attributeData := range entity.GetAttributeDataMap() {
		attrMap = append(attrMap, *attributeData)
	}
	switch entityType {
	case util.AbacEntityCapabilityType, util.VolumeGroupEntityCapabilityType:
		return GetStringFieldValue(util.KindID, attrMap)
	case util.FilterType:
		return GetStringFieldValue(util.EntityUUID, attrMap)
	default:
		return entity.GetEntityGuid().GetEntityId()
	}
}

/*
equivalent of GetKindID, this is for 'metric' results
*/
func GetKindIDMetrics(ewm *insights_interface.EntityWithMetric, entityType string) string {
	switch entityType {
	case util.AbacEntityCapabilityType, util.VolumeGroupEntityCapabilityType:
		kindID, _ := ewm.GetString(util.KindID)
		return kindID
	case util.FilterType:
		kindID, _ := ewm.GetString(util.EntityUUID)
		return kindID
	default:
		return ewm.GetEntityGuid().GetEntityId()
	}
}

func GetKind(entity *insights_interface.Entity) string {
	entityType := entity.GetEntityGuid().GetEntityTypeName()

	if entityType == util.VMHostAffinityPolicy {
		return util.VMHostAffinityPolicy
	}
	if entityType == util.VMAntiAffinityPolicyType {
		return util.VMAntiAffinityPolicy
	}
	if entityType == util.VolumeGroupEntityCapability {
		return util.VolumeGroupKind
	}
	if entityType == util.AbacEntityCapabilityType {
		kind, _ := entity.GetString(util.Kind)
		return kind
	}
	if entityType == util.FilterType {
		kind, _ := entity.GetString(util.EntityKind)
		return kind
	}
	if entityType == util.ContentPlacementPolicyType {
		return util.TemplatePlacementPolicy
	}
	glog.Warningf("unhandled type of entity to determine kind: %v", entityType)
	return ""
}

/*
equivalent of GetKind, this is for 'metric' result
*/
func GetKindMetrics(ewm *insights_interface.EntityWithMetric) string {
	entityType := ewm.GetEntityGuid().GetEntityTypeName()

	if entityType == util.VMHostAffinityPolicy {
		return util.VMHostAffinityPolicy
	}
	if entityType == util.VMAntiAffinityPolicyType {
		return util.VMAntiAffinityPolicy
	}
	if entityType == util.VolumeGroupEntityCapability {
		return util.VolumeGroupKind
	}
	if entityType == util.AbacEntityCapabilityType {
		kind, _ := ewm.GetString(util.Kind)
		return kind
	}
	if entityType == util.FilterType {
		kind, _ := ewm.GetString(util.EntityKind)
		return kind
	}
	if entityType == util.ContentPlacementPolicyType {
		return util.TemplatePlacementPolicy
	}
	glog.Warningf("unhandled type of entity to determine kind: %s", entityType)
	return ""
}

func GetKindAlias(kind string) string {
	if alias, ok := util.KindAlias[kind]; ok {
		return alias
	}
	return kind
}

func GetCategories(entity *insights_interface.Entity) []string {
	entityType := entity.GetEntityGuid().GetEntityTypeName()
	switch entityType {
	case util.VMHostAffinityPolicy:
		categories := GetStringListFieldValueFromEntity(entity, util.VMCategoryUuids)
		return append(categories, GetStringListFieldValueFromEntity(entity, util.HostCategoryUuids)...)
	case util.VMAntiAffinityPolicyType:
		return GetStringListFieldValueFromEntity(entity, util.CategoryUuids)
	case util.ContentPlacementPolicyType:
		categories := GetStringListFieldValueFromEntity(entity, util.ContentFilterCategoryExtIds)
		return append(categories, GetStringListFieldValueFromEntity(entity, util.ClusterFilterCategoryExtIds)...)
	case util.Filter:
		categories := GetStringListFieldValueFromEntity(entity, util.FilterExpRhsEntityUuids)
		categoriesProper := make([]string, 0)
		for _, category := range categories {
			categoriesProper = append(categoriesProper, strings.Split(category, ":")...)
		}
		return categoriesProper
	default:
		return GetStringListFieldValueFromEntity(entity, util.CategoryIDList)
	}
}

/*
equivalent of GetCategories, this is for 'metric' result
*/
func GetCategoriesMetrics(ewm *insights_interface.EntityWithMetric) []string {
	entityType := ewm.GetEntityGuid().GetEntityTypeName()
	switch entityType {
	case util.VMHostAffinityPolicy:
		vmCategories, _ := ewm.GetStringList(util.VMCategoryUuids)
		hostCategories, _ := ewm.GetStringList(util.HostCategoryUuids)
		return append(vmCategories, hostCategories...)
	case util.VMAntiAffinityPolicyType:
		categories, _ := ewm.GetStringList(util.CategoryUuids)
		return categories
	case util.ContentPlacementPolicyType:
		contentFilterCategories, _ := ewm.GetStringList(util.ContentFilterCategoryExtIds)
		clusterFilterCategories, _ := ewm.GetStringList(util.ClusterFilterCategoryExtIds)
		return append(contentFilterCategories, clusterFilterCategories...)
	case util.Filter:
		categories, _ := ewm.GetStringList(util.FilterExpRhsEntityUuids)
		categoriesProper := make([]string, 0)
		for _, category := range categories {
			categoriesProper = append(categoriesProper, strings.Split(category, ":")...)
		}
		return categoriesProper
	default:
		categories, _ := ewm.GetStringList(util.CategoryIDList)
		return categories
	}
}

func MakeQueryMetricsEntityID(entityType string, entityID string, attrs []string) *insights_interface.GetEntitiesWithMetricsArg {
	return &insights_interface.GetEntitiesWithMetricsArg{Query: &insights_interface.Query{
		EntityList: []*insights_interface.EntityGuid{GetEntityGUID(entityType, entityID)},
		GroupBy:    &insights_interface.QueryGroupBy{RawColumns: MakeRawColumns(attrs)},
	}}
}

func AbacCategoryKeyByName(keyName string) *insights_interface.GetEntitiesWithMetricsArg {
	whereClause := MakeCondition(util.NameAttribute, insights_interface.ComparisonExpression_kEQ, &keyName)
	return &insights_interface.GetEntitiesWithMetricsArg{
		Query: &insights_interface.Query{
			EntityList: []*insights_interface.EntityGuid{
				{
					EntityTypeName: proto.String(util.AbacCategoryKeyAttribute),
				},
			},
			WhereClause: whereClause,
			GroupBy: &insights_interface.QueryGroupBy{
				RawColumns: MakeRawColumns([]string{util.UUIDAttribute, util.NameAttribute}),
			},
		},
	}
}

func AbacCategoryByKeyUUIDAndValue(keyUUID string, value string) *insights_interface.GetEntitiesWithMetricsArg {
	whereClause, _ := ReduceBooleanExpressions(
		[]insights_interface.BooleanExpression{
			*MakeCondition(util.ValueAttribute, insights_interface.ComparisonExpression_kEQ, &value),
			*MakeCondition(util.AbacCategoryKeyType, insights_interface.ComparisonExpression_kEQ, &keyUUID),
		},
		insights_interface.BooleanExpression_kAnd,
	)

	return &insights_interface.GetEntitiesWithMetricsArg{
		Query: &insights_interface.Query{
			EntityList: []*insights_interface.EntityGuid{
				{EntityTypeName: proto.String(util.AbacCategoryType)},
			},
			WhereClause: &whereClause,
			GroupBy: &insights_interface.QueryGroupBy{
				RawColumns: MakeRawColumns([]string{util.UUIDAttribute}),
			},
		},
	}
}

func MakeQueryMetrics(entityType string, attrs []string) *insights_interface.GetEntitiesWithMetricsArg {
	return &insights_interface.GetEntitiesWithMetricsArg{Query: &insights_interface.Query{
		EntityList: []*insights_interface.EntityGuid{{EntityTypeName: &entityType}},
		GroupBy:    &insights_interface.QueryGroupBy{RawColumns: MakeRawColumns(attrs)},
	}}
}

func MakeQueryMetricsEntityIDSimple(entityType string, entityID string) *insights_interface.GetEntitiesArg {
	metaData := true
	return &insights_interface.GetEntitiesArg{
		EntityGuidList: []*insights_interface.EntityGuid{GetEntityGUID(entityType, entityID)},
		MetaDataOnly:   &metaData,
	}
}

func MakeQueryMetricsFetchAll(entityType string, offset int64, length int64) *insights_interface.GetEntitiesWithMetricsArg {
	createdAttr := util.Created
	return &insights_interface.GetEntitiesWithMetricsArg{Query: &insights_interface.Query{
		EntityList: []*insights_interface.EntityGuid{{EntityTypeName: &entityType}},
		GroupBy: &insights_interface.QueryGroupBy{
			RawColumns: MakeRawColumns([]string{util.Created}),
			RawLimit: &insights_interface.QueryLimit{
				Limit: &length, Offset: &offset,
			},
			RawSortOrder: &insights_interface.QueryOrderBy{SortColumn: &createdAttr},
		},
	}}
}

func GetRetLength(ret *insights_interface.GetEntitiesWithMetricsRet) int64 {
	if *ret.TotalGroupCount == 0 {
		glog.V(2).Infof("GetRetLength: TotalGroupCount is 0")
		return 0
	}
	groupResultsList := ret.GetGroupResultsList()
	if len(groupResultsList) == 0 {
		glog.V(2).Infof("GetRetLength: groupResultsList is empty")
		return 0
	}
	return groupResultsList[0].GetTotalEntityCount()
}

func MakeRawColumns(attrs []string) []*insights_interface.QueryRawColumn {
	rawColumns := make([]*insights_interface.QueryRawColumn, 0)
	for _, attr := range attrs {
		attrCopy := attr
		queryRawColumn := &insights_interface.QueryRawColumn{Column: &attrCopy}
		rawColumns = append(rawColumns, queryRawColumn)
	}
	return rawColumns
}

func MakeDeleteQuery(table string, id string) *insights_interface.DeleteEntityArg {
	arg := &insights_interface.DeleteEntityArg{}
	arg.EntityGuid = GetEntityGUID(table, id)
	return arg
}

func MakeCountsFetchArg(categoryIds []string, kind string) *insights_interface.GetEntitiesWithMetricsArg {
	arg := &insights_interface.GetEntitiesWithMetricsArg{Query: &insights_interface.Query{
		EntityList: make([]*insights_interface.EntityGuid, len(categoryIds)),
		GroupBy:    &insights_interface.QueryGroupBy{RawColumns: MakeRawColumns([]string{util.CategoryIDAttribute, util.CountAttribute})}}}

	for i := 0; i < len(categoryIds); i++ {
		rowID := GetCountsUUID(categoryIds[i], kind)
		arg.Query.EntityList[i] = GetEntityGUID(util.CategoryCountsIdfEntity, rowID)
	}
	return arg
}

func MakeCountsUpdateArg(categoryID string, kind string, count int64) *insights_interface.UpdateEntityArg {
	arg := &insights_interface.UpdateEntityArg{}

	uuid := GetCountsUUID(categoryID, kind)
	arg.EntityGuid = GetEntityGUID(util.CategoryCountsIdfEntity, uuid)
	AattributeDataArgList := MakeAttributeDataArgList(map[string]interface{}{
		util.CategoryIDAttribute: categoryID,
		util.Kind:                kind,
		util.CountAttribute:      count,
	})
	arg.AttributeDataArgList = AattributeDataArgList
	return arg
}

func MakeCountsDeleteArg(categoryID string, kind string) *insights_interface.DeleteEntityArg {
	return MakeDeleteQuery(util.CategoryCountsIdfEntity, GetCountsUUID(categoryID, kind))
}

func MakeAssociationUpdateArgFlat(categoryID string, kind string, kindID string) *insights_interface.UpdateEntityArg {
	arg := &insights_interface.UpdateEntityArg{}

	arg.EntityGuid = GetEntityGUID(util.CategoryAssociationIdfEntity, GetAsscUUID(categoryID, kind, kindID))
	AattributeDataArgList := MakeAttributeDataArgList(map[string]interface{}{
		util.CategoryIDAttribute: categoryID,
		util.Kind:                kind,
		util.KindID:              kindID,
	})
	arg.AttributeDataArgList = AattributeDataArgList
	return arg
}

func GetAsscUUID(categoryID string, kind string, kindID string) string {
	return uuid4.NewFromStrings([]string{categoryID, kind, kindID})
}

func GetCountsUUID(categoryID string, kind string) string {
	return uuid4.NewFromStrings([]string{categoryID, kind})
}

func MakeAttributeDataArgList(dataMap map[string]interface{}) []*insights_interface.AttributeDataArg {
	argsList := make([]*insights_interface.AttributeDataArg, 0)

	for attr, val := range dataMap {
		if val == nil || val == "" {
			continue
		}
		attrCopy := attr
		valueType := CheckValueTypeAndSet(val)
		attrDataArg := &insights_interface.AttributeDataArg{AttributeData: &insights_interface.AttributeData{
			Name:  &attrCopy,
			Value: valueType,
		}}
		argsList = append(argsList, attrDataArg)
	}
	return argsList
}

func MakeBatchUpdateQuery(args []*insights_interface.UpdateEntityArg) *insights_interface.BatchUpdateEntitiesArg {
	batchArg := &insights_interface.BatchUpdateEntitiesArg{}
	batchArg.EntityList = make([]*insights_interface.UpdateEntityArg, 0, len(args))

	batchArg.EntityList = append(batchArg.EntityList, args...)
	return batchArg
}

func MakeBatchDeleteQuery(args []*insights_interface.DeleteEntityArg) *insights_interface.BatchDeleteEntitiesArg {
	batchArg := &insights_interface.BatchDeleteEntitiesArg{}
	batchArg.EntityList = make([]*insights_interface.DeleteEntityArg, 0, len(args))

	batchArg.EntityList = append(batchArg.EntityList, args...)
	return batchArg
}

func MakeUpdateArg(id string, entityName string, attrs []*insights_interface.AttributeDataArg) *insights_interface.UpdateEntityArg {
	return &insights_interface.UpdateEntityArg{
		EntityGuid:           GetEntityGUID(entityName, id),
		AttributeDataArgList: attrs,
	}
}

func MakeUpdateArgWithCasValue(id string, entityName string, casValue *uint64, attrs []*insights_interface.AttributeDataArg) *insights_interface.UpdateEntityArg {
	return &insights_interface.UpdateEntityArg{
		EntityGuid:           GetEntityGUID(entityName, id),
		AttributeDataArgList: attrs,
		CasValue:             casValue,
	}
}

func GetCategoryTypeNum(immutableVal interface{}, internalVal interface{}) int64 {
	immutable := false
	if immutableVal != nil {
		immutable = immutableVal.(bool)
	}
	internal := false
	if internalVal != nil {
		internal = internalVal.(bool)
	}

	if internal {
		return 3
	}
	if immutable {
		return 2
	}
	return 1
}

func Total(ret *insights_interface.GetEntitiesWithMetricsRet) (bool, int64) {
	if ret != nil {
		for _, ewm := range ret.GetGroupResultsList() {
			return true, ewm.GetTotalEntityCount()
		}
	}
	return false, 0
}

func HasResults(ret *insights_interface.GetEntitiesWithMetricsRet) bool {
	x, _ := Total(ret)
	return x
}

func MakeCondition[T any](attr string, operator insights_interface.ComparisonExpression_Operator, value *T) *insights_interface.BooleanExpression {
	exp := &insights_interface.BooleanExpression{ComparisonExpr: &insights_interface.ComparisonExpression{
		Lhs:      &insights_interface.Expression{Leaf: &insights_interface.LeafExpression{Column: &attr}},
		Operator: &operator,
	}}
	if value != nil {
		exp.ComparisonExpr.Rhs = &insights_interface.Expression{Leaf: &insights_interface.LeafExpression{Value: GetDataValue(*value)}}
	}
	return exp
}

// CheckCategoriesExist
/*
util to check presence of categories specified by the uuids in idf.
max 1000 categories can be specified. this limit is imposed by idf.
*/
func CheckCategoriesExist(idf idf.IIdfExecutor, categoryUuids []string) (mapset.Set, error) {
	N := len(categoryUuids)
	glog.Infof("making call to idf to check existence of %v categories in table:category", N)

	// validate categories:construct idf query
	idfArg := &insights_interface.GetEntitiesWithMetricsArg{Query: &insights_interface.Query{}}
	idfArg.Query.EntityList = make([]*insights_interface.EntityGuid, N)
	idfArg.Query.WhereClause = MakeCondition[string](util.ParentExtIDAttribute, insights_interface.ComparisonExpression_kExists, nil)
	for i, uuid := range categoryUuids {
		idfArg.Query.EntityList[i] = GetEntityGUID(util.CategoryIdfEntity, uuid)
	}

	// validate categories:idf call
	glog.V(1).Infof("check categories existence idf query:%v", idfArg)
	idfRet, err := idf.GetEntitiesWithMetrics(idfArg)
	if err != nil {
		errMsg := fmt.Sprintf("error when making idf call to check existence of categories:%v", err)
		return nil, status.Error(codes.Internal, errMsg)
	}
	glog.V(1).Infof("existing categories idf ret:%v", idfRet)

	// validate categories:process results
	glog.Info("processing idf results extract existing category uuids")
	categoriesFound := mapset.NewSet()
	if HasResults(idfRet) {
		for _, ewm := range idfRet.GroupResultsList[0].RawResults {
			categoriesFound.Add(*ewm.EntityGuid.EntityId)
		}
	} else {
		return nil, status.Error(codes.InvalidArgument, "all input categories invalid")
	}
	glog.Infof("%v/%v categories exist in table:category", categoriesFound.Cardinality(), N)
	return categoriesFound, nil
}

func GetCountsIDTmp(categoryID string, kind string) string {
	return categoryID + util.CountsIDSeparator + kind
}

func SplitCountsIDTmp(countsID string) (categoryID string, kind string) {
	x := strings.Split(countsID, util.CountsIDSeparator)
	return x[0], x[1]
}

func BatchPersist[T any](table string, db idf.IIdfExecutor, items []T, makeUpdateArg func(T) *insights_interface.UpdateEntityArg,
) error {
	total := len(items)
	return util.Batcher("update: "+table, util.CategoryBatchSize, total,
		func(ch chan *insights_interface.UpdateEntityArg) {
			for _, item := range items {
				glog.Infof("writing item %v in table %s", item, table)
				ch <- makeUpdateArg(item)
			}
			close(ch)
		},
		func(args []*insights_interface.UpdateEntityArg) error {
			_, err := db.BatchUpdateEntities(MakeBatchUpdateQuery(args))
			runtime.GC()
			return err
		},
	)
}

// MakeCategoryKeyUpdateArg creates an UpdateEntityArg for a category key to persist in the abac_category_key table
func MakeCategoryKeyUpdateArg(casValue *uint64, values map[string]interface{}) *insights_interface.UpdateEntityArg {
	attrs := make(map[string]interface{})
	immutable, _ := values[util.ImmutableAttribute].(bool)
	internal, _ := values[util.InternalAttribute].(bool)
	keyProto := categoriesproto.AbacCategoryKey{
		Uuid:        values[util.UUIDAttribute].(string),
		Name:        values[util.NameAttribute].(string),
		Description: values[util.DescriptionAttribute].(string),
		Immutable:   immutable,
		Internal:    internal,
		OwnerUuid:   values[util.OwnerUUIDAttribute].(string),
	}
	if n, ok := values[util.CARDINALITY]; ok {
		keyProto.Cardinality = n.(uint64)
	}
	pb, _ := proto.Marshal(&keyProto)
	attrs[util.ZPROTO] = pb
	return MakeUpdateArgWithCasValue(values[util.UUIDAttribute].(string), util.AbacCategoryKeyType, casValue, MakeAttributeDataArgList(attrs))
}

// MakeCategoryValueUpdateArg creates an UpdateEntityArg for a category value to persist in the abac_category table
func MakeCategoryValueUpdateArg(casValue *uint64, values map[string]interface{}) *insights_interface.UpdateEntityArg {
	attrs := make(map[string]interface{})
	immutable, _ := values[util.ImmutableAttribute].(bool)
	internal, _ := values[util.InternalAttribute].(bool)
	attrs[util.UUIDAttribute] = values[util.UUIDAttribute].(string)
	attrs[util.DescriptionAttribute] = values[util.DescriptionAttribute].(string)
	attrs[util.OwnerUUIDAttribute] = values[util.OwnerUUIDAttribute].(string)
	attrs[util.ImmutableAttribute] = values[util.ImmutableAttribute].(bool)
	attrs[util.InternalAttribute] = values[util.InternalAttribute].(bool)
	attrs[util.ValueAttribute] = values[util.ValueAttribute].(string)
	attrs[util.AbacCategoryKeyType] = values[util.AbacCategoryKeyType].(string)
	valueProto := categoriesproto.AbacCategory{
		Uuid:            values[util.UUIDAttribute].(string),
		AbacCategoryKey: values[util.AbacCategoryKeyType].(string),
		Value:           values[util.ValueAttribute].(string),
		Immutable:       immutable,
		Description:     values[util.DescriptionAttribute].(string),
		Internal:        internal,
		OwnerUuid:       values[util.OwnerUUIDAttribute].(string),
	}
	pb, _ := proto.Marshal(&valueProto)
	attrs[util.ZPROTO] = pb
	return MakeUpdateArgWithCasValue(values[util.UUIDAttribute].(string), util.AbacCategoryType, casValue, MakeAttributeDataArgList(attrs))
}

// MakeEntityCapabilityValueUpdateArg creates an UpdateEntityArg for an entity capability to persist in the abac_entity_capability table
func MakeEntityCapabilityValueUpdateArg(casValue *uint64, values map[string]string) *insights_interface.UpdateEntityArg {
	attrs := make(map[string]interface{})
	uuid := values[util.UUIDAttribute]
	kind := values[util.Kind]
	kindID := values[util.KindID]
	attrs[util.UUIDAttribute] = values[util.UUIDAttribute]
	attrs[util.Kind] = values[util.Kind]
	attrs[util.KindID] = values[util.KindID]
	valueProto := categoriesproto.AbacEntityCapability{
		Uuid:   uuid,
		Kind:   kind,
		KindId: kindID,
	}
	pb, _ := proto.Marshal(&valueProto)
	attrs[util.ZPROTO] = pb
	return MakeUpdateArgWithCasValue(uuid4.NewFromStrings([]string{kind, kindID}), util.AbacEntityCapabilityType, casValue, MakeAttributeDataArgList(attrs))
}

// GetCasValue retrieves the CAS value for an entity from the database
// If entityID is not present, then it's cas value is 0. If present, then some non-zero value will be returned
// increment cas value on the caller's side, when returned bool value is false. False value represents that such an entry exits in db. Hence, increment it's cas value.
func GetCasValue(entityID string, tableName string, db idf.IIdfExecutor) (*uint64, bool) {
	arg := MakeQueryMetricsEntityIDSimple(tableName, entityID)
	result, err := db.GetEntities(arg)
	if err != nil {
		glog.Errorf("error while fetching entity: %v with entityID: %s from table: %s", err, entityID, tableName)
		return nil, false
	}
	if len(result.Entity) == 0 {
		zeroValue := uint64(0)
		return &zeroValue, true
	}
	casValue := result.Entity[0].GetCasValue()
	return &casValue, false
}

func GetDataMap(metricDataList []*insights_interface.MetricData) map[string]*insights_interface.MetricData {
	dataMap := make(map[string]*insights_interface.MetricData)
	for _, metricData := range metricDataList {
		dataMap[metricData.GetName()] = metricData
	}
	return dataMap
}

func GetStringDataValue(data *insights_interface.MetricData) string {
	if data != nil && len(data.GetValueList()) > 0 {
		dataValue := data.GetValueList()[0].GetValue()
		if dataValue != nil {
			return dataValue.GetStrValue()
		}
	}
	return ""
}

func GetIntDataValue(data *insights_interface.MetricData) int64 {
	if data != nil && len(data.GetValueList()) > 0 {
		dataValue := data.GetValueList()[0].GetValue()
		if dataValue != nil {
			return dataValue.GetInt64Value()
		}
	}
	return 0
}

func FilterTableMetricOfCategory(categoryID string) *insights_interface.GetEntitiesWithMetricsArg {
	categoryAttr := []string{util.CATEGORY}
	whereClause := MakeCondition(util.FilterExpLhsEntityType, insights_interface.ComparisonExpression_kContains, &categoryAttr)
	likeCondition := MakeCondition(util.FilterExpRhsEntityUuids, insights_interface.ComparisonExpression_kLike, proto.String(".*"+categoryID+".*"))
	combinedWhereClause, _ := ReduceBooleanExpressions([]insights_interface.BooleanExpression{*whereClause, *likeCondition}, insights_interface.BooleanExpression_kAnd)

	return &insights_interface.GetEntitiesWithMetricsArg{
		Query: &insights_interface.Query{
			EntityList: []*insights_interface.EntityGuid{
				{
					EntityTypeName: proto.String(util.FilterType),
				},
			},
			WhereClause: &combinedWhereClause,
		},
	}
}

func AbacCategoryMetric(entityID string, columns []string) *insights_interface.GetEntitiesWithMetricsArg {
	return &insights_interface.GetEntitiesWithMetricsArg{
		Query: &insights_interface.Query{
			EntityList: []*insights_interface.EntityGuid{
				GetEntityGUID(util.AbacCategoryType, entityID),
			},
			GroupBy: &insights_interface.QueryGroupBy{
				RawColumns: MakeRawColumns(columns),
			},
		},
	}
}

func GetStringAttrValue(idfResult *insights_interface.GetEntitiesWithMetricsRet, attr string) string {
	if GetRetLength(idfResult) > 0 {
		firstResult := idfResult.GetGroupResultsList()[0].GetRawResults()[0]
		dataMap := GetDataMap(firstResult.GetMetricDataList())
		return GetStringDataValue(dataMap[attr])
	}
	return ""
}

func AbacCategoryKeySimple(entityID string) *insights_interface.GetEntitiesArg {
	return MakeQueryMetricsEntityIDSimple(util.AbacCategoryKeyType, entityID)
}

func AbacCategorySimple(entityID string) *insights_interface.GetEntitiesArg {
	return MakeQueryMetricsEntityIDSimple(util.AbacCategoryType, entityID)
}

func FixComma(db idf.IIdfExecutor) error {
	tables := []string{util.AbacCategoryKeyType, util.AbacCategoryType, util.CategoryIdfEntity}
	columns := [][]string{
		{util.DescriptionAttribute, util.NameAttribute, util.ImmutableAttribute, util.InternalAttribute, util.UUIDAttribute, util.OwnerUUIDAttribute},
		{util.AbacCategoryKeyAttribute, util.DescriptionAttribute, util.UUIDAttribute, util.ImmutableAttribute, util.InternalAttribute, util.OwnerUUIDAttribute, util.UserSpecifiedNameAttribute, util.ValueAttribute},
		{util.ParentExtIDAttribute, util.DescriptionAttribute, util.KeyAttribute, util.NameAttribute, util.FqNameAttribute, util.UserSpecifiedNameAttribute, util.KeyAttribute, util.CategoryTypeAttribute, util.ExtIDAttribute, util.ImmutableAttribute, util.InternalAttribute, util.OwnerUUIDAttribute},
	}
	for idx, table := range tables {
		glog.Infof("Starting to process table: %s", table)
		glog.Infof("Fetching all entries from table: %s", table)
		// Fetch all entries at once
		arg := &insights_interface.GetEntitiesWithMetricsArg{
			Query: &insights_interface.Query{
				EntityList: []*insights_interface.EntityGuid{
					{
						EntityTypeName: proto.String(table),
					},
				},
				GroupBy: &insights_interface.QueryGroupBy{
					RawColumns: MakeRawColumns(columns[idx]),
				},
			},
		}
		ret, err := db.GetEntitiesWithMetrics(arg)
		if err != nil {
			glog.Errorf("Error fetching entries from table %s: %v", table, err)
			return err
		}
		glog.Infof("Fetched %d entries from table: %s", GetRetLength(ret), table)
		if GetRetLength(ret) == 0 {
			glog.Infof("No entries to process in table: %s", table)
			continue
		}

		glog.Infof("Processing entries from table: %s", table)
		// Process each entry
		updates := make([]*insights_interface.UpdateEntityArg, 0)
		for _, entity := range ret.GetGroupResultsList()[0].GetRawResults() {
			updated := false
			attrs := make(map[string]*insights_interface.DataValue)
			attrs[util.DescriptionAttribute] = &insights_interface.DataValue{
				ValueType: &insights_interface.DataValue_StrValue{StrValue: ""},
			}
			attrs[util.OwnerUUIDAttribute] = &insights_interface.DataValue{
				ValueType: &insights_interface.DataValue_StrValue{StrValue: ""},
			}
			for _, attrData := range entity.GetMetricDataList() {
				if len(attrData.GetValueList()) == 0 {
					continue
				}
				attrs[attrData.GetName()] = attrData.GetValueList()[0].GetValue()
			}
			// glog.Infof("attrs: %v", attrs)
			for key, value := range attrs {
				if value == nil {
					continue
				}
				if key != util.DescriptionAttribute {
					switch value.GetValueType().(type) {
					case *insights_interface.DataValue_StrValue:
						if strings.Contains(value.GetStrValue(), ",") {
							glog.Infof("Found comma in string value for attribute: %s of entity %s in table: %s. Replacing with '-'", key, entity.GetEntityGuid().GetEntityId(), table)
							newValue := strings.ReplaceAll(value.GetStrValue(), ",", "-")
							attrs[key] = &insights_interface.DataValue{
								ValueType: &insights_interface.DataValue_StrValue{StrValue: newValue},
							}
							updated = true
						}
					case *insights_interface.DataValue_StrList_:
						glog.Infof("Converting string list to single string for key: %s in table: %s", key, table)
						joinedValue := strings.Join(value.GetStrList().GetValueList(), "-")
						if strings.Contains(joinedValue, ",") {
							glog.Infof("Found comma in string list value for attribute: %s of entity %s in table: %s. Replacing with '-'", key, entity.GetEntityGuid().GetEntityId(), table)
							joinedValue = strings.ReplaceAll(joinedValue, ",", "-")
						}
						attrs[key] = &insights_interface.DataValue{
							ValueType: &insights_interface.DataValue_StrValue{StrValue: joinedValue},
						}
						updated = true
					default:
						glog.V(1).Infof("No action required for attribute: %s of entity %s in table: %s", key, entity.GetEntityGuid().GetEntityId(), table)
						attrs[key] = value
					}
				} else {
					attrs[key] = value
				}
			}

			if updated {
				glog.Infof("Attributes updated for entity ID: %s in table: %s. Preparing update argument.", entity.GetEntityGuid().GetEntityId(), table)
				convertedAttrs := make(map[string]interface{})
				for key, value := range attrs {
					if value == nil {
						continue
					}
					// check the type of value and convert accordingly
					switch value.GetValueType().(type) {
					case *insights_interface.DataValue_StrValue:
						convertedAttrs[key] = value.GetStrValue()
					case *insights_interface.DataValue_Int64Value:
						convertedAttrs[key] = value.GetInt64Value()
					case *insights_interface.DataValue_BoolValue:
						convertedAttrs[key] = value.GetBoolValue()
					}
				}
				updateArg := &insights_interface.UpdateEntityArg{}
				if table == util.AbacCategoryKeyType {
					casValue, _ := GetCasValue(entity.GetEntityGuid().GetEntityId(), table, db)
					(*casValue)++
					updateArg = MakeCategoryKeyUpdateArg(casValue, convertedAttrs)
				} else if table == util.AbacCategoryType {
					casValue, _ := GetCasValue(entity.GetEntityGuid().GetEntityId(), table, db)
					(*casValue)++
					updateArg = MakeCategoryValueUpdateArg(casValue, convertedAttrs)
				} else {
					updateArg = MakeUpdateArg(entity.GetEntityGuid().GetEntityId(), table, MakeAttributeDataArgList(convertedAttrs))
				}
				glog.V(1).Infof("updateArg: %v", updateArg)
				updates = append(updates, updateArg)
			}
		}

		// Batch update the modified entries
		if len(updates) > 0 {
			glog.Infof("Batch updating %d entries in table: %s", len(updates), table)
			_, err := db.BatchUpdateEntities(MakeBatchUpdateQuery(updates))
			if err != nil {
				glog.Errorf("Error updating entries in table %s: %v", table, err)
				return err
			}
			glog.Infof("Successfully updated %d entries in table: %s", len(updates), table)
		} else {
			glog.Infof("No updates required for the entries in table: %s", table)
		}

		glog.Infof("Finished processing table: %s", table)
	}
	return nil
}
