//go:generate mockgen -package=mocks -destination=mocks/mock_${GOFILE} -source=$GOFILE
package idf

import "github.com/nutanix-core/go-cache/insights/insights_interface"

type IIdfExecutor interface {
	GetEntitiesWithMetrics(arg *insights_interface.GetEntitiesWithMetricsArg) (*insights_interface.GetEntitiesWithMetricsRet, error)
	UpdateEntity(arg *insights_interface.UpdateEntityArg) (*insights_interface.UpdateEntityRet, error)
	DeleteEntity(arg *insights_interface.DeleteEntityArg) (*insights_interface.DeleteEntityRet, error)
	BatchUpdateEntities(arg *insights_interface.BatchUpdateEntitiesArg) (*insights_interface.BatchUpdateEntitiesRet, error)
	BatchDeleteEntities(arg *insights_interface.BatchDeleteEntitiesArg) (*insights_interface.BatchDeleteEntitiesRet, error)
	BatchGetEntities(arg *insights_interface.BatchGetEntitiesWithMetricsArg) (*insights_interface.BatchGetEntitiesWithMetricsRet, error)
	ExecuteWithCursor(args *insights_interface.GetEntitiesWithMetricsArg) ([]*insights_interface.Entity, error)
	GetEntities(args *insights_interface.GetEntitiesArg) (*insights_interface.GetEntitiesRet, error)
}
