package main

import (
	"github.com/golang/glog"
	"github.com/nutanix-core/go-cache/insights/insights_interface"
	"github.com/nutanix-core/go-cache/util-go/net"
	"fix_comma/idf"
	idfUtil "fix_comma/idf/query"
	"fix_comma/logpackage"
)

func main() {
	logpackage.Init()
	db := &idf.IdfExecutor{RPC: &insights_interface.InsightsRpcClient{Impl: net.NewProtobufRPCClient(
		*insights_interface.DefaultInsightAddr, uint16(*insights_interface.DefaultInsightPort))}}
	glog.Infof("Starting categories golang data-sync service.")
	idfUtil.FixComma(db)
	glog.Flush()
}
