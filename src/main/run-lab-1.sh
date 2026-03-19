#!/usr/bin/env bash
go build -buildmode=plugin ../mrapps/wc.go
rm mr-*
echo "创建调度节点 coordinator"
go run mrcoordinator.go sock123 pg-*.txt &
center=$!
echo "coordinator-PID: $center"
sleep 1
for i in {1..3}; do
    echo "创建工作节点 work-$i"
    go run mrworker.go wc.so sock123 &
    echo "work-$i-PID: $!"
done
wait $center
echo "任务完成"
