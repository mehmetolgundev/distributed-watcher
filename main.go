package main

import (
	"context"
	"github.com/mehmetolgundev/distributed-watcher/domain/usecase"
	"github.com/mehmetolgundev/distributed-watcher/infra/db"
	"github.com/mehmetolgundev/distributed-watcher/infra/zookeeper"
	"github.com/mehmetolgundev/distributed-watcher/repository"
	"sync"
)

func main() {
	ctx := context.Background()
	mongoClient := db.NewClient(ctx)
	zookeeperClient := zookeeper.NewClient()

	taskDBRepository := repository.NewTaskDBRepository(mongoClient)
	taskZKRepository := repository.NewTaskZKRepository(zookeeperClient)

	taskService := usecase.NewTaskService(taskDBRepository, taskZKRepository)
	taskService.Register(ctx)

	wg := &sync.WaitGroup{}
	wg.Add(3)
	go taskService.WatchLeader(wg)
	go taskService.WatchNodes(ctx, wg)
	go taskService.Start(ctx, wg)
	wg.Wait()
}
