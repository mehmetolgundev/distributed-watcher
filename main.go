package main

import (
	"context"
	"github.com/mehmetolgundev/distributed-watcher/domain/usecase"
	"github.com/mehmetolgundev/distributed-watcher/infra/db"
	"github.com/mehmetolgundev/distributed-watcher/infra/zookeeper"
	"github.com/mehmetolgundev/distributed-watcher/repository"
)

func main() {
	ctx := context.Background()
	mongoClient := db.NewClient(ctx)
	zookeeperClient := zookeeper.NewClient()

	taskDBRepository := repository.NewTaskDBRepository(mongoClient)
	taskZKRepository := repository.NewTaskZKRepository(zookeeperClient)

	taskService := usecase.NewTaskService(taskDBRepository, taskZKRepository)
	go taskService.Register(ctx)
	go taskService.Start(ctx)
}
