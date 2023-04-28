package main

import (
	"context"

	"github.com/mehmetolgundev/distributed-watcher/repository"

	"github.com/go-zookeeper/zk"
	"github.com/mehmetolgundev/distributed-watcher/clients/mongo"
	"github.com/mehmetolgundev/distributed-watcher/clients/zookeeper"
	"github.com/mehmetolgundev/distributed-watcher/domain"
)

func main() {
	zookeeperClient := zookeeper.NewClient()
	mongoClient := mongo.NewClient(context.Background())
	taskRepository := repository.NewTaskRepository(mongoClient)

	zookeeperService := domain.NewZookeeperService(zookeeperClient, taskRepository)

	zookeeperService.CreateNodeIfNotExists("/watcher", nil, 0, zk.WorldACL(zk.PermAll))

	zookeeperService.Register()
	zookeeperService.LeaderElection(context.Background())

	zookeeperService.WatchLeader(context.Background())

}
