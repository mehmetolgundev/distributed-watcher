package domain

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/go-zookeeper/zk"
	"github.com/mehmetolgundev/distributed-watcher/clients/zookeeper"
	"github.com/mehmetolgundev/distributed-watcher/domain/entity"
	"github.com/mehmetolgundev/distributed-watcher/repository"
)

type ZookeeperService struct {
	ZookeeperClient *zookeeper.Client
	TaskRepository  *repository.TaskRepository
	NodeId          string
	LeaderNodeId    string
}

func NewZookeeperService(zookeeperClient *zookeeper.Client, taskRepository *repository.TaskRepository) *ZookeeperService {
	return &ZookeeperService{
		ZookeeperClient: zookeeperClient,
		TaskRepository:  taskRepository,
	}
}

func (zs *ZookeeperService) CreateNodeIfNotExists(path string, data []byte, flag int32, acl []zk.ACL) {
	isExists, _, err := zs.ZookeeperClient.Conn.Exists(path)
	if err != nil {
		panic(err)
	}
	if isExists {
		return
	}
	createdPath, err := zs.ZookeeperClient.Conn.Create(path, data, flag, acl)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s created.\n", createdPath)
}

func (zs *ZookeeperService) Register() {
	flag := int32(zk.FlagEphemeral | zk.FlagSequence)

	path, err := zs.ZookeeperClient.Conn.Create("/watcher/node-", nil, flag, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	zs.NodeId = path[len("/watcher/node-"):]

	fmt.Printf("%s created.\n", path)
}

func (zs *ZookeeperService) LeaderElection(ctx context.Context) {
	err := zs.ZookeeperClient.LeaderLock.Lock()
	if err != nil {
		panic(err)
	}
	defer zs.ZookeeperClient.LeaderLock.Unlock()
	isExists, _, err := zs.ZookeeperClient.Conn.Exists("/watcher/leader")
	if err != nil {
		panic(err)
	}

	if !isExists {

		_, err := zs.ZookeeperClient.Conn.Create("/watcher/leader", []byte(zs.NodeId), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			panic(err)
		}

		fmt.Printf("%s became leader \n", zs.NodeId)
		go zs.leaderOperation(ctx)
		zs.LeaderNodeId = zs.NodeId
	} else {
		lead, _, err := zs.ZookeeperClient.Conn.Get("/watcher/leader")
		if err != nil {
			panic(err)
		}

		fmt.Printf("Leader : %s \n", string(lead))
		zs.LeaderNodeId = string(lead)
	}

}
func (zs *ZookeeperService) leaderOperation(ctx context.Context) {
	var (
		task *entity.Task
		err  error
	)
	for {
		task, err = zs.TaskRepository.GetTask(ctx)
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			panic(err)
		}
		if !errors.Is(err, mongo.ErrNoDocuments) {
			fmt.Println(task.EventId)
			zs.TaskRepository.SetTaskAssigned(ctx, task)
		}

		time.Sleep(2 * time.Second)
	}

}
func (zs *ZookeeperService) WatchLeader(ctx context.Context) {

	for {
		_, _, event, err := zs.ZookeeperClient.Conn.GetW(fmt.Sprintf("/watcher/node-%s", zs.LeaderNodeId))
		if err != nil {
			panic(err)
		}
		e := <-event
		if e.Type == zk.EventNodeDeleted {
			fmt.Println("Leader deleted")
			zs.LeaderElection(ctx)
		}
	}

}
