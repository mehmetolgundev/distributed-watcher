package repository

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/go-zookeeper/zk"
	"github.com/mehmetolgundev/distributed-watcher/infra/zookeeper"
	"hash/fnv"
	"sort"
	"strconv"
)

type TaskZKRepository struct {
	zookeeperClient *zookeeper.Client
	NodeId          string
	LeaderNodeId    string
	Nodes           []uint64
	NodeHashValue   uint64
}

func NewTaskZKRepository(zookeeperClient *zookeeper.Client) *TaskZKRepository {
	return &TaskZKRepository{
		zookeeperClient: zookeeperClient,
	}
}

func (t *TaskZKRepository) RegisterNode(ctx context.Context) {
	t.createNodeIfNotExists("/watcher", nil, 0, zk.WorldACL(zk.PermAll))
	flag := int32(zk.FlagEphemeral | zk.FlagSequence)

	path, err := t.zookeeperClient.Conn.Create("/watcher/node-", nil, flag, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	t.NodeId = path[len("/watcher/node-"):]
	t.NodeHashValue = t.hash(fmt.Sprintf("node-%s", t.NodeId))
	nodeHashData := make([]byte, 8)
	binary.LittleEndian.PutUint64(nodeHashData, t.NodeHashValue)
	_, err = t.zookeeperClient.Conn.Create(fmt.Sprintf("/watcher/nodes/%d", t.NodeHashValue), nodeHashData, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s named node created.\n", path)
}
func (t *TaskZKRepository) ElectLeader(ctx context.Context) {
	for {
		if t.LeaderNodeId == "" {
			t.becomeLeaderIfNotExists(ctx)
		} else {
			_, _, event, err := t.zookeeperClient.Conn.GetW(fmt.Sprintf("/watcher/node-%s", t.LeaderNodeId))
			if err != nil {
				panic(err)
			}
			e := <-event
			if e.Type == zk.EventNodeDeleted {
				fmt.Println("Leader deleted")
				t.becomeLeaderIfNotExists(ctx)
			}
		}
	}
}
func (t *TaskZKRepository) WatchNodes(ctx context.Context) {
	nodes, _, event, err := t.zookeeperClient.Conn.ChildrenW("/watcher/nodes")
	if err != nil {
		panic(err)
	}
	t.Nodes = nil
	for _, node := range nodes {
		v, _ := strconv.ParseUint(node, 10, 32)
		t.Nodes = append(t.Nodes, v)
	}
	sort.Slice(t.Nodes, func(i, j int) bool {
		return t.Nodes[i] < t.Nodes[j]
	})
	_ = <-event
	t.WatchNodes(ctx)
}
func (t *TaskZKRepository) QueueTask(taskGroupId int64, taskEventId int64) {
	hashedGroupId := t.hash(string(taskGroupId))
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(taskEventId))
	t.createNode(fmt.Sprintf("/watcher/task/%d", hashedGroupId), data, 0, zk.WorldACL(zk.PermAll))
}
func (t *TaskZKRepository) IsLeader() bool {
	isExists, _, err := t.zookeeperClient.Conn.Exists("/watcher/leader")
	if err != nil {
		panic(err)
	}
	if isExists {
		lead, _, err := t.zookeeperClient.Conn.Get("/watcher/leader")
		if err != nil {
			panic(err)
		}
		if t.NodeId == string(lead) {
			return true
		}
	}
	return false
}
func (t *TaskZKRepository) createNodeIfNotExists(path string, data []byte, flag int32, acl []zk.ACL) {
	isExists, _, err := t.zookeeperClient.Conn.Exists(path)
	if err != nil {
		panic(err)
	}
	if isExists {
		return
	}
	createdPath, err := t.zookeeperClient.Conn.Create(path, data, flag, acl)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s named node created.\n", createdPath)
}
func (t *TaskZKRepository) createNode(path string, data []byte, flag int32, acl []zk.ACL) {
	createdPath, err := t.zookeeperClient.Conn.Create(path, data, flag, acl)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s named node created.\n", createdPath)
}
func (t *TaskZKRepository) becomeLeaderIfNotExists(ctx context.Context) {
	err := t.zookeeperClient.LeaderLock.Lock()
	if err != nil {
		panic(err)
	}
	defer t.zookeeperClient.LeaderLock.Unlock()
	isExists, _, err := t.zookeeperClient.Conn.Exists("/watcher/leader")
	if err != nil {
		panic(err)
	}

	if !isExists {

		_, err := t.zookeeperClient.Conn.Create("/watcher/leader", []byte(t.NodeId), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err != nil {
			panic(err)
		}

		fmt.Printf("%s became leader \n", t.NodeId)
		//go t.leaderOperation(ctx)
		t.LeaderNodeId = t.NodeId
	} else {
		lead, _, err := t.zookeeperClient.Conn.Get("/watcher/leader")
		if err != nil {
			panic(err)
		}

		fmt.Printf("Leader : %s \n", string(lead))
		t.LeaderNodeId = string(lead)
	}

}
func (t *TaskZKRepository) hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
