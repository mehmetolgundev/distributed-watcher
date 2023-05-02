package repository

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"github.com/mehmetolgundev/distributed-watcher/infra/zookeeper"
	"hash/fnv"
	"sort"
	"time"
)

type TaskZKRepository struct {
	zookeeperClient *zookeeper.Client
	NodeId          string
	LeaderNodeId    string
	Nodes           []uint32
	NodeHashValue   uint32
}

func NewTaskZKRepository(zookeeperClient *zookeeper.Client) *TaskZKRepository {
	return &TaskZKRepository{
		zookeeperClient: zookeeperClient,
	}
}

func (t *TaskZKRepository) RegisterNode() {
	t.createNodeIfNotExists("/watcher", nil, 0, zk.WorldACL(zk.PermAll))
	t.createNodeIfNotExists("/watcher/nodes", nil, 0, zk.WorldACL(zk.PermAll))
	t.createNodeIfNotExists("/watcher/tasks", nil, 0, zk.WorldACL(zk.PermAll))
	flag := int32(zk.FlagEphemeral | zk.FlagSequence)

	path, err := t.zookeeperClient.Conn.Create("/watcher/nodes/node-", nil, flag, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	t.NodeId = path[len("/watcher/nodes/node-"):]
	t.NodeHashValue = t.hash(fmt.Sprintf("node-%s", t.NodeId))
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data, t.NodeHashValue)
	_, err = t.zookeeperClient.Conn.Set(path, data, -1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s named node created.\n", path)
}
func (t *TaskZKRepository) WatchLeader() {
	for {
		_, _, event, err := t.zookeeperClient.Conn.GetW(fmt.Sprintf("/watcher/nodes/node-%s", t.LeaderNodeId))
		if err != nil {
			panic(err)
		}
		e := <-event
		if e.Type == zk.EventNodeDeleted {
			fmt.Println("Leader deleted")
			t.TryToBecomeLeader()
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
		t.Nodes = append(t.Nodes, t.hash(node))
	}
	sort.Slice(t.Nodes, func(i, j int) bool {
		return t.Nodes[i] < t.Nodes[j]
	})
	fmt.Println(t.Nodes[0])
	_ = <-event
	t.WatchNodes(ctx)
}
func (t *TaskZKRepository) QueueTask(taskGroupId int64, taskEventId int64) error {
	hashedGroupId := t.hash(string(taskGroupId))
	responsibleNode := t.findProperNode(hashedGroupId)
	if responsibleNode == 0 {
		fmt.Println("There is no available node.")
		return fmt.Errorf("There is no available node.")
	}
	t.createNodeIfNotExists(fmt.Sprintf("/watcher/tasks/%d", responsibleNode), nil, 0, zk.WorldACL(zk.PermAll))
	t.createNode(fmt.Sprintf("/watcher/tasks/%d/%d", responsibleNode, taskEventId), nil, 0, zk.WorldACL(zk.PermAll))
	return nil
}
func (t *TaskZKRepository) TakeTask() {

	tasks, _, err := t.zookeeperClient.Conn.Children(fmt.Sprintf("/watcher/tasks/%d", t.NodeHashValue))
	if err != nil && !errors.Is(err, zk.ErrNoNode) {
		panic(err)
	}
	for _, taskId := range tasks {
		fmt.Printf("%s task will be done", taskId)
		time.Sleep(3 * time.Second)
		/*	err = t.zookeeperClient.Conn.Delete(fmt.Sprintf("/watcher/tasks/%d/%s", t.NodeHashValue, taskId), -1)
			if err != nil {
				panic(err)
			}*/
		fmt.Printf("%s task done", taskId)
	}

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
func (t *TaskZKRepository) TryToBecomeLeader() {
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
func (t *TaskZKRepository) hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
func (t *TaskZKRepository) findProperNode(hashValue uint32) uint32 {
	if len(t.Nodes) == 0 || len(t.Nodes) == 1 {
		return 0
	}
	for _, h := range t.Nodes {
		fmt.Printf("Node hash : %d Task Hash : %d \n", h, hashValue)
		if h >= hashValue && h != t.NodeHashValue {
			fmt.Println("found")
			return h
		}
	}
	if t.Nodes[0] != t.NodeHashValue {
		fmt.Printf("Node[0] will be used hash : %d \n", t.Nodes[0])
		return t.Nodes[0]
	}
	fmt.Printf("Node[1] will be used hash : %d \n", t.Nodes[1])
	return t.Nodes[1]

}
