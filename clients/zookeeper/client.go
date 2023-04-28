package zookeeper

import (
	"time"

	"github.com/go-zookeeper/zk"
)

type Client struct {
	Conn       *zk.Conn
	LeaderLock *zk.Lock
}

func NewClient() *Client {
	conn, _, err := zk.Connect([]string{"localhost:2181"}, 5*time.Minute)
	if err != nil {
		panic(err)
	}
	lock := zk.NewLock(conn, "/watcher/lock", zk.WorldACL(zk.PermAll))

	c := &Client{
		Conn:       conn,
		LeaderLock: lock,
	}
	return c
}
