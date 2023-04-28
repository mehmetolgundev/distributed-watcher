package repository

import (
	"context"
	"github.com/mehmetolgundev/distributed-watcher/domain/entity"
)

type TaskDBRepository interface {
	GetTask(ctx context.Context) (*entity.Task, error)
	UpdateTask(ctx context.Context, task *entity.Task)
}
type TaskZKRepository interface {
	RegisterNode(ctx context.Context)
	ElectLeader(ctx context.Context)
	IsLeader() bool
	WatchNodes(ctx context.Context)
	QueueTask(taskGroupId int64, taskEventId int64)
}
