package usecase

import (
	"context"
	"github.com/mehmetolgundev/distributed-watcher/domain/entity"
	"github.com/mehmetolgundev/distributed-watcher/domain/repository"
	"sync"
	"time"
)

type TaskService struct {
	TaskDBRepository repository.TaskDBRepository
	TaskZKRepository repository.TaskZKRepository
}

func NewTaskService(taskDBRepository repository.TaskDBRepository, taskZKRepository repository.TaskZKRepository) *TaskService {
	return &TaskService{
		TaskDBRepository: taskDBRepository,
		TaskZKRepository: taskZKRepository,
	}
}

func (zs *TaskService) Register(ctx context.Context) {
	zs.TaskZKRepository.RegisterNode()
	zs.TaskZKRepository.TryToBecomeLeader()

}
func (zs *TaskService) WatchLeader(wg *sync.WaitGroup) {

	go zs.TaskZKRepository.WatchLeader()
}
func (zs *TaskService) WatchNodes(ctx context.Context, wg *sync.WaitGroup) {
	go zs.TaskZKRepository.WatchNodes(ctx)
}
func (zs *TaskService) Start(ctx context.Context, wg *sync.WaitGroup) {
	for {
		if zs.TaskZKRepository.IsLeader() {
			zs.assignTask(ctx)
		} else {
			zs.TaskZKRepository.TakeTask()
		}
	}

}
func (zs *TaskService) assignTask(ctx context.Context) {
	var (
		task *entity.Task
		err  error
	)

	task, err = zs.TaskDBRepository.GetTask(ctx)
	if err != nil {
		panic(err)
	}
	if task != nil {
		err = zs.TaskZKRepository.QueueTask(task.GroupId, task.EventId)
		if err != nil {
			time.Sleep(5 * time.Second)
			return
		}
		task.Status = "Queued"
		zs.TaskDBRepository.UpdateTask(ctx, task)
	}

}
