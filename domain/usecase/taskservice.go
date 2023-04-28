package usecase

import (
	"context"
	"fmt"
	"github.com/mehmetolgundev/distributed-watcher/domain/entity"
	"github.com/mehmetolgundev/distributed-watcher/domain/repository"
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
	zs.TaskZKRepository.RegisterNode(ctx)
	go zs.TaskZKRepository.ElectLeader(ctx)
	go zs.TaskZKRepository.WatchNodes(ctx)
}
func (zs *TaskService) Start(ctx context.Context) {
	for {
		if zs.TaskZKRepository.IsLeader() {
			zs.assignTask(ctx)
		} else {

		}
	}

}
func (zs *TaskService) assignTask(ctx context.Context) {
	var (
		task *entity.Task
		err  error
	)
	for {
		task, err = zs.TaskDBRepository.GetTask(ctx)
		if err != nil {
			panic(err)
		}
		if task != nil {
			fmt.Println(task.EventId)
			zs.TaskZKRepository.QueueTask(task.GroupId, task.EventId)
			task.Status = "Queued"
			zs.TaskDBRepository.UpdateTask(ctx, task)
		}

	}

}
