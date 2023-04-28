package repository

import (
	"context"

	"github.com/mehmetolgundev/distributed-watcher/clients/mongo"
	"github.com/mehmetolgundev/distributed-watcher/domain/entity"
	"go.mongodb.org/mongo-driver/bson"
	mongodb "go.mongodb.org/mongo-driver/mongo"
)

const collectionName = "Task"

type TaskRepository struct {
	MongoClient *mongo.MongoClient
	Collection  *mongodb.Collection
}

func NewTaskRepository(mongoClient *mongo.MongoClient) *TaskRepository {
	tr := &TaskRepository{
		MongoClient: mongoClient,
		Collection:  mongoClient.Client.Database("TaskManagement").Collection(collectionName),
	}
	return tr
}

func (t *TaskRepository) GetTask(ctx context.Context) (*entity.Task, error) {
	var task entity.Task
	filter := bson.M{
		"status": "Created",
	}
	r := t.Collection.FindOne(ctx, filter)
	err := r.Decode(&task)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (t *TaskRepository) SetTaskAssigned(ctx context.Context, task *entity.Task) {

	filter := bson.M{
		"eventId": task.EventId,
	}
	task.Status = "Assigned"
	_, err := t.Collection.ReplaceOne(ctx, filter, &task)
	if err != nil {
		panic(err)
	}

}
