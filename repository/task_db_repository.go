package repository

import (
	"context"
	"errors"
	"github.com/mehmetolgundev/distributed-watcher/domain/entity"
	"github.com/mehmetolgundev/distributed-watcher/infra/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const collectionName = "Task"
const dbName = "TaskManagement"

type TaskDBRepository struct {
	collection *mongo.Collection
}

func NewTaskDBRepository(mongoClient *db.MongoClient) *TaskDBRepository {
	return &TaskDBRepository{
		collection: mongoClient.Client.Database(dbName).Collection(collectionName),
	}
}

func (t *TaskDBRepository) GetTask(ctx context.Context) (*entity.Task, error) {
	var task entity.Task
	filter := bson.M{
		"Status": "Created",
	}
	r := t.collection.FindOne(ctx, filter)
	err := r.Decode(&task)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}
func (t *TaskDBRepository) UpdateTask(ctx context.Context, task *entity.Task) {

	filter := bson.M{
		"EventId": task.EventId,
	}
	_, err := t.collection.ReplaceOne(ctx, filter, &task)
	if err != nil {
		panic(err)
	}

}
