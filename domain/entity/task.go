package entity

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Task struct {
	ID      primitive.ObjectID `bson:"_id"`
	EventId int64              `bson:"EventId"`
	GroupId int64              `bson:"GroupId"`
	Event   string             `bson:"Event"`
	Status  string             `bson:"Status"`
}
