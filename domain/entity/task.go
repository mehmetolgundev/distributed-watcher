package entity

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Task struct {
	ID      primitive.ObjectID `bson:"_id"`
	EventId int64              `bson:"eventId"`
	Event   string             `bson:"event"`
	Status  string             `bson:"status"`
}
