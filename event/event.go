package event

import (
	"time"

	"github.com/hunknownz/watchrelay/resource"

	"gorm.io/datatypes"
)

type EventAction int

const (
	EventActionCreate EventAction = iota
	EventActionUpdate
	EventActionDelete
	EventActionGap // Gap is a placeholder for a missing event.
)

type LogEvent struct {
	Revision       uint64 `gorm:"primary_key"`
	CreateRevision uint64
	PrevRevision   uint64
	ResourceName   string
	Created        bool
	Deleted        bool
	Value          datatypes.JSON
	CreatedAt      time.Time
}

func (e *LogEvent) TableName() string {
	return "watchrelay"
}

type IEvent interface {
	IsGap() bool
	GetValue() any
	GetResourceName() string
	GetAction() EventAction
	GetRevision() uint64
	GetCreateRevision() uint64
	GetCreatedAt() time.Time
}

type Event[T resource.IVersionedResource] struct {
	CreateRevision uint64
	Revision       uint64
	ResourceName   string
	Action         EventAction
	Value          T
	CreatedAt      time.Time
}

func (e *Event[T]) IsGap() bool {
	return e.Action == EventActionGap
}

func (e *Event[T]) GetResourceName() string {
	return e.ResourceName
}

func (e *Event[T]) GetAction() EventAction {
	return e.Action
}

func (e *Event[T]) GetRevision() uint64 {
	return e.Revision
}

func (e *Event[T]) GetCreateRevision() uint64 {
	return e.CreateRevision
}

func (e *Event[T]) GetCreatedAt() time.Time {
	return e.CreatedAt
}

func (e *Event[T]) GetValue() any {
	return e.Value
}

type EventFunc func(rv, createRv uint64, action EventAction, createdAt time.Time, v []byte) (IEvent, error)
