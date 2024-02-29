package watchrelay

import (
	"time"

	"gorm.io/datatypes"
)

type EventAction int

const (
	EventActionCreate EventAction = iota
	EventActionUpdate
	EventActionDelete
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

type Event struct {
	CreateRevision uint64
	Revision       uint64
	ResourceName   string
	Action         EventAction
	Value          []byte
	CreatedAt      time.Time
}
