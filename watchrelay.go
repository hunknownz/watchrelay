package watchrelay

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/hunknownz/watchrelay/storage/mysql"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type WatchRelay struct {
	seq    *Sequence
	sqlLog *SQLLog

	db      *gorm.DB
	dialect Dialect
}

// NewWatchRelay creates a new WatchRelay with the given database.
// If underlying database connection is not a *sql.DB, like in a transaction, it will returns error.
func NewWatchRelay(db *gorm.DB) (w *WatchRelay, err error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	var (
		dialect  Dialect
		startRev uint64
	)
	switch db.Dialector.Name() {
	case "mysql":
		dialect, startRev, err = mysql.New(sqlDB)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("watchrelay: unsupported database dialect")
	}

	sqlLog := &SQLLog{
		d: dialect,
	}
	w = &WatchRelay{
		seq:    NewSequence(startRev),
		sqlLog: sqlLog,
		db:     db,
	}
	return
}

// BatchHook is executed before or after creating, updating, or deleting resources in the database.
type BatchHook[T IVersionedResource] func(*gorm.DB, ...T) error

// Hook is executed before or after creating, updating, or deleting resource in the database.
type Hook[T IVersionedResource] func(*gorm.DB, T) error

// Create creates new resources and event logs in the database.
func Create[T IVersionedResource](w *WatchRelay, ctx context.Context, beforeCreate, afterCreate BatchHook[T], resources ...T) error {
	if w == nil {
		return errors.New("watchrelay: WatchRelay is nil")
	}
	db := w.db

	fn := func(tx *gorm.DB) error {
		if beforeCreate != nil {
			err := beforeCreate(tx, resources...)
			if err != nil {
				return err
			}
		}

		events := make([]*LogEvent, len(resources))
		for i, resource := range resources {
			resource.SetResourceVersion(w.seq.Next())

			b, err := json.Marshal(resource)
			if err != nil {
				return err
			}
			value := datatypes.JSON(b)

			resourceName := GetResourceName(resource)
			events[i] = &LogEvent{
				Revision:       resource.GetResourceVersion(),
				CreateRevision: resource.GetResourceVersion(),
				PrevRevision:   0,
				ResourceName:   resourceName,
				Created:        true,
				Deleted:        false,
				Value:          value,
				CreatedAt:      time.Now(),
			}
		}

		if err := tx.Create(resources).Error; err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Create(events).Error; err != nil {
			tx.Rollback()
			return err
		}

		if afterCreate != nil {
			err := afterCreate(tx, resources...)
			if err != nil {
				tx.Rollback()
				return err
			}
		}

		return nil
	}

	return db.WithContext(ctx).Transaction(fn)
}

// Update updates resources and event logs in the database.
func Update[T IVersionedResource](w *WatchRelay, ctx context.Context, beforeUpdate, afterUpdate Hook[T], resource T) error {
	if w == nil {
		return errors.New("watchrelay: WatchRelay is nil")
	}
	db := w.db
	tx := db.Begin()

	if beforeUpdate != nil {
		err := beforeUpdate(tx, resource)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	resource.SetResourceVersion(w.seq.Next())
	b, err := json.Marshal(resource)
	if err != nil {
		tx.Rollback()
		return err
	}
	value := datatypes.JSON(b)

	event := &Event{
		Revision:     resource.GetResourceVersion(),
		ResourceName: GetResourceName(resource),
		Action:       EventActionUpdate,
		Value:        value,
	}

	if err := tx.Save(resource).Error; err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Create(event).Error; err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

func Patch[T IVersionedResource](w *WatchRelay, ctx context.Context, beforePatch, afterPatch Hook[T], resource T) error {
	if w == nil {
		return errors.New("watchrelay: WatchRelay is nil")
	}
	db := w.db
	tx := db.Begin()

	if beforePatch != nil {
		err := beforePatch(tx, resource)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	resource.SetResourceVersion(w.seq.Next())
	b, err := json.Marshal(resource)
	if err != nil {
		tx.Rollback()
		return err
	}
	value := datatypes.JSON(b)

	event := &Event{
		Revision:     resource.GetResourceVersion(),
		ResourceName: GetResourceName(resource),
		Action:       EventActionUpdate,
		Value:        value,
	}

	if err := tx.Save(resource).Error; err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Create(event).Error; err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

// Delete deletes resources and event logs in the database.
func Delete[T IVersionedResource](w *WatchRelay, ctx context.Context, beforeDelete, afterDelete BatchHook[T], resources ...T) error {
	if w == nil {
		return errors.New("watchrelay: WatchRelay is nil")
	}
	db := w.db
	tx := db.Begin()

	if beforeDelete != nil {
		err := beforeDelete(tx, resources...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	events := make([]*Event, len(resources))
	for i, resource := range resources {
		resource.SetResourceVersion(w.seq.Next())
		b, err := json.Marshal(resource)
		if err != nil {
			tx.Rollback()
			return err
		}
		value := datatypes.JSON(b)

		events[i] = &Event{
			Revision:     resource.GetResourceVersion(),
			ResourceName: GetResourceName(resource),
			Action:       EventActionDelete,
			Value:        value,
		}
	}

	if err := tx.Delete(resources).Error; err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Create(events).Error; err != nil {
		tx.Rollback()
		return err
	}

	if afterDelete != nil {
		err := afterDelete(tx, resources...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit().Error
}

func After(w *WatchRelay, ctx context.Context, resourceName string, rev uint64, limit int64) (uint64, []*Event, error) {
	return w.sqlLog.After(ctx, resourceName, rev, limit)
}

func Watch(ctx context.Context, resourceName string, rev uint64) (chan []*Event, error) {
	return nil, nil
}
