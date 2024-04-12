package watchrelay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hunknownz/watchrelay/event"
	"github.com/hunknownz/watchrelay/resource"
	"github.com/hunknownz/watchrelay/sqllog"
	"github.com/hunknownz/watchrelay/storage/mysql"
	"github.com/sirupsen/logrus"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type WatchRelay struct {
	seq    *Sequence
	sqlLog *sqllog.SQLLog

	db      *gorm.DB
	dialect sqllog.Dialect
}

type WatchResult[T resource.IVersionedResource] struct {
	Revision uint64
	Events   chan []*event.Event[T]
}

type ConditionFunc[T resource.IVersionedResource] func(v T) bool

func RegisterResource[T resource.IVersionedResource](w *WatchRelay) error {
	var res T
	resourceName := resource.GetResourceName(res)
	fn := func(rv, createRv uint64, action event.EventAction, createdAt time.Time, v []byte) (event.IEvent, error) {
		t := new(T)
		err := json.Unmarshal(v, t)
		if err != nil {
			return nil, err
		}
		return &event.Event[T]{
			Value:          *t,
			CreateRevision: createRv,
			Revision:       rv,
			Action:         action,
			ResourceName:   resourceName,
			CreatedAt:      createdAt,
		}, nil
	}
	w.sqlLog.Register(resourceName, fn)

	return nil
}

// NewWatchRelay creates a new WatchRelay with the given database.
// If underlying database connection is not a *sql.DB, like in a transaction, it will returns error.
func NewWatchRelay(db *gorm.DB) (w *WatchRelay, err error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	var (
		dialect  sqllog.Dialect
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

	w = &WatchRelay{
		seq:    NewSequence(startRev),
		sqlLog: sqllog.NewSQLLog(dialect),
		db:     db,
	}
	return
}

func (w *WatchRelay) Start(ctx context.Context) {
	w.sqlLog.Start(ctx)
}

// BatchHook is executed before or after creating, updating, or deleting resources in the database.
type BatchHook[T resource.IVersionedResource] func(*gorm.DB, ...T) error

// Hook is executed before or after creating, updating, or deleting resource in the database.
type Hook[T resource.IVersionedResource] func(*gorm.DB, T) error

// Create creates new resources and event logs in the database.
func Create[T resource.IVersionedResource](w *WatchRelay, ctx context.Context, beforeCreate, afterCreate BatchHook[T], resources ...T) error {
	if w == nil {
		return errors.New("watchrelay: WatchRelay is nil")
	}

	if len(resources) == 0 {
		return nil
	}

	resourceName := resource.GetResourceName(resources[0])
	if !w.sqlLog.IsRegisterd(resourceName) {
		return fmt.Errorf("watchrelay: resource %s not registered", resourceName)
	}

	db := w.db

	fn := func(tx *gorm.DB) error {
		if beforeCreate != nil {
			err := beforeCreate(tx, resources...)
			if err != nil {
				return err
			}
		}

		events := make([]*event.LogEvent, len(resources))
		for i, res := range resources {
			res.SetResourceVersion(w.seq.Next())

			b, err := json.Marshal(res)
			if err != nil {
				return err
			}
			value := datatypes.JSON(b)

			events[i] = &event.LogEvent{
				Revision:       res.GetResourceVersion(),
				CreateRevision: res.GetResourceVersion(),
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
func Update[T resource.IVersionedResource](w *WatchRelay, ctx context.Context, beforeUpdate, afterUpdate Hook[T], res T) error {
	if w == nil {
		return errors.New("watchrelay: WatchRelay is nil")
	}

	resourceName := resource.GetResourceName(res)
	if !w.sqlLog.IsRegisterd(resourceName) {
		return fmt.Errorf("watchrelay: resource %s not registered", resourceName)
	}

	db := w.db
	tx := db.Begin()

	if beforeUpdate != nil {
		err := beforeUpdate(tx, res)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	res.SetResourceVersion(w.seq.Next())

	event := &event.Event[T]{
		Revision:     res.GetResourceVersion(),
		ResourceName: resourceName,
		Action:       event.EventActionUpdate,
		Value:        res,
	}

	if err := tx.Save(res).Error; err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Create(event).Error; err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

func Patch[T resource.IVersionedResource](w *WatchRelay, ctx context.Context, beforePatch, afterPatch Hook[T], res T) error {
	if w == nil {
		return errors.New("watchrelay: WatchRelay is nil")
	}

	resourceName := resource.GetResourceName(res)
	if !w.sqlLog.IsRegisterd(resourceName) {
		return fmt.Errorf("watchrelay: resource %s not registered", resourceName)
	}

	db := w.db
	tx := db.Begin()

	if beforePatch != nil {
		err := beforePatch(tx, res)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	res.SetResourceVersion(w.seq.Next())

	event := &event.Event[T]{
		Revision:     res.GetResourceVersion(),
		ResourceName: resource.GetResourceName(res),
		Action:       event.EventActionUpdate,
		Value:        res,
	}

	if err := tx.Save(res).Error; err != nil {
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
func Delete[T resource.IVersionedResource](w *WatchRelay, ctx context.Context, beforeDelete, afterDelete BatchHook[T], resources ...T) error {
	if w == nil {
		return errors.New("watchrelay: WatchRelay is nil")
	}

	if len(resources) == 0 {
		return nil
	}

	resourceName := resource.GetResourceName(resources[0])
	if !w.sqlLog.IsRegisterd(resourceName) {
		return fmt.Errorf("watchrelay: resource %s not registered", resourceName)
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

	events := make([]*event.Event[T], len(resources))
	for i, res := range resources {
		res.SetResourceVersion(w.seq.Next())

		events[i] = &event.Event[T]{
			Revision:     res.GetResourceVersion(),
			ResourceName: resourceName,
			Action:       event.EventActionDelete,
			Value:        res,
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

func After[T resource.IVersionedResource](w *WatchRelay, ctx context.Context, cond ConditionFunc[T], rev uint64, limit int64) (uint64, []*event.Event[T], error) {
	if w == nil {
		return 0, nil, errors.New("watchrelay: WatchRelay is nil")
	}

	var t T
	resourceName := resource.GetResourceName(t)
	if !w.sqlLog.IsRegisterd(resourceName) {
		return 0, nil, fmt.Errorf("watchrelay: resource %s not registered", resourceName)
	}
	rev, iEvents, err := w.sqlLog.After(ctx, resourceName, rev, limit)
	if err != nil {
		return 0, nil, err
	}
	events := make([]*event.Event[T], 0, len(iEvents))
	for i := range iEvents {
		event, ok := iEvents[i].(*event.Event[T])
		if !ok {
			logrus.Errorf("watchrelay: invalid event type %T", iEvents[i])
			continue
		}
		if cond != nil && !cond(event.Value) {
			continue
		}
		events = append(events, event)
	}

	return rev, events, nil
}

func Watch[T resource.IVersionedResource](w *WatchRelay, ctx context.Context, cond ConditionFunc[T], rev uint64) WatchResult[T] {
	eventFilter := func(events []*event.Event[T]) ([]*event.Event[T], bool) {
		if cond == nil {
			return events, true
		}

		filtered := make([]*event.Event[T], 0, len(events))
		for _, event := range events {
			v := event.GetValue().(T)
			if cond(v) {
				filtered = append(filtered, event)
			}
		}
		return filtered, len(filtered) > 0
	}

	// start watch
	ctx, cancel := context.WithCancel(ctx)
	readCh := sqllog.Watch[T](w.sqlLog, ctx, eventFilter)

	// should contain current resource version
	if rev > 0 {
		rev--
	}

	results := make(chan []*event.Event[T], 128)
	watchResult := WatchResult[T]{
		Revision: rev,
		Events:   results,
	}

	curRev, events, err := After[T](w, ctx, cond, rev, 0)
	if err != nil {
		logrus.Errorf("watchrelay: failed to list events after revision %d: %v", rev, err)
		cancel()
	}

	go func() {
		defer func() {
			close(results)
			cancel()
		}()

		lastRev := rev
		if len(events) > 0 {
			lastRev = curRev
		}

		if len(events) > 0 {
			results <- events
		}

		for value := range readCh {
			events, ok := filter[T](value, lastRev)
			if !ok {
				results <- events
			}
		}
	}()

	return watchResult
}

func filter[T resource.IVersionedResource](events []*event.Event[T], rev uint64) ([]*event.Event[T], bool) {
	for len(events) > 0 && events[0].Revision <= rev {
		events = events[1:]
	}
	return events, len(events) > 0
}
