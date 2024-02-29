package watchrelay

import (
	"context"
	"database/sql"
	"time"
)

type SQLLog struct {
	d          Dialect
	ctx        context.Context
	currentRev uint64
}

func New(d Dialect) *SQLLog {
	l := &SQLLog{
		d: d,
	}
	return l
}

func RowsToEvents(rows *sql.Rows) (rev uint64, events []*Event, err error) {
	defer rows.Close()

	for rows.Next() {
		var (
			revision, createRevision uint64
			created, deleted         bool
			resourceName             string
			value                    []byte
			createdAt                time.Time
		)
		if err := rows.Scan(&rev, &revision, &createRevision, &resourceName, &created, &deleted, &value, &createdAt); err != nil {
			return 0, nil, err
		}

		var action EventAction
		if created {
			action = EventActionCreate
		} else if deleted {
			action = EventActionDelete
		} else {
			action = EventActionUpdate
		}
		event := &Event{
			CreateRevision: createRevision,
			Revision:       revision,
			ResourceName:   resourceName,
			Action:         action,
			Value:          value,
			CreatedAt:      createdAt,
		}
		if err != nil {
			return 0, nil, err
		}
		events = append(events, event)

	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	return rev, events, nil
}

func (s *SQLLog) After(ctx context.Context, resourceName string, revision uint64, limit int64) (rev uint64, events []*Event, err error) {
	rows, afterErr := s.d.After(ctx, resourceName, revision, limit)
	if afterErr != nil {
		err = afterErr
		return
	}
	return RowsToEvents(rows)
}

func (s *SQLLog) Watch(ctx context.Context) error {
	return nil
}

func (s *SQLLog) startWatch() (chan []*Event, error) {
	startRev, err := s.d.CurrentRevision(s.ctx)
	if err != nil {
		return nil, err
	}

	ch := make(chan []*Event)
	go s.poll(ch, startRev)
	return ch, nil
}

func (s *SQLLog) poll(result chan []*Event, startRev uint64) {
	s.currentRev = startRev

	var (
		// skip        uint64
		// skipTime    time.Time
		waitForMore = true
	)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(result)

	for {
		if waitForMore {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
			}
		}
		waitForMore = true
	}
}
