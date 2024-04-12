package watchrelay

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/hunknownz/watchrelay/event"
	"github.com/hunknownz/watchrelay/sqllog"
	"github.com/sirupsen/logrus"
)

const (
	pollBatchSize = 512
	pollInterval  = time.Second
)

type SQLLog struct {
	d          sqllog.Dialect
	ctx        context.Context
	currentRev uint64
	notify     chan uint64

	fMutex       sync.Mutex
	eventFuncMap map[string]event.EventFunc
}

func New(d sqllog.Dialect) *SQLLog {
	l := &SQLLog{
		d:      d,
		notify: make(chan uint64, 1024),
	}
	return l
}

func (s *SQLLog) IsRegisterd(resourceName string) bool {
	_, ok := s.eventFuncMap[resourceName]
	return ok
}

func (s *SQLLog) RowsToEvents(rows *sql.Rows) (rev uint64, events []event.IEvent, err error) {
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

		var action event.EventAction
		if created {
			action = event.EventActionCreate
		} else if deleted {
			action = event.EventActionDelete
		} else {
			action = event.EventActionUpdate
		}
		generateFunc, ok := s.eventFuncMap[resourceName]
		if !ok {
			logrus.Errorf("watchrelay: no event function for resource %s", resourceName)
			continue
		}

		event, err := generateFunc(revision, createRevision, action, createdAt, value)
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

func (s *SQLLog) After(ctx context.Context, resourceName string, revision uint64, limit int64) (rev uint64, events []event.IEvent, err error) {
	rows, afterErr := s.d.After(ctx, resourceName, revision, limit)
	if afterErr != nil {
		err = afterErr
		return
	}
	return s.RowsToEvents(rows)
}

func (s *SQLLog) Watch(ctx context.Context) error {
	return nil
}

func (s *SQLLog) startWatch() (chan []event.IEvent, error) {
	startRev, err := s.d.CurrentRevision(s.ctx)
	if err != nil {
		return nil, err
	}

	ch := make(chan []event.IEvent)
	go s.poll(ch, startRev)
	return ch, nil
}

func (s *SQLLog) poll(result chan []event.IEvent, startRev uint64) {
	s.currentRev = startRev

	var (
		// skip        uint64
		// skipTime    time.Time
		waitForMore = true
	)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	defer close(result)

	for {
		if waitForMore {
			select {
			case <-s.ctx.Done():
				return
			case check := <-s.notify:
				if check <= s.currentRev {
					continue
				}
			case <-ticker.C:
			}
		}
		waitForMore = true

		rows, err := s.d.After(s.ctx, "", s.currentRev, pollBatchSize)
		if err != nil {
			logrus.Errorf("watchrelay: failed to list after %d: %v", s.currentRev, err)
			continue
		}

		_, events, err := s.RowsToEvents(rows)
		if err != nil {
			logrus.Errorf("watchrelay: failed to convert rows to events: %v", err)
			continue
		}

		if len(events) == 0 {
			continue
		}

		waitForMore = len(events) < 128

		rev := s.currentRev
		var (
			seq      []event.IEvent
			saveLast bool
		)

		for _, event := range events {
			next := rev + 1
			if event.GetRevision() != next {

			}

			saveLast = true
			rev = event.GetRevision()
			if !event.IsGap() {
				seq = append(seq, event)
			}
		}

		if saveLast {
			s.currentRev = rev
			if len(seq) > 0 {
				result <- seq
			}
		}
	}
}
