package watchrelay

import (
	"context"
	"database/sql"
	"time"
)

type Dialect interface {
	After(ctx context.Context, resourceName string, revision uint64, limit int64) (*sql.Rows, error)
	ClearExpiredEvents(ctx context.Context, dur time.Duration) (int, error)
	CurrentRevision(ctx context.Context) (uint64, error)
}
