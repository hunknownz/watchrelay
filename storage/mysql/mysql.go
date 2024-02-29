package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/hunknownz/watchrelay/storage/generic"
	"github.com/sirupsen/logrus"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS watchrelay
			(
				revision bigint(20) unsigned NOT NULL,
				create_revision bigint(20) unsigned,
				prev_revision bigint(20) unsigned,
				resource_name VARCHAR(511) CHARACTER SET ascii,
				created BOOLEAN,
				deleted BOOLEAN,
				value MEDIUMBLOB,
				created_at datetime(3) DEFAULT NULL,
				PRIMARY KEY (revision)
			);`,
		`CREATE INDEX watchrelay_resource_name_index ON watchrelay (resource_name)`,
		`CREATE INDEX watchrelay_resource_name_revision_index ON watchrelay (resource_name,revision)`,
		`CREATE INDEX watchrelay_revision_deleted_index ON watchrelay (revision,deleted)`,
	}
)

type MysqlDialect struct {
	db *sql.DB

	AfterSQL    string
	AfterAllSQL string
	RevSQL      string
}

func (d *MysqlDialect) After(ctx context.Context, resourceName string, revision uint64, limit int64) (*sql.Rows, error) {
	var query string
	if resourceName == "" {
		query = d.AfterAllSQL
	} else {
		query = d.AfterSQL
	}
	if limit > 0 {
		query = fmt.Sprintf("%s LIMIT %d", query, limit)
	}
	if resourceName == "" {
		return d.db.QueryContext(ctx, query, revision)
	}
	return d.db.QueryContext(ctx, query, resourceName, revision)
}

func (d *MysqlDialect) CurrentRevision(ctx context.Context) (uint64, error) {
	var sqlRev sql.NullInt64
	err := d.db.QueryRowContext(ctx, d.RevSQL).Scan(&sqlRev)
	if err != nil {
		return 0, err
	}
	var rev uint64
	if sqlRev.Valid {
		rev = uint64(sqlRev.Int64)
	} else {
		rev = 0
	}
	return rev, nil
}

func (d *MysqlDialect) ClearExpiredEvents(ctx context.Context, dur time.Duration) (int, error) {
	return 0, nil
}

func New(db *sql.DB) (*MysqlDialect, uint64, error) {
	var exists bool
	err := db.QueryRow("SELECT 1 FROM information_schema.TABLES WHERE table_schema = DATABASE() AND table_name = ?", "watchrelay").Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf("failed to check if table %s exists: %v", "watchrelay", err)
	}

	if !exists {
		for _, stmt := range schema {
			_, err := db.Exec(stmt)
			if err != nil {
				// If the table already exists, we can ignore the error.
				if mysqlError, ok := err.(*mysql.MySQLError); !ok || mysqlError.Number != 1061 {
					return nil, 0, err
				}
			}
		}
	}

	dialect := &MysqlDialect{
		db: db,

		AfterSQL: fmt.Sprintf(`
			SELECT (%s), %s
			FROM watchrelay AS log
			WHERE
			    log.resource_name = ? AND
				log.revision > ?
			ORDER BY log.revision ASC`, generic.RevisionSQL, generic.Columns),
		AfterAllSQL: fmt.Sprintf(`
			SELECT (%s), %s
			FROM watchrelay AS log
			WHERE
				log.revision > ?
			ORDER BY log.revision ASC`, generic.RevisionSQL, generic.Columns),
		RevSQL: generic.RevisionSQL,
	}

	rev, err := dialect.CurrentRevision(context.Background())
	if err != nil {
		return nil, 0, err
	}

	return dialect, rev, nil
}
