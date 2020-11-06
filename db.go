package clickhouse

import (
	"fmt"
	"os"
	"time"

	// Import ClickHouse driver.
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
)

// DB is a wrapper around sql.DB which keeps track of the ClickHouse database.
type DB struct {
	config *Config
	db     *sqlx.DB
}

// NewDB creates new connection to ClickHouse using SQLX.
func NewDB(cfg *Config) (*DB, error) {
	if cfg.ZoneInfo != "" {
		if err := os.Setenv("ZONEINFO", cfg.ZoneInfo); err != nil {
			return nil, err
		}
	}

	db, err := sqlx.Open("clickhouse", cfg.GetDSN())
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		if err2 := db.Close(); err2 != nil {
			return nil, fmt.Errorf("multiple errors: %w, %v", err, err2)
		}

		return nil, err
	}

	return &DB{config: cfg, db: db}, nil
}

// Config returns config.
func (db *DB) Config() *Config {
	return db.config
}

// DB returns pointer to sqlx.DB.
func (db *DB) DB() *sqlx.DB {
	return db.db
}

// Close closes connection to database.
func (db *DB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

// IsConnected checks connection status to database.
func (db *DB) IsConnected() bool {
	if db.db == nil {
		return false
	}

	if err := db.db.Ping(); err != nil {
		return false
	}

	return true
}

// GetServerTime gets and returns database server time.
func (db *DB) GetServerTime() (time.Time, error) {
	var st time.Time

	row := db.db.QueryRow("SELECT now()")
	err := row.Scan(&st)

	return st, err
}

// MultiInsert creates transaction to batch insertions.
func (db *DB) MultiInsert(query string, rows [][]interface{}) error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		if err2 := tx.Rollback(); err2 != nil {
			return fmt.Errorf("multiple errors: %w, %v", err, err2)
		}

		return err
	}

	defer stmt.Close()

	for i := range rows {
		if _, err := stmt.Exec(rows[i]...); err != nil {
			if err2 := tx.Rollback(); err2 != nil {
				return fmt.Errorf("multiple errors: %w, %v", err, err2)
			}
		}
	}

	return tx.Commit()
}
