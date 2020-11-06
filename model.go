package clickhouse

import (
	"fmt"
	"strings"
)

// Model is an interface for ClickHouse data structures.
type Model interface {
	GetFields() []string
	GetValues() []interface{}
	TableName() string
}

// PrepareInsertionSQL prepares a SQL prepare statement to insert records
// into the database.
func PrepareInsertionSQL(model Model) string {
	fields := model.GetFields()
	binds := strings.Repeat("?,", len(fields))
	binds = binds[:len(binds)-1]

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", model.TableName(), strings.Join(fields, ", "), binds)
}
