package clickhouse

import (
	"fmt"
	"strings"
)

type Model interface {
	GetFields() []string
	GetValues() []interface{}
	TableName() string
}

// PrepareInsertionSQL подготавливает SQL prepare statement для вставки записей
// в базу данных.
func PrepareInsertionSQL(model Model) string {
	fields := model.GetFields()
	binds := strings.Repeat("?,", len(fields))
	binds = binds[:len(binds)-1]

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", model.TableName(), strings.Join(fields, ", "), binds)
}
