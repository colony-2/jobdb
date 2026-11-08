package sqlite

import (
	"database/sql/driver"
	sqlitedriver "modernc.org/sqlite"
	"sync/atomic"
)

var leaseEventExamined atomic.Int64

func init() {
	sqlitedriver.MustRegisterScalarFunction("jobdb_test_lease_event_examined", 1, func(_ *sqlitedriver.FunctionContext, _ []driver.Value) (driver.Value, error) {
		leaseEventExamined.Add(1)
		return int64(1), nil
	})
}
