package go_mocket

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/rand"
	"strings"
)

type FakeStmt struct {
	connection   *FakeConn
	q            string    // just for debugging SQL query generated by sql package
	command      string    // String name of the command SELECT etc, taken as first word in the query
	next         *FakeStmt // used for returning multiple results.
	closed       bool      // If connection closed already
	colName      []string  //Names of columns in response
	colType      []string  // Not used for now
	placeholders int       // Amount of passed args
}

func (s *FakeStmt) ColumnConverter(idx int) driver.ValueConverter {
	return driver.DefaultParameterConverter
}

func (s *FakeStmt) Close() error {
	// No connection added
	if s.connection == nil {
		panic("nil conn in FakeStmt.Close")
	}
	if s.connection.db == nil {
		panic("in FakeStmt.Close, conn's db is nil (already closed)")
	}
	if !s.closed {
		s.closed = true
	}
	if s.next != nil {
		s.next.Close()
	}
	return nil
}

var errClosed = errors.New("fake_db_driver: statement has been closed")

func (smt *FakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	panic("Using ExecContext")
}

func (smt *FakeStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if smt.closed {
		return nil, errClosed
	}

	fResp := Catcher.FindResponse(smt.q, args)

	// To emulate any exception during query which returns rows
	if fResp.Exceptions != nil && fResp.Exceptions.HookExecBadConnection != nil && fResp.Exceptions.HookExecBadConnection() {
		return nil, driver.ErrBadConn
	}

	if fResp.Callback != nil {
		fResp.Callback(smt.q, args)
	}

	switch smt.command {
	case "INSERT":
		id := fResp.LastInsertId
		if id == 0 {
			id = rand.Int63()
		}
		res := NewFakeResult(id, 1)
		return res, nil
	case "UPDATE":
		return driver.RowsAffected(fResp.RowsAffected), nil
	case "DELETE":
		return driver.RowsAffected(fResp.RowsAffected), nil
	}
	return nil, fmt.Errorf("unimplemented statement Exec command type of %q", smt.command)
}

func (s *FakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("Use QueryContext")
}

func (smt *FakeStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {

	if smt.closed {
		return nil, errClosed
	}

	if len(args) > 0 {
		// Replace all "?" to "%v" and replace them with the values after
		for i := 0; i < len(args); i++ {
			smt.q = strings.Replace(smt.q, "?", "%v", 1)
			smt.q = fmt.Sprintf(smt.q, args[i].Value)
		}
	}

	fResp := Catcher.FindResponse(smt.q, args)

	if fResp.Exceptions != nil && fResp.Exceptions.HookQueryBadConnection != nil && fResp.Exceptions.HookQueryBadConnection() {
		return nil, driver.ErrBadConn
	}

	resultRows := make([][]*row, 0, 1)
	columnNames := make([]string, 0, 1)
	columnTypes := make([][]string, 0, 1)
	rows := []*row{}

	// Check if we have such query in the map
	colIndexes := make(map[string]int)

	// Collecting column names from first record
	if len(fResp.Response) > 0 {
		for colName, _ := range fResp.Response[0] {
			columnNames = append(columnNames, colName)
			colIndexes[colName] = len(columnNames) - 1
		}
	}

	// Extracting values from result according columns
	for _, record := range fResp.Response {
		oneRow := &row{cols: make([]interface{}, len(columnNames))}
		for _, col := range columnNames {
			oneRow.cols[colIndexes[col]] = []byte(record[col].(string))
		}
		rows = append(rows, oneRow)
	}
	resultRows = append(resultRows, rows)

	cursor := &RowsCursor{
		posRow:  -1,
		rows:    resultRows,
		cols:    columnNames,
		colType: columnTypes, // TODO: implement support of that
		errPos:  -1,
		closed:  false,
	}

	if fResp.Callback != nil {
		fResp.Callback(smt.q, args)
	}

	return cursor, nil
}

// Returns number of args passed to query
func (s *FakeStmt) NumInput() int {
	return s.placeholders
}

type FakeTx struct {
	c *FakeConn
}

// hook to simulate broken connections
var HookBadCommit func() bool

func (tx *FakeTx) Commit() error {
	tx.c.currTx = nil
	if HookBadCommit != nil && HookBadCommit() {
		return driver.ErrBadConn
	}
	return nil
}

// hook to simulate broken connections
var HookBadRollback func() bool

func (tx *FakeTx) Rollback() error {
	tx.c.currTx = nil
	if HookBadRollback != nil && HookBadRollback() {
		return driver.ErrBadConn
	}
	return nil
}