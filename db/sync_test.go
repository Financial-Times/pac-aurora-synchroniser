package db

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTablesWithLastModifiedFrom(t *testing.T) {
	sourceDB, err := New(getSourceTestDatabaseURL(t), 10)
	require.NoError(t, err)
	err = createTestTableWithField(sourceDB, "test_table1", []string{"uuid"}, "body", lastModifiedField, "pub_ref")
	require.NoError(t, err)
	err = createTestTableWithField(sourceDB, "test_table2", []string{"id"}, "pluto", "draft_ref", lastModifiedField)
	require.NoError(t, err)
	err = createTestTableWithField(sourceDB, "test_table3", []string{"uuid"}, "body", "pub_ref")
	require.NoError(t, err)

	tables, err := getTablesWithLastModifiedFrom(sourceDB)
	assert.NoError(t, err)
	assert.Contains(t, tables, "test_table1")
	assert.Contains(t, tables, "test_table2")
	assert.NotContains(t, tables, "test_table3")

	err = dropTables(sourceDB, "test_table1", "test_table2", "test_table3")
	require.NoError(t, err)
}

var testRow1 = map[string]string{
	"id":              "1",
	lastModifiedField: "2018-04-19T12:54:46.814Z",
	"body":            "{body-1}",
	"draft_ref":       "tid_001",
}

var testRow2 = map[string]string{
	"id":              "2",
	lastModifiedField: "2018-04-20T13:04:42.814Z",
	"body":            "{body-2}",
	"draft_ref":       "tid_002",
}

var testRow3 = map[string]string{
	"id":              "3",
	lastModifiedField: "2018-04-07T11:50:46.112Z",
	"body":            "{body-3}",
	"draft_ref":       "tid_003",
}

var moreRecentTestRow2 = map[string]string{
	"id":              "2",
	lastModifiedField: "2018-04-25T00:04:22.814Z",
	"body":            "{new-body-2}",
	"draft_ref":       "new_tid_002",
}

var moreRecentTestRow3 = map[string]string{
	"id":              "3",
	lastModifiedField: "2018-04-10T20:51:43.142Z",
	"body":            "{new-body-3}",
	"draft_ref":       "new_tid_003",
}

func TestSyncTableDoesUpdateNewRowFromSource(t *testing.T) {
	sourceDB, err := New(getSourceTestDatabaseURL(t), 10)
	require.NoError(t, err)
	targetDB, err := New(getTargetTestDatabaseURL(t), 10)
	require.NoError(t, err)

	err = createTestTableWithField(sourceDB, "test_table", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = addRowsToTable(sourceDB, "test_table", testRow1, moreRecentTestRow2, moreRecentTestRow3)
	require.NoError(t, err)

	err = createTestTableWithField(targetDB, "test_table", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = addRowsToTable(targetDB, "test_table", testRow1, testRow3, testRow2)
	require.NoError(t, err)

	err = syncTable("test_table", sourceDB, targetDB)
	assert.NoError(t, err)
	assertNumberOfRows(t, targetDB, "test_table", 3)
	assertContainsRow(t, targetDB, "test_table", testRow1)
	assertContainsRow(t, targetDB, "test_table", moreRecentTestRow2)
	assertContainsRow(t, targetDB, "test_table", moreRecentTestRow3)
	assertNotContainsRow(t, targetDB, "test_table", testRow2)
	assertNotContainsRow(t, targetDB, "test_table", testRow3)

	err = dropTables(sourceDB, "test_table")
	require.NoError(t, err)
	err = dropTables(targetDB, "test_table")
	require.NoError(t, err)
}

func TestSyncTableDoesNotUpdateOldRowFromSource(t *testing.T) {
	sourceDB, err := New(getSourceTestDatabaseURL(t), 10)
	require.NoError(t, err)
	targetDB, err := New(getTargetTestDatabaseURL(t), 10)
	require.NoError(t, err)

	err = createTestTableWithField(sourceDB, "test_table", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = addRowsToTable(sourceDB, "test_table", testRow1, testRow2, testRow3)
	require.NoError(t, err)

	err = createTestTableWithField(targetDB, "test_table", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = addRowsToTable(targetDB, "test_table", testRow1, moreRecentTestRow3, moreRecentTestRow2)
	require.NoError(t, err)

	err = syncTable("test_table", sourceDB, targetDB)

	assert.NoError(t, err)
	assertNumberOfRows(t, targetDB, "test_table", 3)
	assertContainsRow(t, targetDB, "test_table", testRow1)
	assertContainsRow(t, targetDB, "test_table", moreRecentTestRow2)
	assertContainsRow(t, targetDB, "test_table", moreRecentTestRow3)
	assertNotContainsRow(t, targetDB, "test_table", testRow2)
	assertNotContainsRow(t, targetDB, "test_table", testRow3)

	err = dropTables(sourceDB, "test_table")
	require.NoError(t, err)
	err = dropTables(targetDB, "test_table")
	require.NoError(t, err)
}

func TestSyncTableCreateRowIfMissingInTarget(t *testing.T) {
	sourceDB, err := New(getSourceTestDatabaseURL(t), 10)
	require.NoError(t, err)
	targetDB, err := New(getTargetTestDatabaseURL(t), 10)
	require.NoError(t, err)

	err = createTestTableWithField(sourceDB, "test_table", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = addRowsToTable(sourceDB, "test_table", testRow1, testRow2, testRow3)
	require.NoError(t, err)

	err = createTestTableWithField(targetDB, "test_table", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = addRowsToTable(targetDB, "test_table", testRow1)
	require.NoError(t, err)

	err = syncTable("test_table", sourceDB, targetDB)

	assert.NoError(t, err)
	assertNumberOfRows(t, targetDB, "test_table", 3)
	assertContainsRow(t, targetDB, "test_table", testRow1)
	assertContainsRow(t, targetDB, "test_table", testRow2)
	assertContainsRow(t, targetDB, "test_table", testRow3)

	err = dropTables(sourceDB, "test_table")
	require.NoError(t, err)
	err = dropTables(targetDB, "test_table")
	require.NoError(t, err)
}

func TestSyncDatabases(t *testing.T) {
	sourceDB, err := New(getSourceTestDatabaseURL(t), 10)
	require.NoError(t, err)
	err = createTestTableWithField(sourceDB, "test_table1", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = createTestTableWithField(sourceDB, "test_table2", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = createTestTableWithField(sourceDB, "test_table3", []string{"uuid"}, "body", "pub_ref")
	require.NoError(t, err)

	targetDB, err := New(getTargetTestDatabaseURL(t), 10)
	require.NoError(t, err)
	err = createTestTableWithField(targetDB, "test_table1", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = createTestTableWithField(targetDB, "test_table2", []string{"id"}, lastModifiedField, "body", "draft_ref")
	require.NoError(t, err)
	err = createTestTableWithField(targetDB, "test_table3", []string{"uuid"}, "body", "pub_ref")
	require.NoError(t, err)

	err = addRowsToTable(sourceDB, "test_table1", testRow1, testRow2)
	err = addRowsToTable(targetDB, "test_table1", testRow1, moreRecentTestRow3)

	err = addRowsToTable(sourceDB, "test_table2", testRow2, moreRecentTestRow3)
	err = addRowsToTable(targetDB, "test_table2", testRow1, moreRecentTestRow2)

	err = Sync(sourceDB, targetDB)
	assert.NoError(t, err)
	assertNumberOfRows(t, targetDB, "test_table1", 3)
	assertContainsRow(t, targetDB, "test_table1", testRow1)
	assertContainsRow(t, targetDB, "test_table1", testRow2)
	assertContainsRow(t, targetDB, "test_table1", moreRecentTestRow3)
	assertNumberOfRows(t, targetDB, "test_table2", 3)
	assertContainsRow(t, targetDB, "test_table2", testRow1)
	assertContainsRow(t, targetDB, "test_table2", moreRecentTestRow2)
	assertContainsRow(t, targetDB, "test_table2", moreRecentTestRow3)

	err = dropTables(sourceDB, "test_table1", "test_table2", "test_table3")
	require.NoError(t, err)
	err = dropTables(targetDB, "test_table1", "test_table2", "test_table3")
	require.NoError(t, err)

}

func assertNumberOfRows(t *testing.T, db *sql.DB, table string, n int) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM " + table + "").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, n, count)
}

func assertContainsRow(t *testing.T, db *sql.DB, table string, row map[string]string) {
	header := "SELECT * FROM " + table + " WHERE"
	whereStmt := ""
	var args []interface{}
	for field, value := range row {
		whereStmt += "AND " + field + "=? "
		args = append(args, value)
	}

	query := header + whereStmt[3:] + ";"
	rows, err := db.Query(query, args...)
	require.NoError(t, err)
	defer rows.Close()
	assert.True(t, rows.Next())
	columns, err := rows.Columns()
	assert.Equal(t, len(row), len(columns))
}

func assertNotContainsRow(t *testing.T, db *sql.DB, table string, row map[string]string) {
	header := "SELECT * FROM " + table + " WHERE"
	whereStmt := ""
	var args []interface{}
	for field, value := range row {
		whereStmt += "AND " + field + "=? "
		args = append(args, value)
	}

	query := header + whereStmt[3:] + ";"
	rows, err := db.Query(query, args...)
	require.NoError(t, err)
	defer rows.Close()
	assert.False(t, rows.Next())
}

func addRowsToTable(db *sql.DB, table string, rows ...map[string]string) error {
	headerStmt := "INSERT INTO " + table + " "
	fieldsStmt := ""
	valuesStmt := ""

	var fields []string
	for field := range rows[0] {
		fieldsStmt += "," + field
		fields = append(fields, field)
	}

	var args []interface{}
	for _, row := range rows {
		valuesStmt += ",("
		rowValuesStmt := ""
		for _, field := range fields {
			rowValuesStmt += ",?"
			args = append(args, row[field])
		}
		valuesStmt += rowValuesStmt[1:] + ")"
	}

	stmt := headerStmt + "(" + fieldsStmt[1:] + ") VALUES " + valuesStmt[1:] + ";"

	_, err := db.Exec(stmt, args...)
	return err
}

func createTestTableWithField(db *sql.DB, table string, primaryKeys []string, otherFields ...string) error {
	headerStmt := "CREATE TABLE " + table + " "
	fieldsStmt := ""
	for _, primaryKey := range primaryKeys {
		fieldsStmt += ", " + primaryKey + " VARCHAR(36) PRIMARY KEY"
	}
	for _, field := range otherFields {
		fieldsStmt += ", " + field + " VARCHAR(36)"
	}

	createStmt := headerStmt + "(" + fieldsStmt[2:] + ");"
	_, err := db.Exec(createStmt)
	return err
}

func dropTables(db *sql.DB, tables ...string) error {
	headerStmt := "DROP TABLE IF EXISTS "
	tablesStmt := ""
	for _, table := range tables {
		tablesStmt += ", " + table
	}
	dropStmt := headerStmt + tablesStmt[2:]
	_, err := db.Exec(dropStmt)
	return err
}
