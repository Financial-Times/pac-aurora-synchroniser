package db

import (
	"database/sql"

	"strings"

	log "github.com/sirupsen/logrus"
)

const lastModifiedField = "last_modified"

func Sync(sourceDB, targetDB *sql.DB) error {
	tables, err := getTablesWithLastModifiedFrom(sourceDB)
	if err != nil {
		log.WithError(err).Error("Error in getting list of tables with last_modified field from the source DB")
		return err
	}

	for _, table := range tables {
		err := syncTable(table, sourceDB, targetDB)
		if err != nil {
			log.WithError(err).WithField("table", table).Error("Error in synchronising table")
			return err
		}
	}
	return nil
}

func getTablesWithLastModifiedFrom(sourceDB *sql.DB) ([]string, error) {
	log.Info("Getting tables with last_modified field from source database")

	var tables []string

	rows, err := sourceDB.Query("SHOW TABLES")

	if err != nil {
		log.WithError(err).Error("Error in getting list of tables in the source DB")
		return []string{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			log.WithError(err).Error("Error in getting list of tables in the source DB")
			return []string{}, err
		}
		hasLastModifiedField, err := checkLastModifiedField(table, sourceDB)
		if err != nil {
			log.WithError(err).WithField("table", table).Error("Error in checking last_modified field in source DB")
			return []string{}, err
		}
		if hasLastModifiedField {
			log.Infof("%s has last_modified field", table)
			tables = append(tables, table)
		}
	}

	return tables, nil
}

func checkLastModifiedField(table string, sourceDB *sql.DB) (bool, error) {
	rows, err := sourceDB.Query("SHOW FIELDS FROM "+table+" WHERE Field = ?;", lastModifiedField)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}

	return false, nil
}

func syncTable(table string, sourceDB, targetDB *sql.DB) error {
	log.Infof("Synchronising %s ...", table)

	primaryKeyFields, err := getPrimaryKeyFields(table, sourceDB)
	if err != nil {
		log.WithError(err).WithField("table", table).Error("Error in getting primary Key Fields from table in the source DB")
		return err
	}

	sourceRows, err := sourceDB.Query("SELECT * FROM " + table + ";")
	if err != nil {
		log.WithError(err).WithField("table", table).Error("Error in getting rows in the source DB")
		return err
	}
	defer sourceRows.Close()

	columns, err := sourceRows.Columns()
	if err != nil {
		log.WithError(err).WithField("table", table).Error("Error in getting columns in the source DB")
		return err
	}
	sourceRowValues := make([]interface{}, len(columns))
	sourceRowValuesPtrs := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		sourceRowValuesPtrs[i] = &sourceRowValues[i]
	}

	for sourceRows.Next() {
		if err := sourceRows.Scan(sourceRowValuesPtrs...); err != nil {
			log.WithError(err).WithField("table", table).Error("Error in scanning row in the source DB")
			return err
		}

		sourceRow := make(map[string]interface{})

		for i := 0; i < len(columns); i++ {
			sourceRow[columns[i]] = sourceRowValues[i]
		}

		targetRow, err := getSameTargetRow(table, targetDB, sourceRow, primaryKeyFields)
		if err != nil {
			log.WithError(err).WithField("table", table).WithField("row", sourceRow).Error("Error in getting target DB row that matches source DB row")
			return err
		}

		if len(targetRow) == 0 || strings.Compare(string(sourceRow[lastModifiedField].([]uint8)), string(targetRow[lastModifiedField].([]uint8))) > 0 {
			primaryKeyValues := make(map[string]interface{})
			for _, primaryKeyField := range primaryKeyFields {
				primaryKeyValues[primaryKeyField] = string(sourceRow[primaryKeyField].([]uint8))
			}
			log.WithFields(primaryKeyValues).WithField("table", table).Info("Moving row to target DB...")

			err := copySourceRowToTargetRow(table, targetDB, sourceRow)
			if err != nil {
				log.WithError(err).WithFields(primaryKeyValues).WithField("table", table).Error("Error in moving row to target DB")
				return err
			}
		}
	}

	return nil
}

func getPrimaryKeyFields(table string, sourceDB *sql.DB) ([]string, error) {
	rows, err := sourceDB.Query("SHOW INDEX FROM " + table + " WHERE Key_name = 'PRIMARY' ;")
	if err != nil {
		return []string{}, err
	}
	defer rows.Close()

	var keyFields []string
	var keyField string

	columns, err := rows.Columns()
	if err != nil {
		return []string{}, err
	}

	values := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		if columns[i] == "Column_name" {
			values[i] = &keyField
		} else {
			values[i] = new(interface{})
		}
	}

	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return []string{}, err
		}
		keyFields = append(keyFields, keyField)
	}

	return keyFields, nil
}

func getSameTargetRow(table string, targetDB *sql.DB, sourceRow map[string]interface{}, primaryKeyFields []string) (map[string]interface{}, error) {
	query, args := buildSameTargetRowQuery(table, sourceRow, primaryKeyFields)
	targetRows, err := targetDB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer targetRows.Close()

	columns, err := targetRows.Columns()
	if err != nil {
		return nil, err
	}
	targetRowValues := make([]interface{}, len(columns))
	targetRowValuesPtrs := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		targetRowValuesPtrs[i] = &targetRowValues[i]
	}

	targetRow := make(map[string]interface{})

	if targetRows.Next() {
		if err := targetRows.Scan(targetRowValuesPtrs...); err != nil {
			return nil, err
		}

		for i := 0; i < len(columns); i++ {
			targetRow[columns[i]] = targetRowValues[i]
		}
	}
	return targetRow, nil
}

func buildSameTargetRowQuery(table string, sourceRow map[string]interface{}, primaryKeyFields []string) (string, []interface{}) {
	query := "SELECT * FROM " + table + " WHERE "
	args := make([]interface{}, len(primaryKeyFields))
	for i, primaryKeyField := range primaryKeyFields {
		query += primaryKeyField + "=? "
		args[i] = sourceRow[primaryKeyField]
	}
	return query, args
}

func copySourceRowToTargetRow(table string, targetDB *sql.DB, sourceRow map[string]interface{}) error {
	replaceStatement, args := buildReplaceStmt(table, sourceRow)
	_, err := targetDB.Exec(replaceStatement, args...)
	if err != nil {
		return err
	}
	return nil
}

func buildReplaceStmt(table string, sourceRow map[string]interface{}) (string, []interface{}) {
	headerStmt := "REPLACE INTO " + table + " "
	fieldsStmt := ""
	valuesStmt := ""
	var args []interface{}
	for field, value := range sourceRow {
		fieldsStmt += "," + field
		valuesStmt += ",?"
		args = append(args, value)
	}
	stmt := headerStmt + "(" + fieldsStmt[1:] + ") VALUES (" + valuesStmt[1:] + ")"
	return stmt, args
}
