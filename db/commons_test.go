package db

import (
	"os"
	"strings"
	"testing"
)

func getSourceTestDatabaseURL(t *testing.T) string {
	return getDatabaseURLFromEnv(t, "SOURCE_DB_TEST_URL")
}

func getTargetTestDatabaseURL(t *testing.T) string {
	return getDatabaseURLFromEnv(t, "TARGET_DB_TEST_URL")
}

func getDatabaseURLFromEnv(t *testing.T, envVar string) string {
	if testing.Short() {
		t.Skip("Database integration for log tests only")
	}

	dbURL := os.Getenv(envVar)
	if strings.TrimSpace(dbURL) == "" {
		t.Fatalf("Please set the environment variable %v to run database integration tests. Alternatively, run `go test -short` to skip them.", envVar)
	}

	return dbURL
}
