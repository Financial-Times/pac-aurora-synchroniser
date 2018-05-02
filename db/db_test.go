package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewHappyDB(t *testing.T) {
	dbURL := getSourceTestDatabaseURL(t)
	_, err := New(dbURL, 10)
	assert.NoError(t, err)
}

func TestNewDBErr(t *testing.T) {
	_, err := New("an-invalid-URL", 10)
	assert.Error(t, err)
}
