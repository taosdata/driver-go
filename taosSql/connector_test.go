package taosSql

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2022/1/27 16:16
// @description: test set config
func TestSetConfig(t *testing.T) {
	db, err := sql.Open("taosSql", "root:taosdata/tcp(localhost:6030)/?debugFlag=135&asyncLog=0")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = db.Exec("drop database if exists test_set_config")
	}()
	_, err = db.Exec("create database if not exists test_set_config")
	assert.NoError(t, err)
}
