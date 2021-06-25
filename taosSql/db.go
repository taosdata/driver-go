/*
 * Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package taosSql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
	"unsafe"
)

type DB interface {
	Subscribe(restart bool, name string, sql string, interval time.Duration) (Topic, error)
	Exec(sql string) (driver.Result, error)
	Query(sql string) (driver.Rows, error)
	Close() error
}

type taosDB struct {
	ref unsafe.Pointer
}

func Open(host string, user string, pass string, dbname string, port int) (db DB, err error) {
	db = taosConnect(host, user, pass, dbname, port)
	if db == nil {
		err = fmt.Errorf("failed to connect to database %s", dbname)
		return
	}
	return
}

func (db *taosDB) Close() error {
	db.close()
	return nil
}

// Subscribe returns a Topic on success.
//
// Parameters:
// db: the database,
// restart:
//   0 - continue a subscription,
//   1 - start from the beginning,
// name: the topic name,
// sql: the sql statement,
// interval: Pulling interval
func (db *taosDB) Subscribe(restart bool, name string, sql string, interval time.Duration) (Topic, error) {
	topic := db.subscribe(restart, name, sql, interval)
	if topic == nil {
		return nil, errors.New("failed to subscribe")
	}
	return topic, nil
}

func (db *taosDB) Exec(sql string) (result driver.Result, err error) {
	res := db.query(sql)
	if res == nil {
		err = fmt.Errorf("failed to exec: %s", sql)
		return
	}
	defer res.freeResult()
	if errno := res.errno(); errno != 0 {
		err = errors.New(res.errstr())
		return
	}
	rowsAffected := res.affectedRows()

	if errno := res.errno(); errno != 0 {
		err = errors.New(res.errstr())
		return
	}
	result = driver.RowsAffected(rowsAffected)
	return
}

func (db *taosDB) Query(sql string) (rows driver.Rows, err error) {
	res := db.query(sql)
	if res == nil {
		err = errors.New("failed to query")
		return
	}
	errno := res.errno()
	if errno != 0 {
		err = errors.New(res.errstr())
		return
	}
	rows = res
	return
}

type Topic interface {
	Consume() (driver.Rows, error)
	Unsubscribe(keepProgress bool)
}

type taosTopic struct {
	ref unsafe.Pointer
	res *taosRes
}

func (sub *taosTopic) Consume() (rows driver.Rows, err error) {
	res := sub.consume()
	errno := res.errno()
	if errno != 0 {
		msg := res.errstr()
		err = errors.New(msg)
		return
	}
	sub.res = res
	rows = res
	return
}

func (sub *taosTopic) Unsubscribe(keepProgress bool) {
	if res := sub.res; res != nil {
		res.freeResult()
	}
	if keepProgress {
		sub.unsubscribe(1)
	} else {
		sub.unsubscribe(0)
	}
}
