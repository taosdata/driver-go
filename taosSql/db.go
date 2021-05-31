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
	Exec(sql string, params ...driver.Value) (driver.Result, error)
	Query(sql string, params ...driver.Value) (driver.Rows, error)
	Close() error
}

type taosDB struct {
	ref unsafe.Pointer
}

type taosStmt struct {
	ref unsafe.Pointer
}

type Topic interface {
	Consume() (driver.Rows, error)
	Unsubscribe(keepProgress bool)
}

type taosTopic struct {
	ref unsafe.Pointer
	res *taosRes
}

func getError() error {
	if errno := getErrno(); errno < 0 {
		return errors.New(tstrerror(errno))
	}
	return nil
}

func Open(dbname string) (db DB, err error) {
	db = taosConnect("", "", "", dbname, 0)
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
func (db *taosDB) Subscribe(restart bool, name string, sql string, interval time.Duration) (topic Topic, err error) {
	topic = db.subscribe(restart, name, sql, interval)
	if topic == nil {
		err = getError()
		if err != nil {
			return
		} else {
			err = errors.New("failed to subscribe")
			return
		}
	}
	return topic, nil
}

func (db *taosDB) execute(sql string, params []driver.Value) (res *taosRes, err error) {
	stmt := db.stmtInit()
	if stmt == nil {
		if err = getError(); err != nil {
			return
		} else {
			err = errors.New("failed to init stmt")
			return
		}
	}

	defer stmt.close()
	if rc := stmt.prepare(sql); rc < 0 {
		err = errors.New(tstrerror(rc))
		return
	}

	if rc := stmt.bindParam(params); rc < 0 {
		err = errors.New(tstrerror(rc))
		return
	}
	if isInsert := stmt.isInsert(); isInsert == 1 {
		if rc := stmt.addBatch(); rc < 0 {
			err = errors.New(tstrerror(rc))
			return
		}
	}
	if rc := stmt.execute(); rc < 0 {
		err = errors.New(tstrerror(rc))
		return
	}
	res = stmt.useResult()
	return
}

func (db *taosDB) Exec(sql string, params ...driver.Value) (result driver.Result, err error) {
	var res *taosRes
	if len(params) == 0 {
		res = db.query(sql)
	} else {
		if res, err = db.execute(sql, params); err != nil {
			return
		}
	}

	if res == nil {
		if err = getError(); err == nil {
			err = fmt.Errorf("failed to exec: %s", sql)
		}
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

func (db *taosDB) Query(sql string, params ...driver.Value) (rows driver.Rows, err error) {
	var res *taosRes
	if len(params) == 0 {
		res = db.query(sql)
	} else {
		if res, err = db.execute(sql, params); err != nil {
			return
		}

	}
	if res == nil {
		if err = getError(); err == nil {
			err = errors.New("failed to query")
		}
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
	if keepProgress {
		sub.unsubscribe(1)
	} else {
		sub.unsubscribe(0)
	}
	if res := sub.res; res != nil {
		res.freeResult()
	}
}
