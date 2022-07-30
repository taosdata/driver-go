# Go Connector for TDengine

[![Build Status](https://cloud.drone.io/api/badges/taosdata/driver-go/status.svg)](https://cloud.drone.io/taosdata/driver-go)

English | [简体中文](README-CN.md)

[TDengine] provides Go `database/sql` driver as [`taosSql`][driver-go].

## Remind

v2 is not compatible with v3 version and corresponds to the TDengine version as follows:

| **driver-go version** | **TDengine version** | 
|-----------------------|----------------------|
| v3.0.0                | 3.0.0.0+             |

## Install

Go 1.14+ is highly recommended for newly created projects.

```sh
go mod init taos-demo
```

import taosSql：

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosSql"
)
```

Use `go mod` for module management:

```sh
go mod tidy
```

Or `go get` to directly install it:

```sh
go get github.com/taosdata/driver-go/v3/taosSql
```

## Usage

### `database/sql` Standard

A simple use case：

```go
package main

import (
    "database/sql"
    "fmt"
    "time"

    _ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
    var taosUri = "root:taosdata@tcp(localhost:6030)/"
    taos, err := sql.Open("taosSql", taosUri)
    if err != nil {
        fmt.Println("failed to connect TDengine, err:", err)
        return
    }
    defer taos.Close()
    taos.Exec("create database if not exists test")
    taos.Exec("use test")
    taos.Exec("create table if not exists tb1 (ts timestamp, a int)")
    _, err = taos.Exec("insert into tb1 values(now, 0)(now+1s,1)(now+2s,2)(now+3s,3)")
    if err != nil {
        fmt.Println("failed to insert, err:", err)
        return
    }
    rows, err := taos.Query("select * from tb1")
    if err != nil {
        fmt.Println("failed to select from table, err:", err)
        return
    }

    defer rows.Close()
    for rows.Next() {
        var r struct {
            ts time.Time
            a  int
        }
        err := rows.Scan(&r.ts, &r.a)
        if err != nil {
            fmt.Println("scan error:\n", err)
            return
        }
        fmt.Println(r.ts, r.a)
    }
}
```

APIs that are worthy to have a check:

- `sql.Open(DRIVER_NAME string, dataSourceName string) *DB`

  This API will create a `database/sql` DB object, results with type `*DB`. `DRIVER_NAME` should be set as `taosSql`,
  and `dataSourceName` should be a URI like `user:password@tcp(host:port)/dbname`. For HA use case,
  use `user:password@cfg(/etc/taos)/dbname` to apply configs in `/etc/taos/taos.cfg`.

- `func (db *DB) Exec(query string, args ...interface{}) (Result, error)`

  Execute non resultset SQLs, like `create`, `alter` etc.

- `func (db *DB) Query(query string, args ...interface{}) (*Rows, error)`

  Execute a query with resultset.

- `func (db *DB) Close() error`

  Close an DB object and disconnect.

### Subscription

Create consumer:

````go
func NewConsumer(conf *Config) (*Consumer, error)
````

Subscribe:

````go
func (c *Consumer) Subscribe(topics []string) error
````

Poll message:

````go
func (c *Consumer) Poll(timeout time.Duration) (*Result, error)
````

Commit message:

````go
func (c *Consumer) Commit(ctx context.Context, message unsafe.Pointer) error
````

Free message:

````go
func (c *Consumer) FreeMessage(message unsafe.Pointer)
````

Unsubscribe:

````go
func (c *Consumer) Unsubscribe() error
````

Close consumer:

````go
func (c *Consumer) Close() error
````

Example code: [`examples/tmq/main.go`](examples/tmq/main.go).

### schemaless

InfluxDB format:

````go
func (conn *Connector) InfluxDBInsertLines(lines []string, precision string) error
````

Example code: [`examples/schemaless/influx/main.go`](examples/schemaless/influx/main.go).

OpenTSDB telnet format:

````go
func (conn *Connector) OpenTSDBInsertTelnetLines(lines []string) error
````

Example code: [`examples/schemaless/telnet/main.go`](examples/schemaless/telnet/main.go).

OpenTSDB json format:

````go
func (conn *Connector) OpenTSDBInsertJsonPayload(payload string) error
````

Example code: [`examples/schemaless/json/main.go`](examples/schemaless/json/main.go).

### stmt insert

Prepare sql:

````go
func (stmt *InsertStmt) Prepare(sql string) error
````

Set the child table name:

````go
func (stmt *InsertStmt) SetSubTableName(name string) error
````

Set the table name:

````go
func (stmt *InsertStmt) SetTableName(name string) error
````

Set the subtable name and tags:

````go
func (stmt *InsertStmt) SetTableNameWithTags(tableName string, tags *param.Param) error
````

Bind parameters:

````go
func (stmt *InsertStmt) BindParam(params []*param.Param, bindType *param.ColumnType) error
````

Add batch:

````go
func (stmt *InsertStmt) AddBatch() error
````

implement:

````go
func (stmt *InsertStmt) Execute() error
````

Get the number of affected rows:

````go
func (stmt *InsertStmt) GetAffectedRows() int
````

Close stmt:

````go
func (stmt *InsertStmt) Close() error
````

Example code: [`examples/stmtinsert/main.go`](examples/stmtinsert/main.go).

## restful implementation of the `database/sql` standard interface

A simple use case：

```go
package main

import (
    "database/sql"
    "fmt"
    "time"

    _ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
    var taosDSN = "root:taosdata@http(localhost:6041)/"
    taos, err := sql.Open("taosRestful", taosDSN)
    if err != nil {
        fmt.Println("failed to connect TDengine, err:", err)
        return
    }
    defer taos.Close()
    taos.Exec("create database if not exists test")
    taos.Exec("create table if not exists test.tb1 (ts timestamp, a int)")
    _, err = taos.Exec("insert into test.tb1 values(now, 0)(now+1s,1)(now+2s,2)(now+3s,3)")
    if err != nil {
        fmt.Println("failed to insert, err:", err)
        return
    }
    rows, err := taos.Query("select * from test.tb1")
    if err != nil {
        fmt.Println("failed to select from table, err:", err)
        return
    }

    defer rows.Close()
    for rows.Next() {
        var r struct {
            ts time.Time
            a  int
        }
        err := rows.Scan(&r.ts, &r.a)
        if err != nil {
            fmt.Println("scan error:\n", err)
            return
        }
        fmt.Println(r.ts, r.a)
    }
}
```

### Usage of taosRestful

import

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosRestful"
)
```

Introduce

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosRestful"
)
```

The driverName of `sql.Open` is `taosRestful`

The DSN format is:

```text
database username:database password@connection-method(domain or ip:port)/[database][? Parameter]
```

Example:

```text
root:taosdata@http(localhost:6041)/test?readBufferSize=52428800
```

Parameters:

- `disableCompression` Whether to accept compressed data, default is `true` Do not accept compressed data, set to `false` if the transferred data is compressed using gzip.
- `readBufferSize` The default size of the buffer for reading data is 4K (4096), which can be adjusted upwards when there is a lot of data in the query result.

### Usage restrictions

Since the restful interface is stateless, the `use db` syntax will not work, you need to put the db name into the sql statement, e.g. `create table if not exists tb1 (ts timestamp, a int)` to `create table if not exists test.tb1 (ts timestamp, a int)` otherwise it will report an error `[0x217] Database not specified or available`.

You can also put the db name in the DSN by changing `root:taosdata@http(localhost:6041)/` to `root:taosdata@http(localhost:6041)/test`. Executing the `create database` statement when the specified db does not exist will not report an error, while executing other queries or inserts will report an error. The example is as follows:

```go
package main

import (
    "database/sql"
    "fmt"
    "time"

    _ "github.com/taosdata/driver-go/v3/taosRestful"
)

func main() {
    var taosDSN = "root:taosdata@http(localhost:6041)/test"
    taos, err := sql.Open("taosRestful", taosDSN)
    if err != nil {
        fmt.Println("failed to connect TDengine, err:", err)
        return
    }
    defer taos.Close()
    taos.Exec("create database if not exists test")
    taos.Exec("create table if not exists tb1 (ts timestamp, a int)")
    _, err = taos.Exec("insert into tb1 values(now, 0)(now+1s,1)(now+2s,2)(now+3s,3)")
    if err != nil {
        fmt.Println("failed to insert, err:", err)
        return
    }
    rows, err := taos.Query("select * from tb1")
    if err != nil {
        fmt.Println("failed to select from table, err:", err)
        return
    }

    defer rows.Close()
    for rows.Next() {
        var r struct {
            ts time.Time
            a  int
        }
        err := rows.Scan(&r.ts, &r.a)
        if err != nil {
            fmt.Println("scan error:\n", err)
            return
        }
        fmt.Println(r.ts, r.a)
    }
}
```

## Directory structure

```text
driver-go
├── af //advanced function
├── common //common function and constants
├── errors // error type
├── examples //examples
├── taosRestful // database operation standard interface (restful)
├── taosSql // database operation standard interface
├── types // inner type
└── wrapper // cgo wrapper
```

## Link

driver-go: [https://github.com/taosdata/driver-go](https://github.com/taosdata/driver-go)

TDengine: [https://github.com/taosdata/TDengine](https://github.com/taosdata/TDengine)
