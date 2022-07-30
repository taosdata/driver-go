# Go Connector for TDengine

[![Build Status](https://cloud.drone.io/api/badges/taosdata/driver-go/status.svg)](https://cloud.drone.io/taosdata/driver-go)

English | [简体中文](README-CN.md)

[TDengine] provides Go `database/sql` driver as [`taosSql`][driver-go].

## Remind

`github.com/taosdata/driver-go/v2`  is completely refactoring based on the v1 version. It also separates the built-in database operation interface `database/sql/driver` into the directory `taosSql` and put other advanced functions such as subscription and stmt in the directory `af`.

## Install

Go 1.14+ is highly recommended for newly created projects.

```sh
go mod init taos-demo
```

import taosSql：

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v2/taosSql"
)
```

Use `go mod` for module management:

```sh
go mod tidy
```

Or `go get` to directly install it:

```sh
go get github.com/taosdata/driver-go/v2/taosSql
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

    _ "github.com/taosdata/driver-go/v2/taosSql"
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
  and `dataSourceName` should be a URI like `user:password@/tcp(host:port)/dbname`. For HA use case,
  use `user:password@cfg(/etc/taos)/dbname` to apply configs in `/etc/taos/taos.cfg`.

- `func (db *DB) Exec(query string, args ...interface{}) (Result, error)`

  Execute non resultset SQLs, like `create`, `alter` etc.

- `func (db *DB) Query(query string, args ...interface{}) (*Rows, error)`

  Execute a query with resultset.

- `func (db *DB) Close() error`

  Close an DB object and disconnect.

### Subscription

Open DB:

```go
func Open(host, user, pass, db string, port int) (*Connector, error)
```

Subscribe:

```go
func (conn *Connector) Subscribe(restart bool, topic string, sql string, interval time.Duration) (Subscriber, error)
```

Subscriber:

```go
type Subscriber interface {
    Consume() (driver.Rows, error)
    Unsubscribe(keepProgress bool)
}
```

Check sample code for subscription
at [`examples/taoslogtail.go`](https://github.com/taosdata/driver-go/blob/master/examples/taoslogtail/taoslogtail.go).

## restful implementation of the `database/sql` standard interface

A simple use case：

```go
package main

import (
    "database/sql"
    "fmt"
    "time"

    _ "github.com/taosdata/driver-go/v2/taosRestful"
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
    _ "github.com/taosdata/driver-go/v2/taosRestful"
)
```

Introduce

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v2/taosRestful"
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

You can also put the db name in the DSN by changing `root:taosdata@http(localhost:6041)/` to `root:taosdata@http(localhost:6041)/test`. This method is supported by taosadapter since TDengine 2.4.0.5. Executing the `create database` statement when the specified db does not exist will not report an error, while executing other queries or inserts will report an error. The example is as follows:

```go
package main

import (
    "database/sql"
    "fmt"
    "time"

    _ "github.com/taosdata/driver-go/v2/taosRestful"
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
├── go.mod
├── go.sum
├── README-CN.md
├── README.md
├── taosRestful // database operation standard interface (restful)
├── taosSql // database operation standard interface
├── types // inner type
└── wrapper // cgo wrapper
```

## Link

driver-go: [https://github.com/taosdata/driver-go](https://github.com/taosdata/driver-go)

TDengine: [https://github.com/taosdata/TDengine](https://github.com/taosdata/TDengine)
