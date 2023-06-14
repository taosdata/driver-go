# Go Connector for TDengine

[![Build Status](https://cloud.drone.io/api/badges/taosdata/driver-go/status.svg)](https://cloud.drone.io/taosdata/driver-go)

English | [简体中文](README-CN.md)

[TDengine] provides Go `database/sql` driver as [`taosSql`][driver-go].

## Remind

v2 is not compatible with v3 version and corresponds to the TDengine version as follows:

| **driver-go version** | **TDengine version** | **major features**                     |
|-----------------------|----------------------|----------------------------------------|
| v3.5.0                | 3.0.5.0+             | tmq: get assignment and seek offset    |
| v3.3.1                | 3.0.4.1+             | schemaless insert over websocket       |
| v3.1.0                | 3.0.2.2+             | provide tmq apis close to kafka        |
| v3.0.4                | 3.0.2.2+             | add request id                         |
| v3.0.3                | 3.0.1.5+             | statement insert over websocket        |
| v3.0.2                | 3.0.1.5+             | bulk pulling over websocket            |
| v3.0.1                | 3.0.0.0+             | tmq over websocket                     |
| v3.0.0                | 3.0.0.0+             | adapt to TDengine 3.0 query and insert |

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
func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error)
````

Subscribe single topic:

````go
func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error
````

Subscribe topics:

````go
func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error
````

Poll message:

````go
func (c *Consumer) Poll(timeoutMs int) tmq.Event
````

Commit message:

````go
func (c *Consumer) Commit() ([]tmq.TopicPartition, error)
````

Get assignment:

```go
func (c *Consumer) Assignment() (partitions []tmq.TopicPartition, err error)
```

Seek offset:

```go
func (c *Consumer) Seek(partition tmq.TopicPartition, ignoredTimeoutMs int) error
```

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

## websocket implementation of the `database/sql` standard interface

A simple use case：

```go
package main

import (
    "database/sql"
    "fmt"
    "time"

    _ "github.com/taosdata/driver-go/v3/taosWS"
)

func main() {
    var taosDSN = "root:taosdata@ws(localhost:6041)/"
    taos, err := sql.Open("taosWS", taosDSN)
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

### Usage of websocket

import

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosWS"
)
```

The driverName of `sql.Open` is `taosWS`

The DSN format is:

```text
database username:database password@connection-method(domain or ip:port)/[database][? parameter]
```

Example:

```text
root:taosdata@ws(localhost:6041)/test?writeTimeout=10s&readTimeout=10m
```

Parameters:

- `writeTimeout` The timeout to send data via websocket.
- `readTimeout` The timeout to receive response data via websocket.

## Using tmq over websocket

Use tmq over websocket. The server needs to start taoAdapter.

### Configure related API

- `func NewConfig(url string, chanLength uint) *Config`

 Create a configuration, pass in the websocket address and the length of the sending channel.

- `func (c *Config) SetConnectUser(user string) error`

 Set username.

- `func (c *Config) SetConnectPass(pass string) error`

 Set password.

- `func (c *Config) SetClientID(clientID string) error`

 Set the client ID.

- `func (c *Config) SetGroupID(groupID string) error`

 Set the subscription group ID.

- `func (c *Config) SetWriteWait(writeWait time.Duration) error`

 Set the waiting time for sending messages.

- `func (c *Config) SetMessageTimeout(timeout time.Duration) error`

 Set the message timeout.

- `func (c *Config) SetErrorHandler(f func(consumer *Consumer, err error))`

 Set the error handler.

- `func (c *Config) SetCloseHandler(f func())`

 Set the close handler.

### Subscription related API

- `func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error)`

 Create a consumer.

- `func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error`

 Subscribe a topic.

- `func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error`

 Subscribe to topics.

- `func (c *Consumer) Poll(timeoutMs int) tmq.Event`

 Poll messages.

- `func (c *Consumer) Commit() ([]tmq.TopicPartition, error)`

 Commit message.

- `func (c *Consumer) Assignment() (partitions []tmq.TopicPartition, err error)`

 Get assignment.

- `func (c *Consumer) Seek(partition tmq.TopicPartition, ignoredTimeoutMs int) error`

 Seek offset.

- `func (c *Consumer) Close() error`

 Close the connection.

Example code: [`examples/tmqoverws/main.go`](examples/tmqoverws/main.go).

## Parameter binding via WebSocket

Use stmt via websocket. The server needs to start taoAdapter.

### Configure related API

- `func NewConfig(url string, chanLength uint) *Config`

  Create a configuration item, pass in the websocket address and the length of the sending pipe.

- `func (c *Config) SetCloseHandler(f func())`

  Set close handler.

- `func (c *Config) SetConnectDB(db string) error`

  Set connect DB.

- `func (c *Config) SetConnectPass(pass string) error`

  Set password.

- `func (c *Config) SetConnectUser(user string) error`

  Set username.

- `func (c *Config) SetErrorHandler(f func(connector *Connector, err error))`

  Set error handler.

- `func (c *Config) SetMessageTimeout(timeout time.Duration) error`

  Set the message timeout.

- `func (c *Config) SetWriteWait(writeWait time.Duration) error`

  Set the waiting time for sending messages.

### Parameter binding related API

* `func NewConnector(config *Config) (*Connector, error)`

  Create a connection.

* `func (c *Connector) Init() (*Stmt, error)`

  Initialize the parameters.

* `func (c *Connector) Close() error`

  Close the connection.

* `func (s *Stmt) Prepare(sql string) error`

  Parameter binding preprocessing SQL statement.

* `func (s *Stmt) SetTableName(name string) error`

  Bind the table name parameter.

* `func (s *Stmt) SetTags(tags *param.Param, bindType *param.ColumnType) error`

  Bind tags.

* `func (s *Stmt) BindParam(params []*param.Param, bindType *param.ColumnType) error`

  Parameter bind multiple rows of data.

* `func (s *Stmt) AddBatch() error`

  Add to a parameter-bound batch.

* `func (s *Stmt) Exec() error`

  Execute a parameter binding.

* `func (s *Stmt) GetAffectedRows() int`

  Gets the number of affected rows inserted by the parameter binding.

* `func (s *Stmt) Close() error`

  Closes the parameter binding.

For a complete example of parameter binding, see [GitHub example file](examples/stmtoverws/main.go)

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
├── wrapper // cgo wrapper
└── ws // websocket
```

## Link

driver-go: [https://github.com/taosdata/driver-go](https://github.com/taosdata/driver-go)

TDengine: [https://github.com/taosdata/TDengine](https://github.com/taosdata/TDengine)
