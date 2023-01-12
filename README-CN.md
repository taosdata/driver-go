# Go Connector for TDengine

[![Build Status](https://cloud.drone.io/api/badges/taosdata/driver-go/status.svg)](https://cloud.drone.io/taosdata/driver-go)

[English](README.md) | 简体中文

[TDengine] 提供了 GO 驱动程序 [`taosSql`][driver-go]，实现了 GO 语言的内置数据库操作接口 `database/sql/driver`。

## 提示

v2 与 v3 版本不兼容，与 TDengine 版本对应如下：

| **driver-go 版本** | **TDengine 版本** |
|------------------|-----------------|
| v3.0.0           | 3.0.0.0+        |
| v3.0.1           | 3.0.0.0+        |
| v3.0.3           | 3.0.1.5+        |
| v3.0.4           | 3.0.2.2+        |
| v3.1.0           | 3.0.2.2+        |

## 安装

对新建项目，建议使用 Go 1.14+，并使用 GO Modules 方式进行管理。

```sh
go mod init taos-demo
```

引入 taosSql：

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosSql"
)
```

使用 `go mod` 方式管理依赖包：

```sh
go mod tidy
```

或通过 `go get` 直接下载安装：

```sh
go get github.com/taosdata/driver-go/v3/taosSql
```

## 用法

### `database/sql` 标准接口

TDengine Go 连接器提供 database/sql 标准接口，使用方法简单示例如下：

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

常用 API 列表：

- `sql.Open(DRIVER_NAME string, dataSourceName string) *DB`

  该 API 用来创建`database/sql` DB 对象，类型为 `*DB` ，DRIVER_NAME 设置为字符串 `taosSql`,
  dataSourceName 设置为字符串`user:password@tcp(host:port)/dbname`，对应于 TDengine 的高可用机制，可以使用 `user:password@cfg(/etc/taos)/dbname`
  来使用`/etc/taos/taos.cfg`中的多 EP 配置。

  **注意**： 该 API 成功创建的时候，并没有做权限等检查，只有在真正执行 Query 或者 Exec 的时候才能真正的去创建连接，并同时检查 user/password/host/port 是不是合法。
  另外，由于整个驱动程序大部分实现都下沉到 taosSql 所依赖的 libtaos 中。所以，sql.Open 本身特别轻量。

- `func (db *DB) Exec(query string, args ...interface{}) (Result, error)`

  sql.Open 内置的方法，用来执行非查询相关 SQL，如`create`, `alter`等。

- `func (db *DB) Query(query string, args ...interface{}) (*Rows, error)`

  sql.Open 内置的方法，用来执行查询语句。

- `func (db *DB) Close() error`

  sql.Open 内置的方法，关闭 DB 对象。

### 订阅

创建消费：

```go
func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error)
```

订阅单个主题：

```go
func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error
```

订阅：

```go
func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error
```

轮询消息：

```go
func (c *Consumer) Poll(timeoutMs int) tmq.Event
```

提交消息：

```go
func (c *Consumer) Commit() ([]tmq.TopicPartition, error)
```

取消订阅：

```go
func (c *Consumer) Unsubscribe() error
```

关闭消费：

```go
func (c *Consumer) Close() error
```

示例代码：[`examples/tmq/main.go`](examples/tmq/main.go)。

### schemaless

InfluxDB 格式：

```go
func (conn *Connector) InfluxDBInsertLines(lines []string, precision string) error
```

示例代码：[`examples/schemaless/influx/main.go`](examples/schemaless/influx/main.go)。

OpenTSDB telnet 格式：

```go
func (conn *Connector) OpenTSDBInsertTelnetLines(lines []string) error
```

示例代码：[`examples/schemaless/telnet/main.go`](examples/schemaless/telnet/main.go)。

OpenTSDB json 格式：

```go
func (conn *Connector) OpenTSDBInsertJsonPayload(payload string) error
```

示例代码：[`examples/schemaless/json/main.go`](examples/schemaless/json/main.go)。

### stmt 插入

prepare sql：

```go
func (stmt *InsertStmt) Prepare(sql string) error
```

设置子表名：

```go
func (stmt *InsertStmt) SetSubTableName(name string) error
```

设置表名：

```go
func (stmt *InsertStmt) SetTableName(name string) error
```

设置子表名和标签：

```go
func (stmt *InsertStmt) SetTableNameWithTags(tableName string, tags *param.Param) error
```

绑定参数：

```go
func (stmt *InsertStmt) BindParam(params []*param.Param, bindType *param.ColumnType) error
```

添加批次：

```go
func (stmt *InsertStmt) AddBatch() error
```

执行：

```go
func (stmt *InsertStmt) Execute() error
```

获取影响行数：

```go
func (stmt *InsertStmt) GetAffectedRows() int
```

关闭 stmt：

```go
func (stmt *InsertStmt) Close() error
```

示例代码：[`examples/stmtinsert/main.go`](examples/stmtinsert/main.go)。

## restful 实现 `database/sql` 标准接口

通过 restful 方式实现 `database/sql` 接口，使用方法简单示例如下：

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

### 使用

引入

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosRestful"
)
```

`sql.Open` 的 driverName 为 `taosRestful`

DSN 格式为：

```text
数据库用户名:数据库密码@连接方式(域名或 ip:端口)/[数据库][?参数]
```

样例：

```root:taosdata@http(localhost:6041)/test?readBufferSize=52428800```

参数：

- `disableCompression` 是否接受压缩数据，默认为 `true` 不接受压缩数据，如果传输数据使用 gzip 压缩设置为 `false`。
- `readBufferSize` 读取数据的缓存区大小默认为 4K (4096)，当查询结果数据量多时可以适当调大该值。

### 使用限制

由于 restful 接口无状态所以 `use db` 语法不会生效，需要将 db 名称放到 sql 语句中，如：`create table if not exists tb1 (ts timestamp, a int)` 改为 `create table if not exists test.tb1 (ts timestamp, a int)` 否则将报错 `[0x217] Database not specified or available`

也可以将 db 名称放到 DSN 中，将 `root:taosdata@http(localhost:6041)/` 改为 `root:taosdata@http(localhost:6041)/test`。当指定的 db 不存在时执行 `create database` 语句不会报报错，而执行针对该 db 的其他查询或写入操作会报错。完整示例如下：

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

## websocket 实现 `database/sql` 标准接口

通过 websocket 方式实现 `database/sql` 接口，使用方法简单示例如下：

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

### 使用

引入

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosWS"
)
```

`sql.Open` 的 driverName 为 `taosWS`

DSN 格式为：

```text
数据库用户名:数据库密码@连接方式(域名或 ip:端口)/[数据库][?参数]
```

样例：

```root:taosdata@ws(localhost:6041)/test?writeTimeout=10s&readTimeout=10m```

参数：

- `writeTimeout` 通过 websocket 发送数据的超时时间。
- `readTimeout` 通过 websocket 接收响应数据的超时时间。

## 通过 websocket 使用 tmq

通过 websocket 方式使用 tmq。服务端需要启动 taoAdapter。

### 配置相关 API

- `func NewConfig(url string, chanLength uint) *Config`

 创建配置项，传入 websocket 地址和发送管道长度。

- `func (c *Config) SetConnectUser(user string) error`

 设置用户名。

- `func (c *Config) SetConnectPass(pass string) error`

 设置密码。

- `func (c *Config) SetClientID(clientID string) error`

 设置客户端标识。

- `func (c *Config) SetGroupID(groupID string) error`

 设置订阅组 ID。

- `func (c *Config) SetWriteWait(writeWait time.Duration) error`

 设置发送消息等待时间。

- `func (c *Config) SetMessageTimeout(timeout time.Duration) error`

 设置消息超时时间。

- `func (c *Config) SetErrorHandler(f func(consumer *Consumer, err error))`

 设置错误处理方法。

- `func (c *Config) SetCloseHandler(f func())`

 设置关闭处理方法。

### 订阅相关 API

- `func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error)`

 创建消费者。

- `func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error`

 订阅单个主题。

- `func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error`

 订阅主题。

- `func (c *Consumer) Poll(timeoutMs int) tmq.Event`

 轮询消息。

- `func (c *Consumer) Commit() ([]tmq.TopicPartition, error)`

 提交消息。

- `func (c *Consumer) Close() error`

 关闭连接。

示例代码：[`examples/tmqoverws/main.go`](examples/tmqoverws/main.go)。

## 通过 WebSocket 进行参数绑定

通过 websocket 方式使用 stmt。服务端需要启动 taoAdapter。

### 配置相关 API

- `func NewConfig(url string, chanLength uint) *Config`

  创建配置项，传入 websocket 地址和发送管道长度。

- `func (c *Config) SetCloseHandler(f func())`

  设置关闭处理方法。

- `func (c *Config) SetConnectDB(db string) error`

  设置连接 DB。

- `func (c *Config) SetConnectPass(pass string) error`

  设置连接密码。

- `func (c *Config) SetConnectUser(user string) error`

  设置连接用户名。

- `func (c *Config) SetErrorHandler(f func(connector *Connector, err error))`

  设置错误处理函数。

- `func (c *Config) SetMessageTimeout(timeout time.Duration) error`

  设置消息超时时间。

- `func (c *Config) SetWriteWait(writeWait time.Duration) error`

  设置发送消息等待时间。

### 参数绑定相关 API

* `func NewConnector(config *Config) (*Connector, error)`

  创建连接。

* `func (c *Connector) Init() (*Stmt, error)`

  初始化参数。

* `func (c *Connector) Close() error`

  关闭连接。

* `func (s *Stmt) Prepare(sql string) error`

  参数绑定预处理 SQL 语句。

* `func (s *Stmt) SetTableName(name string) error`

  参数绑定设置表名。

* `func (s *Stmt) SetTags(tags *param.Param, bindType *param.ColumnType) error`

  参数绑定设置标签。

* `func (s *Stmt) BindParam(params []*param.Param, bindType *param.ColumnType) error`

  参数绑定多行数据。

* `func (s *Stmt) AddBatch() error`

  添加到参数绑定批处理。

* `func (s *Stmt) Exec() error`

  执行参数绑定。

* `func (s *Stmt) GetAffectedRows() int`

  获取参数绑定插入受影响行数。

* `func (s *Stmt) Close() error`

  结束参数绑定。

完整参数绑定示例参见 [GitHub 示例文件](examples/stmtoverws/main.go)

## 目录结构

```text
driver-go
├── af //高级功能
├── common //通用方法以及常量
├── errors //错误类型
├── examples //样例
├── taosRestful // 数据库操作标准接口 (restful)
├── taosSql // 数据库操作标准接口
├── types // 内置类型
├── wrapper // cgo 包装器
└── ws // websocket
```

## 导航

driver-go: [https://github.com/taosdata/driver-go](https://github.com/taosdata/driver-go)

TDengine: [https://github.com/taosdata/TDengine](https://github.com/taosdata/TDengine)
