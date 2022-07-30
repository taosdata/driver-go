# Go Connector for TDengine

[![Build Status](https://cloud.drone.io/api/badges/taosdata/driver-go/status.svg)](https://cloud.drone.io/taosdata/driver-go)

[English](README.md) | 简体中文

[TDengine]提供了GO驱动程序 [`taosSql`][driver-go]，实现了GO语言的内置数据库操作接口 `database/sql/driver`。

## 提示

`github.com/taosdata/driver-go/v2` 对 v1 版本进行重构,分离出内置数据库操作接口 `database/sql/driver` 到目录 `taosSql`；订阅、stmt等其他功能放到目录 `af`。

## 安装

对新建项目，建议使用Go 1.14+，并使用 GO Modules 方式进行管理。

```sh
go mod init taos-demo
```

引入taosSql：

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v2/taosSql"
)
```

使用`go mod`方式管理依赖包：

```sh
go mod tidy
```

或通过`go get`直接下载安装：

```sh
go get github.com/taosdata/driver-go/v2/taosSql
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

常用API列表：

- `sql.Open(DRIVER_NAME string, dataSourceName string) *DB`

  该API用来创建`database/sql` DB对象，类型为`*DB`，DRIVER_NAME设置为字符串`taosSql`,
  dataSourceName设置为字符串`user:password@/tcp(host:port)/dbname`，对应于TDengine的高可用机制，可以使用 `user:password@cfg(/etc/taos)/dbname`
  来使用`/etc/taos/taos.cfg`中的多EP配置。

  **注意**： 该API成功创建的时候，并没有做权限等检查，只有在真正执行Query或者Exec的时候才能真正的去创建连接，并同时检查user/password/host/port是不是合法。
  另外，由于整个驱动程序大部分实现都下沉到taosSql所依赖的libtaos中。所以，sql.Open本身特别轻量。

- `func (db *DB) Exec(query string, args ...interface{}) (Result, error)`

  sql.Open内置的方法，用来执行非查询相关SQL，如`create`, `alter`等。

- `func (db *DB) Query(query string, args ...interface{}) (*Rows, error)`

  sql.Open内置的方法，用来执行查询语句。

- `func (db *DB) Close() error`

  sql.Open内置的方法，关闭DB对象。

### 订阅接口

Open DB:

```go
func Open(host, user, pass, db string, port int) (*Connector, error)
```

Subscribe:

```go
func (conn *Connector) Subscribe(restart bool, topic string, sql string, interval time.Duration) (Subscriber, error)
```

Topic:

```go
type Subscriber interface {
    Consume() (driver.Rows, error)
    Unsubscribe(keepProgress bool)
}
```

详情参见示例代码：[`examples/taoslogtail.go`](examples/taoslogtail/taoslogtail.go)。

## restful 实现 `database/sql` 标准接口

通过 restful 方式实现 `database/sql` 接口，使用方法简单示例如下：

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

### 使用

引入

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v2/taosRestful"
)
```

`sql.Open` 的 driverName 为 `taosRestful`

DSN 格式为：

```数据库用户名:数据库密码@连接方式(域名或ip:端口)/[数据库][?参数]```

样例：

```root:taosdata@http(localhost:6041)/test?readBufferSize=52428800```

参数：

- `disableCompression` 是否接受压缩数据，默认为 `true` 不接受压缩数据，如果传输数据使用 gzip 压缩设置为 `false`。
- `readBufferSize` 读取数据的缓存区大小默认为 4K (4096)，当查询结果数据量多时可以适当调大该值。

### 使用限制

由于 restful 接口无状态所以 `use db` 语法不会生效，需要将 db 名称放到 sql 语句中，如：`create table if not exists tb1 (ts timestamp, a int)` 改为 `create table if not exists test.tb1 (ts timestamp, a int)` 否则将报错 `[0x217] Database not specified or available`

也可以将 db 名称放到 DSN 中，将 `root:taosdata@http(localhost:6041)/` 改为 `root:taosdata@http(localhost:6041)/test`，此方法在 TDengine 2.4.0.5 版本的 taosadapter 开始支持。当指定的 db 不存在时执行 `create database` 语句不会报报错，而执行针对该 db 的其他查询或写入操作会报错。完整示例如下：

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

## 目录结构

```text
driver-go
├── af //高级功能
├── common //通用方法以及常量
├── errors //错误类型
├── examples //样例
├── go.mod
├── go.sum
├── README-CN.md
├── README.md
├── taosRestful // 数据库操作标准接口(restful)
├── taosSql // 数据库操作标准接口
├── types // 内置类型
└── wrapper // cgo 包装器
```

## 导航

driver-go: [https://github.com/taosdata/driver-go](https://github.com/taosdata/driver-go)

TDengine: [https://github.com/taosdata/TDengine](https://github.com/taosdata/TDengine)
