# Go Connector for TDengine

[![Build Status](https://cloud.drone.io/api/badges/taosdata/driver-go/status.svg)](https://cloud.drone.io/taosdata/driver-go)

[English](README.md) | 简体中文

[TDengine]提供了GO驱动程序 [`taosSql`][driver-go]，实现了GO语言的内置数据库操作接口 `database/sql/driver`。

## 安装

对新建项目，建议使用Go 1.14+，并使用 GO Modules 方式进行管理。

```sh
go mod init taos-demo
```

引入taosSql：

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/taosSql"
)
```

使用`go mod`方式管理依赖包：

```sh
go mod tidy
```

或通过`go get`直接下载安装：

```sh
go get github.com/taosdata/driver-go/taosSql
```

因为目前STMT系列API在Windows下未全部开放，如果在Windows系统下使用taosSql，必须使用`win`分支才能编译通过：

```sh
go get github.com/taosdata/driver-go/taosSql@win
```

## 用法

### `database/sql` 标准接口

TDengine Go 连接器提供 database/sql 标准接口，使用方法简单示例如下：

```go
import (
	"fmt"
	"database/sql"
	_ "github.com/taosdata/driver-go/taosSql"
)
func main() {
	var taosuri = "root:taosdata/tcp(localhost:6030)/"
	taos, err := sql.Open("taosSql", taosuri)
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

  该API用来创建`database/sql` DB对象，类型为`*DB`，DRIVER_NAME设置为字符串`taosSql`, dataSourceName设置为字符串`user:password@/tcp(host:port)/dbname`，对应于TDengine的高可用机制，可以使用 `user:password@/cfg/dbname`来使用`/etc/taos/taos.cfg`中的多EP配置。

  **注意**： 该API成功创建的时候，并没有做权限等检查，只有在真正执行Query或者Exec的时候才能真正的去创建连接，并同时检查user/password/host/port是不是合法。 另外，由于整个驱动程序大部分实现都下沉到taosSql所依赖的libtaos中。所以，sql.Open本身特别轻量。

- `func (db *DB) Exec(query string, args ...interface{}) (Result, error)`

  sql.Open内置的方法，用来执行非查询相关SQL，如`create`, `alter`等。

- `func (db *DB) Query(query string, args ...interface{}) (*Rows, error)`

  sql.Open内置的方法，用来执行查询语句。

- `func (db *DB) Close() error`

  sql.Open内置的方法，关闭DB对象。

### 订阅接口

Open DB:

```go
Open(dbname string) (db DB, err error)
```

Subscribe:

```go
type DB interface {
	Subscribe(restart bool, name string, sql string, interval time.Duration) (Topic, error)
	Close() error
}
```

Topic:

```go
type Topic interface {
	Consume() (driver.Rows, error)
	Unsubscribe(keepProgress bool)
}
```

详情参见示例代码：[`examples/taoslogtail.go`](https://github.com/taosdata/driver-go/blob/master/examples/taoslogtail/taoslogtail.go)。

[driver-go]: https://github.com/taosdata/driver-go
[TDengine]: https://github.com/taosdata/TDengine