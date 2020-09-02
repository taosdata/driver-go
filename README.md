# Go connector for TDengine
TDengine提供了GO驱动程序taosSql. taosSql实现了GO语言的内置接口database/sql/driver。用户只需按如下方式引入包就可以在应用程序中访问TDengin, 详见https://github.com/taosdata/driver-go/blob/develop/taosSql/driver_test.go
```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/taosSql"
)
```
## 常用API
```
sql.Open(DRIVER_NAME string, dataSourceName string) *DB
```
该API用来打开DB，返回一个类型为\*DB的对象，一般情况下，DRIVER_NAME设置为字符串taosSql, dataSourceName设置为字符串user:password@/tcp(host:port)/dbname，如果客户想要用多个goroutine并发访问TDengine, 那么需要在各个goroutine中分别创建一个sql.Open对象并用之访问TDengine
注意： 该API成功创建的时候，并没有做权限等检查，只有在真正执行Query或者Exec的时候才能真正的去创建连接，并同时检查user/password/host/port是不是合法。 另外，由于整个驱动程序大部分实现都下沉到taosSql所依赖的libtaos中。所以，sql.Open本身特别轻量。
```
func (db *DB) Exec(query string, args ...interface{}) (Result, error)
```
sql.Open内置的方法，用来执行非查询相关SQL
```
func (db *DB) Query(query string, args ...interface{}) (*Rows, error)
```
sql.Open内置的方法，用来执行查询语句

Please refer to the [demo app](https://github.com/taosdata/TDengine/tree/develop/tests/examples/go/src/taosapp) for details.
