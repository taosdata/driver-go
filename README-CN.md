# 简介

| Github Action Tests                                                                  | CodeCov                                                                                                                           |
|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| ![actions](https://github.com/taosdata/driver-go/actions/workflows/go.yml/badge.svg) | [![codecov](https://codecov.io/gh/taosdata/driver-go/graph/badge.svg?token=70E8APPMKR)](https://codecov.io/gh/taosdata/driver-go) |

[English](README.md) | 简体中文

`driver-go` 是 TDengine 的官方 Go 语言连接器，实现了 Go 语言 `database/sql` 包的接口。Go 开发人员可以通过它开发存取
TDengine 集群数据的应用软件。

`driver-go` 提供了三种连接方式：

- 原生连接：通过客户端驱动程序 taosc 直接与服务端程序 taosd 建立连接。这种方式需要保证客户端的驱动程序 taosc 和服务端的
  taosd 版本保持一致。
- REST 连接：通过 taosAdapter 组件提供的 REST API 建立与 taosd 的连接。这种方式仅支持执行 SQL。
- Websocket 连接： 通过 taosAdapter 组件提供的 WebSocket API 建立与 taosd 的连接，不依赖 TDengine 客户端驱动。

## 支持的平台

- 原生连接支持的平台和 TDengine 客户端驱动支持的平台一致。
- WebSocket/REST 连接支持所有能运行 Go 的平台。

# 获取驱动

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

# 文档

- 开发示例请见[开发指南](https://docs.taosdata.com/develop/)
- 版本历史、TDengine 对应版本以及 API 说明请见[参考手册](https://docs.taosdata.com/reference/connector/go/)

# 贡献

鼓励每个人帮助改进这个项目，以下是这个项目的开发测试流程：

## 前置条件

1. Go 1.14 及以上
2. 使用原生连接时需要安装 TDengine 客户端，并允许 CGO `export CGO_ENABLED=1`

## 构建

编写示例程序后使用 `go build` 即可构建

## 测试

1. 执行测试前确保已经安装 TDengine 服务端，并且已经启动 taosd 与 taosAdapter，数据库干净无数据
2. 项目目录下执行 `go test ./...` 运行测试，测试会连接到本地的 TDengine 服务器与 taosAdapter 进行测试
3. 输出结果 `PASS` 为测试通过，`FAIL` 为测试失败，查看详细信息需要执行 `go test -v ./...`
4. 测试覆盖率可以通过执行 `go test -coverprofile=coverage.out ./...` 生成测试覆盖率文件，然后执行
   `go tool cover -html=coverage.out` 查看测试覆盖率

# 引用

- [TDengine 官网](https://www.taosdata.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

# 许可证

[MIT License](./LICENSE)
