<!-- omit in toc -->
# TDengine Go Connector

<!-- omit in toc -->

| GitHub Action Tests                                                                  | CodeCov                                                                                                                           |
|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| ![actions](https://github.com/taosdata/driver-go/actions/workflows/go.yml/badge.svg) | [![codecov](https://codecov.io/gh/taosdata/driver-go/graph/badge.svg?token=70E8APPMKR)](https://codecov.io/gh/taosdata/driver-go) |

[English](README.md) | 简体中文

<!-- omit in toc -->
## 目录

<!-- omit in toc -->

- [1.简介](#1简介)
  - [1.1 连接方式](#11-连接方式)
  - [1.2 支持的平台](#12-支持的平台)
- [2 获取驱动](#2-获取驱动)
- [3. 文档](#3-文档)
- [4. 前置条件](#4-前置条件)
- [5. 构建](#5-构建)
- [6. 测试](#6-测试)
  - [6.1 运行测试](#61-运行测试)
  - [6.2 添加用例](#62-添加用例)
  - [6.3 性能测试](#63-性能测试)
- [7. 提交 Issue](#7-提交-issue)
- [8. 提交 PR](#8-提交-pr)
- [9. 引用](#9-引用)
- [10. 许可证](#10-许可证)

## 1.简介

`driver-go` 是 TDengine 的官方 Go 语言连接器，实现了 Go 语言 `database/sql` 包的接口。Go 开发人员可以通过它开发存取
TDengine 集群数据的应用软件。

### 1.1 连接方式

- 原生连接：通过客户端驱动程序 taosc 直接与服务端程序 taosd 建立连接。这种方式需要保证客户端的驱动程序 taosc 和服务端的
  taosd 版本保持一致。
- REST 连接：通过 taosAdapter 组件提供的 REST API 建立与 taosd 的连接。这种方式仅支持执行 SQL。
- Websocket 连接： 通过 taosAdapter 组件提供的 WebSocket API 建立与 taosd 的连接，不依赖 TDengine 客户端驱动。

我们推荐使用 WebSocket 连接方式。
详细说明请参考：[连接方式](https://docs.taosdata.com/develop/connect/#%E8%BF%9E%E6%8E%A5%E6%96%B9%E5%BC%8F) 。

### 1.2 支持的平台

- 原生连接支持的平台和 TDengine 客户端驱动支持的平台一致。
- WebSocket/REST 连接支持所有能运行 Go 的平台。

## 2. 获取驱动

项目引入驱动：

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

## 3. 文档

- 开发示例请见[开发指南](https://docs.taosdata.com/develop/)，包含了数据写入、查询、无模式写入、参数绑定和数据订阅等示例。
- 其他参考信息请见[参考手册](https://docs.taosdata.com/reference/connector/go/)，包含了版本历史、TDengine 对应版本以及 API
  说明和常见问题等。

## 4. 前置条件

- 已安装 Go 1.14 或以上版本,并允许 CGO `export CGO_ENABLED=1`。
- 本地已经部署 TDengine，具体步骤请参考[部署服务端](https://docs.taosdata.com/get-started/package/)，且已经启动 taosd 与
  taosAdapter。

## 5. 构建

编写示例程序后使用 `go build` 即可构建

## 6. 测试

### 6.1 运行测试

1. 执行测试前确保已经安装 TDengine 服务端，并且已经启动 taosd 与 taosAdapter，数据库干净无数据
2. 项目目录下执行 `go test ./...` 运行测试，测试会连接到本地的 TDengine 服务器与 taosAdapter 进行测试
3. 输出结果 `PASS` 为测试通过，`FAIL` 为测试失败，查看详细信息需要执行 `go test -v ./...`

### 6.2 添加用例

在 `*_test.go` 文件中添加测试用例，确保测试用例覆盖到新增的代码。

### 6.3 性能测试

性能测试还在开发中。

## 7. 提交 Issue

我们欢迎提交 [GitHub Issue](https://github.com/taosdata/driver-go/issues/new?template=Blank+issue)。 提交时请说明下面信息：

- 问题描述，是否必现
- 驱动版本
- 连接参数（不需要服务器地址、用户名和密码）
- TDengine 版本

## 8. 提交 PR

我们欢迎开发者一起开发本项目，提交 PR 时请参考下面步骤：

1. Fork 本项目，请参考 ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo))
2. 从 main 分支创建一个新分支，请使用有意义的分支名称 (`git checkout -b my_branch`)。
3. 修改代码，保证所有单元测试通过，并增加新的单元测试验证修改。
4. 提交修改到远端分支 (`git push origin my_branch`)。
5. 在 GitHub 上创建一个 Pull
   Request ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request))。
6. 提交 PR 后，如果 CI 通过，可以在 [codecov](https://app.codecov.io/gh/taosdata/driver-go/pulls) 页面找到自己 pr 查看覆盖率。

## 9. 引用

- [TDengine 官网](https://www.taosdata.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. 许可证

[MIT License](./LICENSE)
