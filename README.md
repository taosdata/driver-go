# Introduction

| Github Action Tests                                                                  | CodeCov                                                                                                                           |
|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| ![actions](https://github.com/taosdata/driver-go/actions/workflows/go.yml/badge.svg) | [![codecov](https://codecov.io/gh/taosdata/driver-go/graph/badge.svg?token=70E8APPMKR)](https://codecov.io/gh/taosdata/driver-go) |

English | [简体中文](README-CN.md)

`driver-go` is the official Go language connector for TDengine. It implements the Go language `database/sql` interface,
allowing Go developers to create applications that interact with TDengine clusters.

# Get the Driver

To import `taosSql`:

```go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/v3/taosSql"
)
```

To manage dependencies using `go mod`:

```sh
go mod tidy
```

Or, you can directly install the driver with `go get`:

```sh
go get github.com/taosdata/driver-go/v3/taosSql
```

# Documentation

- For development examples, see the [Developer Guide](https://docs.tdengine.com/developer-guide/).
- For version history, TDengine version compatibility, and API documentation, see
  the [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/go/).

# Contributing

We encourage everyone to help improve this project. Below is the development and testing process for this project:

## Prerequisites

1. Go 1.14 or above.
2. When using a native connection, you need to install the TDengine client and enable CGO with `export CGO_ENABLED=1`.

## Building

After writing an example program, use `go build` to build the program.

## Testing

1. Before running tests, ensure that the TDengine server is installed and that `taosd` and `taosAdapter` are running.
   The database should be empty.
2. In the project directory, run `go test ./...` to execute the tests. The tests will connect to the local TDengine
   server and taosAdapter for testing.
3. The output result `PASS` means the test passed, while `FAIL` means the test failed. For detailed information, run
   `go test -v ./...`.
4. To check test coverage, run `go test -coverprofile=coverage.out ./...` to generate a coverage file, and then use
   `go tool cover -html=coverage.out` to view the coverage report.

# References

- [TDengine Official Website](https://tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

# License

[MIT License](./LICENSE)
