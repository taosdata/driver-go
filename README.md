# TDengine Go Connector

| GitHub Action Tests                                                                  | CodeCov                                                                                                                           |
|--------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| ![actions](https://github.com/taosdata/driver-go/actions/workflows/go.yml/badge.svg) | [![codecov](https://codecov.io/gh/taosdata/driver-go/graph/badge.svg?token=70E8APPMKR)](https://codecov.io/gh/taosdata/driver-go) |

English | [简体中文](README-CN.md)

## Introduction

`driver-go` is the official Go language connector for TDengine. It implements the Go language `database/sql` interface,
allowing Go developers to create applications that interact with TDengine clusters.

### Connection Methods

- Native Connection: Establishes a connection directly with the server program taosd through the client driver taosc.
  This method requires the client driver taosc and the server taosd to be of the same version.
- REST Connection: Establishes a connection with taosd through the REST API provided by the taosAdapter component. This
  method only supports executing SQL.
- WebSocket Connection: Establishes a connection with taosd through the WebSocket API provided by the taosAdapter
  component, without relying on the TDengine client driver.

### Supported Platforms

- The platforms supported by the native connection are consistent with those supported by the TDengine client driver.
- WebSocket/REST connections support all platforms that can run Go.

## Get the Driver

### Pre-installation

1. Go 1.14 or above installed.
2. When using a native connection, you need to install the TDengine client. For detailed steps, please refer
   to [Install Client Driver](https://docs.tdengine.com/tdengine-reference/client-libraries/#install-client-driver), and
   enable CGO with `export CGO_ENABLED=1`

### Install the Driver

Import the Driver into the Project

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

## Documentation

- For development examples, see the [Developer Guide](https://docs.tdengine.com/developer-guide/).
- For version history, TDengine version compatibility, and API documentation, see
  the [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/go/).

## Prerequisites

1. Go 1.14 or above.
2. TDengine has been deployed locally. For detailed steps, please refer
   to [Deploy Server](https://docs.tdengine.com/get-started/deploy-from-package/), and taosd and taosAdapter have been
   started.

## Build

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

## Submitting Issues

We welcome the submission of [GitHub Issue](https://github.com/taosdata/driver-go/issues/new?template=Blank+issue). When
submitting, please provide the following information:

- Description of the issue and whether it is consistently reproducible
- Driver version
- Connection parameters (excluding server address, username, and password)
- TDengine version

## Submitting PRs

We welcome developers to contribute to this project. Please follow the steps below to submit a PR:

1. Fork this project. Please refer
   to [how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo).
2. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`).
3. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
4. Push the changes to the remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub. Please refer
   to [how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).
6. After submitting the PR, if the CI passes, you can find your PR on
   the [codecov](https://app.codecov.io/gh/taosdata/driver-go/pulls) page to check the coverage.

## References

- [TDengine Official Website](https://tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## License

[MIT License](./LICENSE)
