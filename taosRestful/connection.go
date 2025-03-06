package taosRestful

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
)

var jsonI = jsoniter.ConfigCompatibleWithStandardLibrary

type taosConn struct {
	cfg            *Config
	client         *http.Client
	url            *url.URL
	baseRawQuery   string
	header         map[string][]string
	readBufferSize int
}

func newTaosConn(cfg *Config) (*taosConn, error) {
	readBufferSize := cfg.ReadBufferSize
	if readBufferSize <= 0 {
		readBufferSize = 4 << 10
	}
	tc := &taosConn{cfg: cfg, readBufferSize: readBufferSize}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    cfg.DisableCompression,
	}
	if cfg.SkipVerify {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	tc.client = &http.Client{
		Transport: transport,
	}
	path := "/rest/sql"
	if len(cfg.DbName) != 0 {
		path = fmt.Sprintf("%s/%s", path, cfg.DbName)
	}
	tc.url = &url.URL{
		Scheme: cfg.Net,
		Host:   fmt.Sprintf("%s:%d", cfg.Addr, cfg.Port),
		Path:   path,
	}
	tc.header = map[string][]string{
		"Connection": {"keep-alive"},
	}
	if cfg.Token != "" {
		tc.baseRawQuery = fmt.Sprintf("token=%s", cfg.Token)
	} else {
		basic := base64.StdEncoding.EncodeToString([]byte(cfg.User + ":" + cfg.Passwd))
		tc.header["Authorization"] = []string{fmt.Sprintf("Basic %s", basic)}
	}
	if !cfg.DisableCompression {
		tc.header["Accept-Encoding"] = []string{"gzip"}
	}
	return tc, nil
}

func (tc *taosConn) Begin() (driver.Tx, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "restful does not support transaction"}
}

func (tc *taosConn) Close() (err error) {
	tc.client = nil
	tc.url = nil
	tc.cfg = nil
	tc.header = nil
	return nil
}

func (tc *taosConn) Prepare(query string) (driver.Stmt, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "restful does not support stmt"}
}

func (tc *taosConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (result driver.Result, err error) {
	return tc.execCtx(ctx, query, args)
}

func (tc *taosConn) execCtx(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 0 {
		if !tc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra round trips for preparing and closing a statement
		prepared, err := common.InterpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	result, err := tc.taosQuery(ctx, query, 512)
	if err != nil {
		return nil, err
	}
	if len(result.Data) != 1 || len(result.Data[0]) != 1 {
		return nil, errors.New("wrong result")
	}
	return driver.RowsAffected(result.Data[0][0].(int32)), nil
}

func (tc *taosConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	return tc.queryCtx(ctx, query, args)
}

func (tc *taosConn) queryCtx(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 0 {
		if !tc.cfg.InterpolateParams {
			return nil, driver.ErrSkip
		}
		// try client-side prepare to reduce round trip
		prepared, err := common.InterpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	result, err := tc.taosQuery(ctx, query, tc.readBufferSize)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("wrong result")
	}
	// Read Result
	rs := &rows{
		result: result,
	}
	return rs, err
}

func (tc *taosConn) Ping(ctx context.Context) (err error) {
	return nil
}

func (tc *taosConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, &taosErrors.TaosError{Code: 0xffff, ErrStr: "restful does not support transaction"}
}

func (tc *taosConn) taosQuery(ctx context.Context, sql string, bufferSize int) (*common.TDEngineRestfulResp, error) {
	reqIDValue, err := common.GetReqIDFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	if reqIDValue == 0 {
		reqIDValue = common.GetReqID()
	}
	if tc.baseRawQuery != "" {
		tc.url.RawQuery = fmt.Sprintf("%s&req_id=%d", tc.baseRawQuery, reqIDValue)
	} else {
		tc.url.RawQuery = fmt.Sprintf("req_id=%d", reqIDValue)
	}
	body := ioutil.NopCloser(strings.NewReader(sql))
	req := &http.Request{
		Method:     http.MethodPost,
		URL:        tc.url,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     tc.header,
		Body:       body,
		Host:       tc.url.Host,
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	resp, err := tc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("server response: %s - %s", resp.Status, string(body))
	}
	respBody := resp.Body
	defer func() {
		_, _ = ioutil.ReadAll(respBody)
	}()
	if !tc.cfg.DisableCompression && EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		respBody, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
	}
	data, err := marshalBody(respBody, bufferSize)
	if err != nil {
		return nil, err
	}
	if data.Code != 0 {
		return nil, taosErrors.NewError(data.Code, data.Desc)
	}
	return data, nil
}

func marshalBody(body io.Reader, bufferSize int) (*common.TDEngineRestfulResp, error) {
	var result common.TDEngineRestfulResp
	iter := jsonI.BorrowIterator(make([]byte, bufferSize))
	defer jsonI.ReturnIterator(iter)
	iter.Reset(body)
	timeFormat := time.RFC3339Nano
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, s string) bool {
		switch s {
		case "code":
			result.Code = iter.ReadInt()
		case "desc":
			result.Desc = iter.ReadString()
		case "column_meta":
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				index := 0
				isDecimal := false
				iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
					switch index {
					case 0:
						result.ColNames = append(result.ColNames, iter.ReadString())
						index = 1
					case 1:
						typeStr := iter.ReadString()
						if strings.HasPrefix(typeStr, "DECIMAL(") {
							// parse DECIMAL(10,2) to DECIMAL, 10, 2
							precision, scale, err := parseDecimalType(typeStr)
							if err != nil {
								iter.ReportError("parse decimal", err.Error())
								return false
							}
							isDecimal = true
							result.Precisions = append(result.Precisions, precision)
							result.Scales = append(result.Scales, scale)
						} else {
							t, exist := common.NameTypeMap[typeStr]
							if exist {
								result.ColTypes = append(result.ColTypes, t)
							} else {
								iter.ReportError("unsupported type in column_meta", typeStr)
							}
							result.Precisions = append(result.Precisions, 0)
							result.Scales = append(result.Scales, 0)
						}
						index = 2
					case 2:
						colLen := iter.ReadInt64()
						result.ColLength = append(result.ColLength, colLen)
						index = 0
						if isDecimal {
							switch colLen {
							case 8:
								result.ColTypes = append(result.ColTypes, common.TSDB_DATA_TYPE_DECIMAL64)
							case 16:
								result.ColTypes = append(result.ColTypes, common.TSDB_DATA_TYPE_DECIMAL)
							default:
								iter.ReportError("parse decimal", fmt.Sprintf("invalid length %d", colLen))
								return false
							}
						}
						isDecimal = false
					}
					return true
				})
				return true
			})
		case "data":
			columnCount := len(result.ColTypes)
			column := 0
			iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
				column = 0
				var row = make([]driver.Value, columnCount)
				iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
					defer func() {
						column += 1
					}()
					columnType := result.ColTypes[column]
					if columnType == common.TSDB_DATA_TYPE_JSON {
						row[column] = iter.SkipAndReturnBytes()
						return true
					}
					if iter.ReadNil() {
						row[column] = nil
						return true
					}
					var err error
					switch columnType {
					case common.TSDB_DATA_TYPE_NULL:
						iter.Skip()
						row[column] = nil
					case common.TSDB_DATA_TYPE_BOOL:
						row[column] = iter.ReadAny().ToBool()
					case common.TSDB_DATA_TYPE_TINYINT:
						row[column] = iter.ReadInt8()
					case common.TSDB_DATA_TYPE_SMALLINT:
						row[column] = iter.ReadInt16()
					case common.TSDB_DATA_TYPE_INT:
						row[column] = iter.ReadInt32()
					case common.TSDB_DATA_TYPE_BIGINT:
						row[column] = iter.ReadInt64()
					case common.TSDB_DATA_TYPE_FLOAT:
						row[column] = iter.ReadFloat32()
					case common.TSDB_DATA_TYPE_DOUBLE:
						row[column] = iter.ReadFloat64()
					case common.TSDB_DATA_TYPE_BINARY:
						row[column] = iter.ReadString()
					case common.TSDB_DATA_TYPE_TIMESTAMP:
						b := iter.ReadString()
						row[column], err = time.Parse(timeFormat, b)
						if err != nil {
							iter.ReportError("parse time", err.Error())
						}
					case common.TSDB_DATA_TYPE_NCHAR:
						row[column] = iter.ReadString()
					case common.TSDB_DATA_TYPE_UTINYINT:
						row[column] = iter.ReadUint8()
					case common.TSDB_DATA_TYPE_USMALLINT:
						row[column] = iter.ReadUint16()
					case common.TSDB_DATA_TYPE_UINT:
						row[column] = iter.ReadUint32()
					case common.TSDB_DATA_TYPE_UBIGINT:
						row[column] = iter.ReadUint64()
					case common.TSDB_DATA_TYPE_VARBINARY, common.TSDB_DATA_TYPE_GEOMETRY:
						data := iter.ReadStringAsSlice()
						if len(data)%2 != 0 {
							iter.ReportError("read varbinary", fmt.Sprintf("invalid length %s", string(data)))
						}
						value := make([]byte, len(data)/2)
						for i := 0; i < len(data); i += 2 {
							value[i/2] = hexCharToDigit(data[i])<<4 | hexCharToDigit(data[i+1])
						}
						row[column] = value
					case common.TSDB_DATA_TYPE_DECIMAL, common.TSDB_DATA_TYPE_DECIMAL64:
						row[column] = iter.ReadString()
					default:
						row[column] = nil
						iter.Skip()
					}
					return iter.Error == nil
				})
				if iter.Error != nil {
					return false
				}
				result.Data = append(result.Data, row)
				return true
			})
		case "rows":
			result.Rows = iter.ReadInt()
		default:
			iter.Skip()
		}
		return iter.Error == nil
	})
	if iter.Error != nil && iter.Error != io.EOF {
		return nil, iter.Error
	}
	return &result, nil
}

func parseDecimalType(typeStr string) (int64, int64, error) {
	// parse DECIMAL(10,2) to 10, 2
	if len(typeStr) < 12 || typeStr[len(typeStr)-1] != ')' {
		return 0, 0, fmt.Errorf("invalid decimal type %s", typeStr)
	}
	for i := len(typeStr) - 2; i > 8; i-- {
		if typeStr[i] == ',' {
			precision, err := strconv.ParseInt(typeStr[8:i], 10, 8)
			if err != nil {
				return 0, 0, err
			}
			scale, err := strconv.ParseInt(typeStr[i+1:len(typeStr)-1], 10, 8)
			if err != nil {
				return 0, 0, err
			}
			return precision, scale, nil
		}
	}
	return 0, 0, fmt.Errorf("invalid decimal type %s", typeStr)
}

// EqualFold is strings.EqualFold, ASCII only. It reports whether s and t
// are equal, ASCII-case-insensitively.
func EqualFold(s, t string) bool {
	if len(s) != len(t) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if lower(s[i]) != lower(t[i]) {
			return false
		}
	}
	return true
}

// lower returns the ASCII lowercase version of b.
func lower(b byte) byte {
	if 'A' <= b && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

func hexCharToDigit(char byte) uint8 {
	switch {
	case char >= '0' && char <= '9':
		return char - '0'
	case char >= 'a' && char <= 'f':
		return char - 'a' + 10
	default:
		panic("assertion failed: invalid hex char")
	}
}
