package schemaless

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/taosdata/driver-go/v3/common"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/taosWS"
)

const (
	SchemalessConn  = "conn"
	SchemalessWrite = "insert"
)

type wsConnection struct {
	host         string
	port         int
	user         string
	password     string
	token        string
	db           string
	readTimeout  time.Duration
	writeTimeout time.Duration
	conn         *websocket.Conn
}

func NewWsConnection(ssl bool, user, password, token, host string, port int, db string, readTimeout, writeTimeout time.Duration) (Connection, error) {
	schema := "ws"
	if ssl {
		schema = "wss"
	}
	endpointUrl := &url.URL{
		Scheme: schema,
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   "/rest/schemaless",
	}
	if token != "" {
		endpointUrl.RawQuery = fmt.Sprintf("token=%s", token)
	}

	ws, _, err := common.DefaultDialer.Dial(endpointUrl.String(), nil)
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(common.BufferSize4M)
	_ = ws.SetReadDeadline(time.Now().Add(common.DefaultPongWait))
	ws.SetPongHandler(func(string) error {
		_ = ws.SetReadDeadline(time.Now().Add(common.DefaultPongWait))
		return nil
	})

	conn := wsConnection{
		host:         host,
		port:         port,
		user:         user,
		password:     password,
		token:        token,
		db:           db,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		conn:         ws,
	}
	ctx := context.Background()
	if err = conn.connect(ctx); err != nil {
		_ = conn.close(ctx)
		return nil, err
	}
	return &conn, nil
}

func (w *wsConnection) close(_ context.Context) error {
	if w.conn != nil {
		return w.conn.Close()
	}
	w.conn = nil
	return nil
}

func (w *wsConnection) connect(_ context.Context) error {
	req := &taosWS.WSConnectReq{
		ReqID:    uint64(common.GetReqID()),
		User:     w.user,
		Password: w.password,
		DB:       w.db,
	}
	args, err := json.Marshal(req)
	if err != nil {
		return err
	}
	action := taosWS.WSAction{
		Action: SchemalessConn,
		Args:   args,
	}

	if _, err = w.writeData(&action); err != nil {
		return err
	}
	return nil
}

func (w *wsConnection) insert(_ context.Context, lines string, protocol int, precision string, ttl int, reqID int64) error {
	if reqID == 0 {
		reqID = common.GetReqID()
	}
	req := schemalessWriteReq{
		ReqID:     uint64(reqID),
		DB:        w.db,
		Protocol:  protocol,
		Precision: precision,
		TTL:       ttl,
		Data:      lines,
	}

	args, err := json.Marshal(req)
	if err != nil {
		return err
	}
	action := taosWS.WSAction{
		Action: SchemalessWrite,
		Args:   args,
	}

	if _, err = w.writeData(&action); err != nil {
		return err
	}
	return nil
}

func (w *wsConnection) writeData(action *taosWS.WSAction) (resp *taosWS.WSConnectResp, err error) {
	j, err := json.Marshal(action)
	if err != nil {
		return nil, err
	}
	err = w.writeText(j)
	if err != nil {
		return nil, err
	}

	err = w.readTo(&resp)
	if err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, taosErrors.NewError(resp.Code, resp.Message)
	}
	return
}

func (w *wsConnection) writeText(data []byte) error {
	_ = w.conn.SetWriteDeadline(time.Now().Add(w.writeTimeout))
	err := w.conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		return taosWS.NewBadConnErrorWithCtx(err, string(data))
	}
	return nil
}

func (w *wsConnection) readTo(to interface{}) error {
	var outErr error
	done := make(chan struct{})
	go func() {
		defer close(done)

		mt, respBytes, err := w.conn.ReadMessage()
		if err != nil {
			outErr = taosWS.NewBadConnError(err)
			return
		}
		if mt != websocket.TextMessage {
			outErr = taosWS.NewBadConnErrorWithCtx(fmt.Errorf("readTo: got wrong message type %d", mt), formatBytes(respBytes))
			return
		}
		err = json.Unmarshal(respBytes, to)
		if err != nil {
			outErr = taosWS.NewBadConnErrorWithCtx(err, string(respBytes))
			return
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), w.readTimeout)
	defer cancel()
	select {
	case <-done:
		return outErr
	case <-ctx.Done():
		return taosWS.NewBadConnError(errors.New("read timeout"))
	}
}

func formatBytes(bs []byte) string {
	if len(bs) == 0 {
		return ""
	}
	buffer := &strings.Builder{}
	buffer.WriteByte('[')
	for i := 0; i < len(bs); i++ {
		_, _ = fmt.Fprintf(buffer, "0x%02x", bs[i])
		if i != len(bs)-1 {
			buffer.WriteByte(',')
		}
	}
	buffer.WriteByte(']')
	return buffer.String()
}

type schemalessWriteReq struct {
	ReqID     uint64 `json:"req_id"`
	DB        string `json:"db"`
	Protocol  int    `json:"protocol"`
	Precision string `json:"precision"`
	TTL       int    `json:"ttl"`
	Data      string `json:"data"`
}

var _ Connection = (*wsConnection)(nil)
