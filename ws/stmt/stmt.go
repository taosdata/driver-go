package stmt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/common/serializer"
	"github.com/taosdata/driver-go/v3/ws/client"
)

type Stmt struct {
	connector    *Connector
	id           uint64
	lastAffected int
	connClosed   uint32
}

var ErrConnIsClosed = fmt.Errorf("connection is closed")

func (s *Stmt) isConnClosed() bool {
	return atomic.LoadUint32(&s.connClosed) == 1
}

func (s *Stmt) setConnClosed() {
	atomic.StoreUint32(&s.connClosed, 1)
}

func (s *Stmt) Prepare(sql string) error {
	if s.isConnClosed() {
		return ErrConnIsClosed
	}
	reqID := s.connector.generateReqID()
	req := &PrepareReq{
		ReqID:  reqID,
		StmtID: s.id,
		SQL:    sql,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: STMTPrepare,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := s.connector.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp PrepareResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (s *Stmt) SetTableName(name string) error {
	if s.isConnClosed() {
		return ErrConnIsClosed
	}
	reqID := s.connector.generateReqID()
	req := &SetTableNameReq{
		ReqID:  reqID,
		StmtID: s.id,
		Name:   name,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: STMTSetTableName,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := s.connector.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp SetTableNameResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (s *Stmt) SetTags(tags *param.Param, bindType *param.ColumnType) error {
	if s.isConnClosed() {
		return ErrConnIsClosed
	}
	tagValues := tags.GetValues()
	reverseTags := make([]*param.Param, len(tagValues))
	for i := 0; i < len(tagValues); i++ {
		reverseTags[i] = param.NewParam(1).AddValue(tagValues[i])
	}
	block, err := serializer.SerializeRawBlock(reverseTags, bindType)
	if err != nil {
		return err
	}
	reqID := s.connector.generateReqID()
	reqData := make([]byte, 24)
	binary.LittleEndian.PutUint64(reqData, reqID)
	binary.LittleEndian.PutUint64(reqData[8:], s.id)
	binary.LittleEndian.PutUint64(reqData[16:], SetTagsMessage)
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.Grow(24 + len(block))
	envelope.Msg.Write(reqData)
	envelope.Msg.Write(block)
	respBytes, err := s.connector.sendBinary(reqID, envelope)
	if err != nil {
		return err
	}
	var resp SetTagsResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (s *Stmt) BindParam(params []*param.Param, bindType *param.ColumnType) error {
	if s.isConnClosed() {
		return ErrConnIsClosed
	}
	block, err := serializer.SerializeRawBlock(params, bindType)
	if err != nil {
		return err
	}
	reqID := s.connector.generateReqID()
	reqData := make([]byte, 24)
	binary.LittleEndian.PutUint64(reqData, reqID)
	binary.LittleEndian.PutUint64(reqData[8:], s.id)
	binary.LittleEndian.PutUint64(reqData[16:], BindMessage)
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	envelope.Msg.Grow(24 + len(block))
	envelope.Msg.Write(reqData)
	envelope.Msg.Write(block)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(reqData)
	if err != nil {
		return err
	}
	respBytes, err := s.connector.sendBinary(reqID, envelope)
	if err != nil {
		return err
	}
	var resp BindResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (s *Stmt) AddBatch() error {
	if s.isConnClosed() {
		return ErrConnIsClosed
	}
	reqID := s.connector.generateReqID()
	req := &AddBatchReq{
		ReqID:  reqID,
		StmtID: s.id,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: STMTAddBatch,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := s.connector.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp AddBatchResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	return client.HandleResponseError(err, resp.Code, resp.Message)
}

func (s *Stmt) Exec() error {
	if s.isConnClosed() {
		return ErrConnIsClosed
	}
	reqID := s.connector.generateReqID()
	req := &ExecReq{
		ReqID:  reqID,
		StmtID: s.id,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: STMTExec,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	respBytes, err := s.connector.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp ExecResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return err
	}
	s.lastAffected = resp.Affected
	return nil
}

func (s *Stmt) GetAffectedRows() int {
	return s.lastAffected
}

func (s *Stmt) UseResult() (*Rows, error) {
	if s.isConnClosed() {
		return nil, ErrConnIsClosed
	}
	reqID := s.connector.generateReqID()
	req := &UseResultReq{
		ReqID:  reqID,
		StmtID: s.id,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &client.WSAction{
		Action: STMTUseResult,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return nil, err
	}
	respBytes, err := s.connector.sendText(reqID, envelope)
	if err != nil {
		return nil, err
	}
	var resp UseResultResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	err = client.HandleResponseError(err, resp.Code, resp.Message)
	if err != nil {
		return nil, err
	}
	return &Rows{
		buf:              &bytes.Buffer{},
		conn:             s.connector,
		client:           s.connector.client,
		resultID:         resp.ResultID,
		fieldsCount:      resp.FieldsCount,
		fieldsNames:      resp.FieldsNames,
		fieldsTypes:      resp.FieldsTypes,
		fieldsLengths:    resp.FieldsLengths,
		precision:        resp.Precision,
		fieldsPrecisions: resp.FieldsPrecisions,
		fieldsScales:     resp.FieldsScales,
	}, nil
}

func (s *Stmt) Close() error {
	if s.isConnClosed() {
		return ErrConnIsClosed
	}
	s.setConnClosed()
	defer s.connector.removeStmt(s)
	reqID := s.connector.generateReqID()
	req := &CloseReq{
		ReqID:  reqID,
		StmtID: s.id,
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		return err
	}
	action := &client.WSAction{
		Action: STMTClose,
		Args:   args,
	}
	envelope := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(envelope)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		return err
	}
	s.connector.sendTextWithoutResp(envelope)
	return nil
}
