package stmt

import (
	"encoding/binary"

	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/common/serializer"
	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/client"
)

type Stmt struct {
	connector    *Connector
	id           uint64
	lastAffected int
}

func (s *Stmt) Prepare(sql string) error {
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
	envelope := s.connector.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		s.connector.client.PutEnvelope(envelope)
		return err
	}
	respBytes, err := s.connector.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp PrepareResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
}

func (s *Stmt) SetTableName(name string) error {
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
	envelope := s.connector.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		s.connector.client.PutEnvelope(envelope)
		return err
	}
	respBytes, err := s.connector.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp SetTableNameResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
}

func (s *Stmt) SetTags(tags *param.Param, bindType *param.ColumnType) error {
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
	envelope := s.connector.client.GetEnvelope()
	envelope.Msg.Grow(24 + len(block))
	envelope.Msg.Write(reqData)
	envelope.Msg.Write(block)
	respBytes, err := s.connector.sendBinary(reqID, envelope)
	if err != nil {
		return err
	}
	var resp SetTagsResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
}

func (s *Stmt) BindParam(params []*param.Param, bindType *param.ColumnType) error {
	block, err := serializer.SerializeRawBlock(params, bindType)
	if err != nil {
		return err
	}
	reqID := s.connector.generateReqID()
	reqData := make([]byte, 24)
	binary.LittleEndian.PutUint64(reqData, reqID)
	binary.LittleEndian.PutUint64(reqData[8:], s.id)
	binary.LittleEndian.PutUint64(reqData[16:], BindMessage)
	envelope := s.connector.client.GetEnvelope()
	envelope.Msg.Grow(24 + len(block))
	envelope.Msg.Write(reqData)
	envelope.Msg.Write(block)
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(reqData)
	if err != nil {
		s.connector.client.PutEnvelope(envelope)
		return err
	}
	respBytes, err := s.connector.sendBinary(reqID, envelope)
	if err != nil {
		return err
	}
	var resp BindResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
}

func (s *Stmt) AddBatch() error {
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
	envelope := s.connector.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		s.connector.client.PutEnvelope(envelope)
		return err
	}
	respBytes, err := s.connector.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp AddBatchResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	return nil
}

func (s *Stmt) Exec() error {
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
	envelope := s.connector.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		s.connector.client.PutEnvelope(envelope)
		return err
	}
	respBytes, err := s.connector.sendText(reqID, envelope)
	if err != nil {
		return err
	}
	var resp ExecResp
	err = client.JsonI.Unmarshal(respBytes, &resp)
	if err != nil {
		return err
	}
	if resp.Code != 0 {
		return taosErrors.NewError(resp.Code, resp.Message)
	}
	s.lastAffected = resp.Affected
	return nil
}

func (s *Stmt) GetAffectedRows() int {
	return s.lastAffected
}

func (s *Stmt) Close() error {
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
	envelope := s.connector.client.GetEnvelope()
	err = client.JsonI.NewEncoder(envelope.Msg).Encode(action)
	if err != nil {
		s.connector.client.PutEnvelope(envelope)
		return err
	}
	s.connector.sendTextWithoutResp(envelope)
	return nil
}
