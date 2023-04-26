package schemaless

import (
	"context"
	"time"
)

type Schemaless interface {
	Insert(ctx context.Context, lines string, protocol int, precision string, ttl int, reqID int64) error
	Close(ctx context.Context) error
}

func NewNativeSchemaless(user, password, host string, port int, db string) (Schemaless, error) {
	conn, err := newNativeConnection(user, password, host, port, db)
	if err != nil {
		return nil, err
	}
	return newSchemaless(conn), nil
}

func NewWsSchemaless(ssl bool, user, password, token, host string, port int, db string, readTimeout, writeTimeout time.Duration) (Schemaless, error) {
	conn, err := newWsConnection(ssl, user, password, token, host, port, db, readTimeout, writeTimeout)
	if err != nil {
		return nil, err
	}
	return newSchemaless(conn), nil
}

type connection interface {
	close(ctx context.Context) error
	insert(ctx context.Context, lines string, protocol int, precision string, ttl int, reqID int64) error
}

type schemaless struct {
	conn connection
}

func newSchemaless(conn connection) *schemaless {
	return &schemaless{conn: conn}
}

func (s *schemaless) Insert(ctx context.Context, lines string, protocol int, precision string, ttl int, reqID int64) error {
	return s.conn.insert(ctx, lines, protocol, precision, ttl, reqID)
}

func (s *schemaless) Close(ctx context.Context) (err error) {
	if s.conn != nil {
		err = s.conn.close(ctx)
	}
	s.conn = nil
	return
}
