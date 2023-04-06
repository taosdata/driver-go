package schemaless

import "context"

type Connection interface {
	close(ctx context.Context) error
	insert(ctx context.Context, lines string, protocol int, precision string, ttl int, reqID int64) error
}

type Schemaless struct {
	conn Connection
}

func NewSchemaless(conn Connection) *Schemaless {
	return &Schemaless{conn: conn}
}

func (s *Schemaless) Insert(ctx context.Context, lines string, protocol int, precision string, ttl int, reqID int64) error {
	return s.conn.insert(ctx, lines, protocol, precision, ttl, reqID)
}

func (s *Schemaless) Close(ctx context.Context) (err error) {
	if s.conn != nil {
		err = s.conn.close(ctx)
	}
	s.conn = nil
	return
}
