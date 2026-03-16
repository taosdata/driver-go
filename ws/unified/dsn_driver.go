package unified

const defaultDSNPath = "/ws"

// DSNDriver opens unified clients/connectors from DSN without requiring defaultPath on each call.
type DSNDriver struct {
	defaultPath string
	opts        []Option
}

// NewDSNDriver creates a DSNDriver.
// When defaultPath is empty, /ws is used.
func NewDSNDriver(defaultPath string, opts ...Option) *DSNDriver {
	if defaultPath == "" {
		defaultPath = defaultDSNPath
	}
	return &DSNDriver{
		defaultPath: defaultPath,
		opts:        append([]Option(nil), opts...),
	}
}

// Open parses DSN and returns a connected unified client.
func (d *DSNDriver) Open(dsn string) (*Client, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect()
}

// OpenConnector parses DSN and returns a connector.
func (d *DSNDriver) OpenConnector(dsn string) (*Connector, error) {
	defaultPath := defaultDSNPath
	var opts []Option
	if d != nil {
		if d.defaultPath != "" {
			defaultPath = d.defaultPath
		}
		opts = d.opts
	}
	return NewConnectorFromDSN(dsn, defaultPath, opts...)
}

// Open parses DSN with default /ws path and returns a connected unified client.
func Open(dsn string, opts ...Option) (*Client, error) {
	return NewDSNDriver(defaultDSNPath, opts...).Open(dsn)
}

// OpenConnector parses DSN with default /ws path and returns a connector.
func OpenConnector(dsn string, opts ...Option) (*Connector, error) {
	return NewDSNDriver(defaultDSNPath, opts...).OpenConnector(dsn)
}
