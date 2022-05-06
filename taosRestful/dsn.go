package taosRestful

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/taosdata/driver-go/v2/errors"
)

var (
	errInvalidDSNUnescaped = &errors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: did you forget to escape a param value?"}
	errInvalidDSNAddr      = &errors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: network address not terminated (missing closing brace)"}
	errInvalidDSNPort      = &errors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: network port is not a valid number"}
	errInvalidDSNNoSlash   = &errors.TaosError{Code: 0xffff, ErrStr: "invalid DSN: missing the slash separating the database name"}
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type config struct {
	user               string // Username
	passwd             string // Password (requires User)
	net                string // Network type
	addr               string // Network address (requires Net)
	port               int
	dbName             string            // Database name
	params             map[string]string // Connection parameters
	interpolateParams  bool              // Interpolate placeholders into query string
	disableCompression bool
	readBufferSize     int
	token              string // cloud platform token
}

// NewConfig creates a new Config and sets default values.
func newConfig() *config {
	return &config{
		interpolateParams:  true,
		disableCompression: true,
		readBufferSize:     4 << 10,
	}
}

// ParseDSN parses the DSN string to a Config
func parseDSN(dsn string) (cfg *config, err error) {
	// New config with some default values
	cfg = newConfig()

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.passwd = dsn[k+1 : j]
								break
							}
						}
						cfg.user = dsn[:k]

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, errInvalidDSNUnescaped
							}
							//return nil, errInvalidDSNAddr
						}
						strList := strings.Split(dsn[k+1:i-1], ":")
						if len(strList) == 1 {
							return nil, errInvalidDSNAddr
						}
						if len(strList[0]) != 0 {
							cfg.addr = strList[0]
							cfg.port, err = strconv.Atoi(strList[1])
							if err != nil {
								return nil, errInvalidDSNPort
							}
						}
						break
					}
				}
				cfg.net = dsn[j+1 : k]
			}

			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}
			cfg.dbName = dsn[i+1 : j]

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}

	return
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *config, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}

		// cfg params
		switch value := param[1]; param[0] {
		// Enable client side placeholder substitution
		case "interpolateParams":
			cfg.interpolateParams, err = strconv.ParseBool(value)
			if err != nil {
				return &errors.TaosError{Code: 0xffff, ErrStr: "invalid bool value: " + value}
			}
		case "disableCompression":
			cfg.disableCompression, err = strconv.ParseBool(value)
			if err != nil {
				return &errors.TaosError{Code: 0xffff, ErrStr: "invalid bool value: " + value}
			}
		case "readBufferSize":
			cfg.readBufferSize, err = strconv.Atoi(value)
			if err != nil {
				return &errors.TaosError{Code: 0xffff, ErrStr: "invalid int value: " + value}
			}
		case "token":
			cfg.token = value
		default:
			// lazy init
			if cfg.params == nil {
				cfg.params = make(map[string]string)
			}

			if cfg.params[param[0]], err = url.QueryUnescape(value); err != nil {
				return
			}
		}
	}

	return
}
