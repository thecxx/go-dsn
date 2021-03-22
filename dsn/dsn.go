package dsn

import (
	"errors"
	"net/url"
	"strings"
)

var (
	errorInvalidScheme       = errors.New("missing protocol scheme")
	errorInvalidDSNAddr      = errors.New("network address not terminated (missing closing brace)")
	errorInvalidDSNNoSlash   = errors.New("missing the slash separating the database name")
	errorInvalidDSNUnescaped = errors.New("did you forget to escape a param value")
)

type DSN struct {
	Scheme   string
	Username string
	Password string

	Protocol string
	Addr     string

	Path string

	Params map[string]string
}

// [scheme://][username[:password]@][net[(addr)]]/a/b/c[?param1=value1&paramN=valueN]
func Parse(dsn string) (*DSN, error) {
	var (
		err    error
		scheme string
		source string
	)
	if scheme, source, err = parseScheme(dsn); err != nil {
		return nil, err
	}
	d := &DSN{
		Scheme: scheme,
	}
	if err := parse(d, source); err != nil {
		return nil, err
	}
	return d, nil
}

// parseScheme parses the raw dsn.
// (scheme must be [a-zA-Z][a-zA-Z0-9+-.]*)
func parseScheme(dsn string) (string, string, error) {
	for i := 0; i < len(dsn); i++ {
		c := dsn[i]
		switch {
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z':
		// do nothing
		case '0' <= c && c <= '9' || c == '+' || c == '-' || c == '.':
			if i == 0 {
				return "", dsn, nil
			}
		case c == ':':
			if i == 0 {
				return "", "", errorInvalidScheme
			}
			if dsn[i+1] == '/' && dsn[i+2] == '/' {
				return dsn[:i], dsn[i+3:], nil
			} else {
				return "", dsn, nil
			}
		default:
			// we have encountered an invalid character,
			// so there is no valid scheme
			return "", dsn, nil
		}
	}
	return "", dsn, nil
}

// See `github.com/go-sql-driver/mysql.ParseDSN`
// [username[:password]@][net[(addr)]]/a/b/c[?param1=value1&paramN=valueN]
func parse(dsn *DSN, source string) error {
	foundSlash := false
	for i := 0; i < len(source); i++ {
		if source[i] == '/' {
			foundSlash = true
			j, k := 0, 0
			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in source[:i]
				for j = i; j >= 0; j-- {
					if source[j] == '@' {
						// username[:password]
						// Find the first ':' in source[:j]
						for k = 0; k < j; k++ {
							if source[k] == ':' {
								dsn.Password = source[k+1 : j]
								break
							}
						}
						dsn.Username = source[:k]
						break
					}
				}
				// [protocol[(address)]]
				// Find the first '(' in source[j+1:i]
				for k = j + 1; k < i; k++ {
					if source[k] == '(' {
						// source[i-1] must be == ')' if an address is specified
						if source[i-1] != ')' {
							if strings.ContainsRune(source[k+1:i], ')') {
								return errorInvalidDSNUnescaped
							}
							return errorInvalidDSNAddr
						}
						dsn.Addr = source[k+1 : i-1]
						break
					}
				}

				// Protocol
				dsn.Protocol = source[j+1 : k]
			}

			// /a/b/c[?param1=value1&...&paramN=valueN]
			// Find the first '?' in source[i+1:]
			for j = i + 1; j < len(source); j++ {
				if source[j] == '?' {
					params, err := parseParams(source[j+1:])
					if err != nil {
						return err
					}
					dsn.Params = params
					break
				}
			}

			// Path
			dsn.Path = source[i:j]

			break
		}
	}

	if !foundSlash && len(source) > 0 {
		return errorInvalidDSNNoSlash
	}

	return nil
}

func parseParams(str string) (map[string]string, error) {
	params := make(map[string]string)
	// Parse query
	for key, values := range parseQuery(str) {
		if len(values) > 0 {
			params[key] = values[0]
		}
	}
	return params, nil
}

func parseQuery(query string) url.Values {
	if values, err := url.ParseQuery(query); err == nil {
		return values
	}
	return make(url.Values)
}
