package dsn

import (
	"testing"
)

func TestParse(t *testing.T) {
	d, err := Parse("logger://username:password@http(127.0.0.1:8000)/log/upload?level=debug&ttl=1s")
	if err != nil {
		t.Errorf("parse failed, err = %s\n", err.Error())
		return
	}
	t.Logf("DSN=%#v\n", d)
}
