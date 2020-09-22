package influxql

import (
	"bytes"
	"fmt"
)

type SetStatement struct {
	Setting map[string]int
	DB      string
}

func (s *SetStatement) node() {

}

func (s *SetStatement) String() string {
	if s.Setting == nil {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("SET DATABASE ")
	buf.WriteString(fmt.Sprintf("\"%v\"", s.DB))
	for k, v := range s.Setting {
		buf.WriteString(" ")
		buf.WriteString(k)
		buf.WriteString(" ")
		buf.WriteString(fmt.Sprint(v))
	}
	return buf.String()
}

func (s SetStatement) stmt() {

}

func (s *SetStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.DB, Privilege: ReadPrivilege}}, nil

}
