package influxql

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
)

type AlterConfigStatement struct {
	Setting map[string]int64
	DB      string
}

func (s *AlterConfigStatement) node() {

}

func (s *AlterConfigStatement) String() string {
	if s.Setting == nil {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("alter config on ")
	buf.WriteString(fmt.Sprintf("\"%v\"", s.DB))
	for k, v := range s.Setting {
		buf.WriteString(" ")
		buf.WriteString(k)
		buf.WriteString(" ")
		buf.WriteString(fmt.Sprint(v))
	}
	return buf.String()
}

func (s AlterConfigStatement) stmt() {

}

func (s *AlterConfigStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.DB, Privilege: ReadPrivilege}}, nil

}

type ShowConfigStatement struct {
	DB string
}

func (s *ShowConfigStatement) node() {
}

func (s *ShowConfigStatement) String() string {
	return fmt.Sprintf("show config on \"%v\"", s.DB)
}

func (s *ShowConfigStatement) stmt() {
}

func (s *ShowConfigStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.DB, Privilege: ReadPrivilege}}, nil

}

type DropConfigStatement struct {
	DB string
}

func (d *DropConfigStatement) node() {
}

func (d *DropConfigStatement) String() string {
	return fmt.Sprintf("DROP CONFIG ON \"%v\"", d.DB)
}

func (d *DropConfigStatement) stmt() {
}

func (d *DropConfigStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: d.DB, Privilege: ReadPrivilege}}, nil

}

func (p *Parser) parseShowConfigStatement() (*ShowConfigStatement, error) {
	err := p.parseTokens([]Token{ON})
	if err != nil {
		return nil, err
	}
	_, _, lit := p.ScanIgnoreWhitespace()
	if lit == "" {
		return nil, errors.New("database name required")
	}
	return &ShowConfigStatement{DB: lit}, nil
}
func (p *Parser) parseAlterConfigStatement() (*AlterConfigStatement, error) {
	stmt := &AlterConfigStatement{
		Setting: make(map[string]int64),
	}
	err := p.parseTokens([]Token{ON})
	if err != nil {
		return nil, err
	}
	_, _, lit := p.ScanIgnoreWhitespace()
	if lit == "" {
		return nil, errors.New("database name or * required")
	}
	stmt.DB = lit
	for {
		_, _, lit := p.ScanIgnoreWhitespace()
		if lit == "" {
			if len(stmt.Setting) == 0 {
				return nil, fmt.Errorf("require %v", settings)
			}
			break
		}
		var found bool
		for _, s := range settings {
			if lit == s {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("no setting named %v,require %v", lit, settings)
		}
		var v int64
		if strings.HasSuffix(lit, "Duration") {
			d, err := p.ParseDuration()
			if err != nil {
				return nil, err
			}
			v = int64(d)
		} else {
			d, err := p.ParseInt(0, math.MaxInt64)
			if err != nil {
				return nil, err
			}
			v = int64(d)
		}
		stmt.Setting[lit] = v

	}
	return stmt, nil
}

func (p *Parser) parseDropConfigStatement() (*DropConfigStatement, error) {
	err := p.parseTokens([]Token{ON})
	if err != nil {
		return nil, err
	}
	_, _, lit := p.ScanIgnoreWhitespace()
	if lit == "" {
		return nil, errors.New("database name required")
	}
	return &DropConfigStatement{DB: lit}, nil
}
