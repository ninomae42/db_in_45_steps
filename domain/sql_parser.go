package domain

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/ninomae42/db_in_45_steps/helper"
)

var (
	ErrIncompleteEscape = errors.New("incomplete escape sequence")
	ErrUnclosedString   = errors.New("unclosed string value")
	ErrExpectValue      = errors.New("expect value")
	ErrExpectColumn     = errors.New("expect column name")
	ErrExpectColumnList = errors.New("expect column list")
	ErrExpectTable      = errors.New("expect table name")
	ErrExpectEqual      = errors.New("expect '='")
	ErrExpectComma      = errors.New("expect comma")
	ErrExpectKeyword    = errors.New("expect keyword")
	ErrExpectAND        = errors.New("expect AND")
	ErrExpectWhere      = errors.New("expect where clause")
)

type Parser struct {
	buf string
	pos int
}

func NewParser(s string) Parser {
	return Parser{buf: s, pos: 0}
}

type StmtSelect struct {
	table string
	cols  []string
	keys  []NamedCell
}

type NamedCell struct {
	column string
	value  Cell
}

type StmtCreatTable struct {
	table string
	cols  []Column
	pkey  []string
}

type StmtInsert struct {
	table string
	value []Cell
}

type StmtUpdate struct {
	table string
	keys  []NamedCell
	value []NamedCell
}

type StmtDelete struct {
	table string
	keys  []NamedCell
}

/*
* tryKeyword
• Skip leading spaces.
• Match the keyword, case-insensitive.
• On success, advance pos and return true.
• Otherwise, return false.
• Keywords must be separated by space or punctuation.
*/
func (p *Parser) tryKeyword(kws ...string) bool {
	save := p.pos
	bufLen := len(p.buf)
	for _, kw := range kws {
		p.skipSpaces()
		// インデックスが範囲内に収まっているか、キーワードと一致するか
		start, kwLen := p.pos, len(kw)
		if bufLen < start+kwLen ||
			!strings.EqualFold(p.buf[start:start+kwLen], kw) {
			p.pos = save
			return false
		}

		// インデックスが範囲内に収まっているか、キーワードの直後に区切り文字が存在するか
		if start+kwLen < bufLen &&
			!helper.IsSeparator(p.buf[start+kwLen]) {
			p.pos = save
			return false
		}
		p.pos += kwLen
	}

	return true
}

/*
* tryName
• Skip leading spaces.
• First char is a letter or _, following chars are letters, digits, or _.
• On success, return true and advance pos.
• On failure, return false.
*/
func (p *Parser) tryName() (string, bool) {
	if p.isEnd() {
		return "", false
	}

	pos := p.pos
	if !helper.IsNameStart(p.buf[pos]) {
		return "", false
	}
	pos += 1

	for pos < len(p.buf) && helper.IsNameContinue(p.buf[pos]) {
		pos += 1
	}

	s := p.buf[p.pos:pos]
	p.pos = pos
	return s, true
}

func (p *Parser) tryPunctuation(tok string) bool {
	p.skipSpaces()
	if !(p.pos+len(tok) <= len(p.buf) && p.buf[p.pos:p.pos+len(tok)] == tok) {
		return false
	}
	p.pos += len(tok)
	return true
}

func (p *Parser) parseValue(out *Cell) error {
	p.skipSpaces()
	if p.pos >= len(p.buf) {
		return ErrExpectValue
	}
	ch := p.buf[p.pos]
	if ch == '"' || ch == '\'' {
		return p.parseString(out)
	} else if helper.IsDigit(ch) || ch == '-' || ch == '+' {
		return p.parseInt(out)
	} else {
		return ErrExpectValue
	}
}

func (p *Parser) parseString(out *Cell) error {
	pos := p.pos
	encloseChar := p.buf[p.pos]
	pos += 1 // consume start enclose character

	buf := bytes.Buffer{}
	for pos < len(p.buf) {
		r, size := utf8.DecodeRuneInString(p.buf[pos:])
		if r == rune(encloseChar) {
			break
		}
		if r == '\\' {
			pos += size
			if len(p.buf) <= pos {
				return ErrIncompleteEscape
			}
			// re-interpret 1 character(not byte) after the escape sequence
			r, size = utf8.DecodeRuneInString(p.buf[pos:])
		}
		buf.WriteRune(r)
		pos += size
	}

	if len(p.buf) <= pos {
		return ErrUnclosedString
	}
	pos += 1 // consume end enclose character

	out.Type = TypeStr
	out.Str = append([]byte(nil), buf.Bytes()...) // ensure overwrite
	p.pos = pos

	return nil
}

func (p *Parser) parseInt(out *Cell) (err error) {
	pos := p.pos
	for pos < len(p.buf) &&
		(p.buf[pos] == '+' || p.buf[pos] == '-' || helper.IsDigit(p.buf[pos])) {
		pos += 1
	}

	val, err := strconv.ParseInt(p.buf[p.pos:pos], 10, 64)
	if err != nil {
		return err
	}
	out.Type = TypeI64
	out.I64 = val
	p.pos = pos

	return nil
}

func (p *Parser) parseEqual(out *NamedCell) error {
	var ok bool
	out.column, ok = p.tryName()
	if !ok {
		return ErrExpectColumn
	}
	if !p.tryPunctuation("=") {
		return ErrExpectEqual
	}
	return p.parseValue(&out.value)
}

func (p *Parser) parseSelect(out *StmtSelect) error {
	for !p.tryKeyword("FROM") {
		if 0 < len(out.cols) && !p.tryPunctuation(",") {
			return ErrExpectComma
		}
		if name, ok := p.tryName(); ok {
			out.cols = append(out.cols, name)
		} else {
			return ErrExpectColumn
		}
	}

	if len(out.cols) == 0 {
		return ErrExpectColumnList
	}

	var ok bool
	out.table, ok = p.tryName()
	if !ok {
		return ErrExpectTable
	}

	if !p.tryKeyword("WHERE") {
		return ErrExpectKeyword
	}
	return p.parseWhere(&out.keys)
}

func (p *Parser) parseWhere(out *[]NamedCell) error {
	conds := []NamedCell{}
	for !p.tryPunctuation(";") {
		if 0 < len(conds) && !p.tryKeyword("AND") {
			return ErrExpectAND
		}
		cell := NamedCell{}
		if err := p.parseEqual(&cell); err != nil {
			return err
		} else {
			conds = append(conds, cell)
		}
	}
	if len(conds) == 0 {
		return ErrExpectWhere
	}
	*out = append(*out, conds...)
	return nil
}

// parseCreateTable
// parse create table statement.
// create table t (a int64, b string, c string, primary key (b, c));
func (p *Parser) parseCreateTable(out *StmtCreatTable) error {
	var ok bool
	// parse table name
	if out.table, ok = p.tryName(); !ok {
		return ErrExpectTable
	}
	if !p.tryPunctuation("(") {
		return errors.New("expect (")
	}
	// parse columns
	for !p.tryKeyword("PRIMARY", "KEY") {
		p.skipSpaces()
		if 0 < len(out.cols) && !p.tryPunctuation(",") {
			return ErrExpectComma
		}

		if p.tryKeyword("PRIMARY", "KEY") {
			break
		}

		ok = true
		col := Column{}
		if col.Name, ok = p.tryName(); !ok {
			return errors.New("expect column name")
		}
		if p.tryKeyword("INT64") {
			col.Type = TypeI64
		} else if p.tryKeyword("STRING") {
			col.Type = TypeStr
		} else {
			return errors.New("expect column name")
		}
		out.cols = append(out.cols, col)
	}
	if len(out.cols) == 0 {
		return ErrExpectColumnList
	}

	// parse primary keys
	if !p.tryPunctuation("(") {
		return errors.New("expect (")
	}
	for !p.tryPunctuation(")") {
		if 0 < len(out.pkey) && !p.tryPunctuation(",") {
			return ErrExpectComma
		}
		if keyName, ok := p.tryName(); !ok {
			return errors.New("expect key name")
		} else {
			out.pkey = append(out.pkey, keyName)
		}
	}
	if len(out.pkey) == 0 {
		return errors.New("expect primary keys")
	}

	if !p.tryPunctuation(")") {
		return errors.New("expect )")
	}
	if !p.tryPunctuation(";") {
		return errors.New("expect ;")
	}
	return nil
}

// parseInsert
// parse insert statement.
// ex) insert into t values (1, 'x', 'y');
func (p *Parser) parseInsert(out *StmtInsert) error {
	var ok bool
	// parse table name
	if out.table, ok = p.tryName(); !ok {
		return ErrExpectTable
	}

	if !p.tryKeyword("VALUES") {
		return ErrExpectKeyword
	}
	if !p.tryPunctuation("(") {
		return errors.New("expect (")
	}

	for !p.tryPunctuation(")") {
		if 0 < len(out.value) && !p.tryPunctuation(",") {
			return ErrExpectComma
		}
		val := Cell{}
		if err := p.parseValue(&val); err != nil {
			return err
		} else {
			out.value = append(out.value, val)
		}
	}
	if len(out.value) == 0 {
		return errors.New("expect values")
	}

	if !p.tryPunctuation(";") {
		return errors.New("expect ;")
	}
	return nil
}

// parseUpdate
// parse update statement.
// ex) update t set a = 1 where b = 'x' and c = 'y';
func (p *Parser) parseUpdate(out *StmtUpdate) error {
	var ok bool
	// parse table name
	if out.table, ok = p.tryName(); !ok {
		return ErrExpectTable
	}

	if !p.tryKeyword("SET") {
		return ErrExpectKeyword
	}

	// parse values
	for !p.tryKeyword("WHERE") {
		if 0 < len(out.value) && !p.tryPunctuation(",") {
			return ErrExpectComma
		}

		val := NamedCell{}
		if err := p.parseEqual(&val); err != nil {
			return err
		} else {
			out.value = append(out.value, val)
		}
	}

	if err := p.parseWhere(&out.keys); err != nil {
		return err
	}
	return nil
}

// parseDelete
// parse delete statement.
// ex) delete from t where b = 'x' and c = 'y';
func (p *Parser) parseDelete(out *StmtDelete) error {
	var ok bool
	// parse table name
	if out.table, ok = p.tryName(); !ok {
		return ErrExpectTable
	}

	if !p.tryKeyword("WHERE") {
		return ErrExpectKeyword
	}
	if err := p.parseWhere(&out.keys); err != nil {
		return err
	}
	return nil
}

func (p *Parser) parseStmt() (out interface{}, err error) {
	if p.tryKeyword("SELECT") {
		stmt := &StmtSelect{}
		err = p.parseSelect(stmt)
		out = stmt
	} else if p.tryKeyword("CREATE", "TABLE") {
		stmt := &StmtCreatTable{}
		err = p.parseCreateTable(stmt)
		out = stmt
	} else if p.tryKeyword("INSERT", "INTO") {
		stmt := &StmtInsert{}
		err = p.parseInsert(stmt)
		out = stmt
	} else if p.tryKeyword("UPDATE") {
		stmt := &StmtUpdate{}
		err = p.parseUpdate(stmt)
		out = stmt
	} else if p.tryKeyword("DELETE", "FROM") {
		stmt := &StmtDelete{}
		err = p.parseDelete(stmt)
		out = stmt
	} else {
		err = errors.New("unknown statement")
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (p *Parser) skipSpaces() {
	for p.pos < len(p.buf) && helper.IsSpace(p.buf[p.pos]) {
		p.pos += 1
	}
}

func (p *Parser) isEnd() bool {
	p.skipSpaces()
	return p.pos >= len(p.buf)
}
