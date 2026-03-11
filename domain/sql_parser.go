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

/*
* tryKeyword
• Skip leading spaces.
• Match the keyword, case-insensitive.
• On success, advance pos and return true.
• Otherwise, return false.
• Keywords must be separated by space or punctuation.
*/
func (p *Parser) tryKeyword(kw string) bool {
	if p.isEnd() {
		return false
	}

	// インデックスが範囲内に収まっているか、キーワードと一致するか
	start, kwLen, bufLen := p.pos, len(kw), len(p.buf)
	if bufLen < start+kwLen ||
		!strings.EqualFold(p.buf[start:start+kwLen], kw) {
		return false
	}

	// インデックスが範囲内に収まっているか、キーワードの直後に区切り文字が存在するか
	if start+kwLen < bufLen &&
		!helper.IsSeparator(p.buf[start+kwLen]) {
		return false
	}

	p.pos += kwLen
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
	if !p.tryKeyword("SELECT") {
		return ErrExpectKeyword
	}

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

	return p.parseWhere(&out.keys)
}

func (p *Parser) parseWhere(out *[]NamedCell) error {
	if !p.tryKeyword("WHERE") {
		return ErrExpectKeyword
	}
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

func (p *Parser) skipSpaces() {
	for p.pos < len(p.buf) && helper.IsSpace(p.buf[p.pos]) {
		p.pos += 1
	}
}

func (p *Parser) isEnd() bool {
	p.skipSpaces()
	return p.pos >= len(p.buf)
}
