package domain

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	"github.com/ninomae42/db_in_45_steps/helper"
)

type Parser struct {
	buf string
	pos int
}

func NewParser(s string) Parser {
	return Parser{buf: s, pos: 0}
}

/*
* tryKeyword
• Skip leading spaces.
• Match the keyword, case-insensitive. On success, advance pos and return true.
• Otherwise, return false.
• Keywords must be separated by space or punctuation.
*/
func (p *Parser) tryKeyword(kw string) bool {
	if p.isEnd() {
		return false
	}

	pos := p.pos
	for pos < len(p.buf) && !helper.IsSeparator(p.buf[pos]) {
		pos += 1
	}

	word := p.buf[p.pos:pos]
	if !strings.EqualFold(kw, word) {
		return false
	}

	p.pos = pos
	return true
}

/*
* tryName
• Skip leading spaces.
• First char is a letter or _, following chars are letters, digits, or _.
• On success, return true and advance pos.
• On failure, return false and keep pos.
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

func (p *Parser) parseValue(out *Cell) error {
	p.skipSpaces()
	if p.pos >= len(p.buf) {
		return errors.New("expect value")
	}
	ch := p.buf[p.pos]
	if ch == '"' || ch == '\'' {
		return p.parseString(out)
	} else if helper.IsDigit(ch) || ch == '-' || ch == '+' {
		return p.parseInt(out)
	} else {
		return errors.New("expect value")
	}
}

func (p *Parser) parseString(out *Cell) error {
	pos := p.pos
	encloseChar := p.buf[p.pos]
	pos += 1 // consume start enclose character

	buf := bytes.Buffer{}
	for pos < len(p.buf) && p.buf[pos] != encloseChar {
		if p.buf[pos] == '\\' {
			pos += 1
		}
		buf.WriteByte(p.buf[pos])
		pos += 1
	}

	if pos == len(p.buf) && p.buf[pos] != encloseChar {
		return errors.New("unclosed string value")
	}
	pos += 1 // consume end enclose character

	out.Type = TypeStr
	out.Str = append(out.Str, buf.Bytes()...)
	p.pos = pos

	return nil
}

func (p *Parser) parseInt(out *Cell) (err error) {
	pos := p.pos
	ch := p.buf[pos]
	for pos < len(p.buf) &&
		(ch == '+' || ch == '-' || helper.IsDigit(ch)) {
		pos += 1
		ch = p.buf[pos]
	}

	out.I64, err = strconv.ParseInt(p.buf[p.pos:pos], 10, 64)
	if err != nil {
		return err
	}
	out.Type = TypeI64
	p.pos = pos

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
