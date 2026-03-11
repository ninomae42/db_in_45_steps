package domain

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

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
• Match the keyword, case-insensitive.
• On success, advance pos and return true.
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
	for pos < len(p.buf) {
		r, size := utf8.DecodeRuneInString(p.buf[pos:])
		if r == rune(encloseChar) {
			break
		}
		if r == '\\' {
			pos += size
			if len(p.buf) <= pos {
				return errors.New("incomplete escape sequence")
			}
			// re-interpret 1 character(not byte) after the escape sequence
			r, size = utf8.DecodeRuneInString(p.buf[pos:])
		}
		buf.WriteRune(r)
		pos += size
	}

	if len(p.buf) <= pos {
		return errors.New("unclosed string value")
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

func (p *Parser) skipSpaces() {
	for p.pos < len(p.buf) && helper.IsSpace(p.buf[p.pos]) {
		p.pos += 1
	}
}

func (p *Parser) isEnd() bool {
	p.skipSpaces()
	return p.pos >= len(p.buf)
}
