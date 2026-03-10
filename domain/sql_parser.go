package domain

import (
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
	p.skipSpaces()
	if p.isEnd() {
		return false
	}

	pos := p.pos
	for !helper.IsSeparator(p.buf[pos]) {
		pos += 1
	}

	word := p.buf[p.pos:pos]
	if strings.ToLower(kw) != strings.ToLower(word) {
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
	p.skipSpaces()
	if p.isEnd() {
		return "", false
	}

	pos := p.pos
	if !helper.IsNameStart(p.buf[pos]) {
		return "", false
	}
	pos += 1

	for helper.IsNameContinue(p.buf[pos]) {
		pos += 1
	}

	s := p.buf[p.pos:pos]
	p.pos = pos
	return s, true
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
