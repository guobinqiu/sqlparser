package parser

import (
	"fmt"
	"github.com/huandu/go-clone"
	"strings"
)

type SqlStatement interface {
	PgSql() string
	MsSql() string
}

type SqlDocument struct {
	SqlStatements []SqlStatement
	SqlVars       []SqlVar
	tk            *Tokener
	next          int
}

func NewSqlDocument(s string) *SqlDocument {
	return &SqlDocument{tk: NewTokener([]byte(s))}
}

func (doc *SqlDocument) eof() bool {
	return doc.tk.pos == len(doc.tk.statement)
}

func (doc *SqlDocument) PgSql() string {
	var s string
	for _, v := range doc.SqlStatements {
		s += v.PgSql()
	}
	return fmt.Sprintf("DO $$\n%s\nEND $$;", s)
}

func (doc *SqlDocument) MsSql() string {
	var s string
	for _, v := range doc.SqlStatements {
		s += v.MsSql()
	}
	return s
}

func (doc *SqlDocument) addSqlStatement(sqlStatement SqlStatement) {
	doc.SqlStatements = append(doc.SqlStatements, sqlStatement)
}

func (doc *SqlDocument) setVarValue(name, value string) bool {
	for _, v := range doc.SqlVars {
		if v.name == name {
			v.value = value
			return true
		}
	}
	return false
}

type SqlBlock struct {
	SqlStatements []SqlStatement
	SqlVars       []SqlVar
}

func (blk *SqlBlock) PgSql() string {
	var s string
	for _, v := range blk.SqlStatements {
		s += v.PgSql()
	}
	return fmt.Sprintf("BEGIN%s\nEND\n", s)
}

func (blk *SqlBlock) MsSql() string {
	var s string
	for _, v := range blk.SqlStatements {
		s += v.MsSql()
	}
	return s
}

func (blk *SqlBlock) addSqlStatement(sqlStatement SqlStatement) {
	blk.SqlStatements = append(blk.SqlStatements, sqlStatement)
}

func (blk *SqlBlock) setVarValue(name, value string) bool {
	for _, v := range blk.SqlVars {
		if v.name == name {
			v.value = value
			return true
		}
	}
	return false
}

func Parse(doc *SqlDocument) (SqlStatement, error) {
	for {
		if isInsertIntoValues(doc) {
			sqlStatement, _ := parseInsertIntoValues(doc)
			doc.addSqlStatement(sqlStatement)
			continue
		}
		if isInsertIntoSelect(doc) {
			sqlStatement, _ := parseInsertIntoSelect(doc)
			doc.addSqlStatement(sqlStatement)
			continue
		}
		if isSelectInto(doc) {
			sqlStatement, _ := parseSelectInto(doc)
			doc.addSqlStatement(sqlStatement)
			continue
		}
		if isSelect(doc) {
			sqlStatement, _ := parseSelect(doc)
			doc.addSqlStatement(sqlStatement)
			continue
		}
		if isDeclare(doc) {
			sqlStatement, _ := parseDeclareDoc(doc)
			doc.addSqlStatement(sqlStatement)
			continue
		}
		if isSet(doc) {
			sqlStatement, _ := parseSetDoc(doc)
			doc.addSqlStatement(sqlStatement)
			continue
		}
		if isWhile(doc) {
			sqlStatement, _ := parseWhile(doc)
			doc.addSqlStatement(sqlStatement)
			continue
		}
		token, _ := doc.tk.Peek()
		if strings.EqualFold(token, "begin") {
			doc.tk.Pop()
			sqlStatement, _ := parseSqlBlock(doc, &SqlBlock{})
			doc.addSqlStatement(sqlStatement)
			continue
		}
		if doc.eof() {
			break
		}
		doc.tk.Peek()
		doc.tk.Pop()
	}
	return doc, nil
}

func parseSqlBlock(doc *SqlDocument, blk *SqlBlock) (SqlStatement, error) {
	for {
		if isInsertIntoValues(doc) {
			sqlStatement, _ := parseInsertIntoValues(doc)
			blk.addSqlStatement(sqlStatement)
			continue
		}
		if isInsertIntoSelect(doc) {
			sqlStatement, _ := parseInsertIntoSelect(doc)
			blk.addSqlStatement(sqlStatement)
			continue
		}
		if isSelectInto(doc) {
			sqlStatement, _ := parseSelectInto(doc)
			blk.addSqlStatement(sqlStatement)
			continue
		}
		if isSelect(doc) {
			sqlStatement, _ := parseSelect(doc)
			blk.addSqlStatement(sqlStatement)
			continue
		}
		if isDeclare(doc) {
			sqlStatement, _ := parseDeclareBlk(doc, blk)
			blk.addSqlStatement(sqlStatement)
			continue
		}
		if isSet(doc) {
			sqlStatement, _ := parseSetBlk(doc, blk)
			blk.addSqlStatement(sqlStatement)
			continue
		}
		if doc.eof() {
			break
		}
		tmp := &SqlBlock{}
		token, _ := doc.tk.Peek()
		if strings.EqualFold(token, "end") {
			doc.tk.Pop()
			tmp.addSqlStatement(blk)
			break
		}
		if strings.EqualFold(token, "begin") {
			doc.tk.Pop()
			sqlStatement, _ := parseSqlBlock(doc, tmp)
			blk.addSqlStatement(sqlStatement)
			continue
		}
		doc.tk.Peek()
		doc.tk.Pop()
	}
	return blk, nil
}

func isInsertIntoValues(doc *SqlDocument) bool {
	cpDoc := clone.Clone(doc).(*SqlDocument)
	token, _ := cpDoc.tk.Peek()
	if !strings.EqualFold(token, "insert") {
		return false
	}
	cpDoc.tk.Pop()

	token, _ = cpDoc.tk.Peek()
	if !strings.EqualFold(token, "into") {
		return false
	}
	cpDoc.tk.Pop()

	for {
		token, _ = cpDoc.tk.Peek()
		if strings.EqualFold(token, "values") {
			return true
		}
		if isBegin(token) || token == "" {
			return false
		}
		cpDoc.tk.Pop()
	}
}

func parseInsertIntoValues(doc *SqlDocument) (SqlStatement, error) {
	doc.next = doc.tk.pos
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "insert") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	if !strings.EqualFold(token, "into") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	for {
		token, _ = doc.tk.Peek()
		if strings.EqualFold(token, "values") {
			break
		}
		if isBegin(token) || token == "" {
			return nil, fmt.Errorf("error")
		}
		doc.tk.Pop()
	}
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	if token != "(" {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	for {
		token, _ = doc.tk.Peek()
		if token == ")" {
			return &DQLCmd{s: string(doc.tk.statement[doc.next:doc.tk.pos])}, nil
		}
		if isBegin(token) || token == "" {
			return nil, fmt.Errorf("error")
		}
		doc.tk.Pop()
	}
}

func isInsertIntoSelect(doc *SqlDocument) bool {
	cpDoc := clone.Clone(doc).(*SqlDocument)
	token, _ := cpDoc.tk.Peek()
	if !strings.EqualFold(token, "insert") {
		return false
	}
	cpDoc.tk.Pop()

	token, _ = cpDoc.tk.Peek()
	if !strings.EqualFold(token, "into") {
		return false
	}
	cpDoc.tk.Pop()

	for {
		token, _ = cpDoc.tk.Peek()
		if strings.EqualFold(token, "select") {
			return true
		}
		if isBegin(token) || token == "" {
			return false
		}
		cpDoc.tk.Pop()
	}
}

func parseInsertIntoSelect(doc *SqlDocument) (SqlStatement, error) {
	doc.next = doc.tk.pos
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "insert") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	if !strings.EqualFold(token, "into") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	for {
		token, _ = doc.tk.Peek()
		if strings.EqualFold(token, "select") {
			return parseNestedSelect(doc, 0)
		}
		if isBegin(token) || token == "" {
			return nil, fmt.Errorf("error")
		}
		doc.tk.Pop()
	}
}

func isSelectInto(doc *SqlDocument) bool {
	cpDoc := clone.Clone(doc).(*SqlDocument)
	token, _ := cpDoc.tk.Peek()
	if !strings.EqualFold(token, "select") {
		return false
	}
	cpDoc.tk.Pop()

	for {
		token, _ = cpDoc.tk.Peek()
		if strings.EqualFold(token, "into") {
			return true
		}
		if isBegin(token) || token == "" {
			return false
		}
		cpDoc.tk.Pop()
	}
}

func parseSelectInto(doc *SqlDocument) (SqlStatement, error) {
	doc.next = doc.tk.pos
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "select") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	for {
		token, _ = doc.tk.Peek()
		if strings.EqualFold(token, "into") {
			break
		}
		if token == "" {
			return nil, fmt.Errorf("error")
		}
		doc.tk.Pop()
	}
	doc.tk.Pop()

	for {
		token, _ = doc.tk.Peek()
		if strings.EqualFold(token, "from") {
			return parseFrom(doc, 0)
		}
		if token == "" {
			return nil, fmt.Errorf("error")
		}
		doc.tk.Pop()
	}
}

func isSelect(doc *SqlDocument) bool {
	cpDoc := clone.Clone(doc).(*SqlDocument)
	token, _ := cpDoc.tk.Peek()
	if !strings.EqualFold(token, "select") {
		return false
	}
	doc.tk.Pop()

	for {
		token, _ = cpDoc.tk.Peek()
		if strings.EqualFold(token, "from") {
			return true
		}
		if token == "" {
			return false
		}
		cpDoc.tk.Pop()
	}
}

func parseSelect(doc *SqlDocument) (SqlStatement, error) {
	doc.next = doc.tk.pos
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "select") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	for {
		token, _ = doc.tk.Peek()
		if strings.EqualFold(token, "from") {
			return parseFrom(doc, 0)
		}
		if isBegin(token) || token == "" {
			return nil, fmt.Errorf("error")
		}
		doc.tk.Pop()
	}
}

func parseNestedSelect(doc *SqlDocument, nesting int) (SqlStatement, error) {
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "select") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	for {
		token, _ = doc.tk.Peek()
		if strings.EqualFold(token, "from") {
			return parseFrom(doc, nesting)
		}
		if isBegin(token) || token == "" {
			return nil, fmt.Errorf("error")
		}
		doc.tk.Pop()
	}
}

func parseFrom(doc *SqlDocument, nesting int) (SqlStatement, error) {
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "from") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	if token == "(" {
		nesting++
		doc.tk.Pop()
		return parseNestedSelect(doc, nesting)
	}

	return parsePostFrom(doc, nesting)
}

var keywords = []string{
	"insert",
	"select",
	"delete",
	"update",
	"drop",
	"create",
	"alter",
	"while",
	"return",
	"declare",
	"if",
	"set",
	"with",
	"truncate",
	"begin",
	"end",
}

func isBegin(token string) bool {
	for _, keyword := range keywords {
		if strings.EqualFold(token, keyword) {
			return true
		}
	}
	return false
}

//new: Starts with one of insert select update delete declare set ...
//select xxx from xxx as xxx <new>
//select xxx from xxx where xxx and xxx order by xxx limit xxx <new>
//select xxx from (select xxx from (select xxx from xxx) xxx) xxx as xxx <new>
//select xxx from (select xxx from xxx union all select xxx from xxx) xxx as xxx <new>
//select xxx from xxx join xxx on xxx = xxx <new>
func parsePostFrom(doc *SqlDocument, nesting int) (SqlStatement, error) {
	for {
		token, _ := doc.tk.Peek()
		if token == ")" {
			nesting--
		}
		if strings.EqualFold(token, "union") {
			return parseUnion(doc)
		}
		if strings.EqualFold(token, "join") {
			return parseJoin(doc)
		}
		if isBegin(token) || token == "" {
			if nesting > 0 {
				return nil, fmt.Errorf("error")
			}
			doc.tk.pos -= len(token)
			doc.tk.curToken = doc.tk.prevToken
			return &DQLCmd{s: string(doc.tk.statement[doc.next:doc.tk.pos])}, nil
		}
		doc.tk.Pop()
	}
}

func parseUnion(doc *SqlDocument) (SqlStatement, error) {
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "union") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	if strings.EqualFold(token, "all") {
		doc.tk.Pop()
	}

	return parseNestedSelect(doc, 0)
}

func parseJoin(doc *SqlDocument) (SqlStatement, error) {
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "join") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	if token == "(" {
		doc.tk.Pop()
		return parseNestedSelect(doc, 0)
	}

	for {
		token, _ = doc.tk.Peek()
		if strings.EqualFold(token, "join") {
			return parseJoin(doc)
		}
		if strings.EqualFold(token, "union") {
			return parseUnion(doc)
		}
		if isBegin(token) || token == "" {
			doc.tk.pos -= len(token)
			doc.tk.curToken = doc.tk.prevToken
			return &DQLCmd{s: string(doc.tk.statement[doc.next:doc.tk.pos])}, nil
		}
		doc.tk.Pop()
	}
}

//select
type DQLCmd struct {
	s string
}

func (cmd DQLCmd) PgSql() string {
	return cmd.s + ";"
}

func (cmd DQLCmd) MsSql() string {
	return cmd.s
}

//insert, update, delete
type DMLCmd struct {
	s string
}

func (cmd DMLCmd) PgSql() string {
	return cmd.s + ";"
}

func (cmd DMLCmd) MsSql() string {
	return cmd.s
}

//create, alter
type DDLCmd struct {
	s string
}

func (cmd DDLCmd) PgSql() string {
	return cmd.s + ";"
}

func (cmd DDLCmd) MsSql() string {
	return cmd.s
}

//type DCLCmd struct {
//
//}

type SqlVar struct {
	name  string
	typ   string
	value string
}

type DeclareCmd struct {
	sqlVars []SqlVar
}

func (cmd *DeclareCmd) PgSql() string {
	var a []string
	for _, v := range cmd.sqlVars {
		if strings.HasPrefix(v.name, "@") {
			v.name = "v_" + v.name[1:]
		}
		a = append(a, v.name+" "+v.typ)
	}
	return fmt.Sprintf("declare %s;", strings.Join(a, ", "))
}

func (cmd *DeclareCmd) MsSql() string {
	var a []string
	for _, v := range cmd.sqlVars {
		a = append(a, v.name+" "+v.typ)
	}
	return fmt.Sprintf("declare %s", strings.Join(a, ", "))
}

func isDeclare(doc *SqlDocument) bool {
	cpDoc := clone.Clone(doc).(*SqlDocument)
	token, _ := cpDoc.tk.Peek()
	return strings.EqualFold(token, "declare")
}

func parseDeclareBlk(doc *SqlDocument, parent *SqlBlock) (*DeclareCmd, error) {
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "declare") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	var sqlVars []SqlVar

	for {
		var sqlVar SqlVar

		token, _ = doc.tk.Peek()
		if isBegin(token) || token == "" {
			return nil, fmt.Errorf("error")
		}
		sqlVar.name = token
		doc.tk.Pop()

		token, _ = doc.tk.Peek()
		if isBegin(token) || token == "" {
			return nil, fmt.Errorf("error")
		}
		sqlVar.typ = token
		doc.tk.Pop()

		sqlVars = append(sqlVars, sqlVar)

		token, _ = doc.tk.Peek()
		if token == "," {
			doc.tk.Pop()
		} else if isBegin(token) || token == "" {
			doc.tk.pos -= len(token)
			doc.tk.curToken = doc.tk.prevToken
			break
		} else {
			return nil, fmt.Errorf("error")
		}
	}

	sqlVars = append(sqlVars, sqlVars...)
	parent.SqlVars = sqlVars
	return &DeclareCmd{sqlVars}, nil
}

func parseDeclareDoc(doc *SqlDocument) (*DeclareCmd, error) {
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "declare") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	var sqlVars []SqlVar

	for {
		var sqlVar SqlVar

		token, _ = doc.tk.Peek()
		if isBegin(token) || token == "" {
			return nil, fmt.Errorf("error")
		}
		sqlVar.name = token
		doc.tk.Pop()

		token, _ = doc.tk.Peek()
		if isBegin(token) || token == "" {
			return nil, fmt.Errorf("error")
		}
		sqlVar.typ = token
		doc.tk.Pop()

		sqlVars = append(sqlVars, sqlVar)

		token, _ = doc.tk.Peek()
		if token == "," {
			doc.tk.Pop()
		} else if isBegin(token) || token == "" {
			doc.tk.pos -= len(token)
			doc.tk.curToken = doc.tk.prevToken
			break
		} else {
			return nil, fmt.Errorf("error")
		}
	}

	sqlVars = append(sqlVars, sqlVars...)
	doc.SqlVars = sqlVars
	return &DeclareCmd{sqlVars}, nil
}

type SetCmd struct {
	name  string
	value string
}

func isSet(doc *SqlDocument) bool {
	cpDoc := clone.Clone(doc).(*SqlDocument)
	token, _ := cpDoc.tk.Peek()
	return strings.EqualFold(token, "set")
}

func parseSetDoc(doc *SqlDocument) (SqlStatement, error) {
	cmd := &SetCmd{}
	token, _ := doc.tk.Peek()
	if token != "set" {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	cmd.name = token
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	if token != "=" {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	for {
		token, _ = doc.tk.Peek()
		cmd.value += token
		if isBegin(token) || token == "" {
			break
		}
		doc.tk.Pop()
	}

	if !doc.setVarValue(cmd.name, cmd.value) {
		return nil, fmt.Errorf("error")
	}

	return cmd, nil
}

func parseSetBlk(doc *SqlDocument, parent *SqlBlock) (SqlStatement, error) {
	cmd := &SetCmd{}
	token, _ := doc.tk.Peek()
	if token != "set" {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	cmd.name = token
	doc.tk.Pop()

	token, _ = doc.tk.Peek()
	if token != "=" {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	for {
		token, _ = doc.tk.Peek()
		cmd.value += token
		if isBegin(token) || token == "" {
			break
		}
		doc.tk.Pop()
	}

	if !parent.setVarValue(cmd.name, cmd.value) {
		return nil, fmt.Errorf("error")
	}

	return cmd, nil
}

func (cmd *SetCmd) PgSql() string {
	if strings.HasPrefix(cmd.name, "@") {
		cmd.name = "v_" + cmd.name[1:]
	}
	return fmt.Sprintf("set %s=%s;", cmd.name, cmd.value)
}

func (cmd *SetCmd) MsSql() string {
	return fmt.Sprintf("set %s=%s;", cmd.name, cmd.value)
}

type WhileCmd struct {
	condition string
	SqlBlock  SqlStatement
}

func isWhile(doc *SqlDocument) bool {
	cpDoc := clone.Clone(doc).(*SqlDocument)
	token, _ := cpDoc.tk.Peek()
	return strings.EqualFold(token, "while")
}

//while 条件 begin ... end => while 条件 loop ... end loop;
func (cmd *WhileCmd) PgSql() string {
	return fmt.Sprintf("WHILE%sLOOP\n%sEND LOOP;", cmd.condition, cmd.SqlBlock.MsSql())
}

func (cmd *WhileCmd) MsSql() string {
	return fmt.Sprintf("WHILE%sBEGIN\n%sEND", cmd.condition, cmd.SqlBlock.MsSql())
}

func parseWhile(doc *SqlDocument) (SqlStatement, error) {
	while := &WhileCmd{}
	token, _ := doc.tk.Peek()
	if !strings.EqualFold(token, "while") {
		return nil, fmt.Errorf("error")
	}
	doc.tk.Pop()

	conditionStart := doc.tk.pos
	for {
		token, _ = doc.tk.Peek()
		if strings.EqualFold(token, "begin") {
			break
		}
		if token == "" {
			return nil, fmt.Errorf("error")
		}
		doc.tk.Pop()
	}
	while.condition = string(doc.tk.statement[conditionStart : doc.tk.pos-len(token)])
	while.SqlBlock, _ = parseSqlBlock(doc, &SqlBlock{})
	return while, nil
}
