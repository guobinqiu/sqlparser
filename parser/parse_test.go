package parser

import (
	"fmt"
	"testing"
)

func TestDocumentParse(t *testing.T) {
	s := `
select *
from (select * from (select * from t1 union all
select * from t1) a)
b select * from t2 insert into t1(name,age)
values('xyz', 1)
select * into t1 from t2 insert into t1 select * from t2
`
	doc := NewSqlDocument(s)
	sql, _ := Parse(doc)
	fmt.Println(sql.PgSql())
}

func TestWhile(t *testing.T) {
	s := `
while @date < '2019-07-16'
begin
select * from t1
end
`
	doc := NewSqlDocument(s)
	sql, _ := Parse(doc)
	fmt.Println(sql.PgSql())
	//fmt.Println(sql.MsSql())
}

func TestSqlBlock(t *testing.T) {
	s := `
select * from t0
begin
select * from t1
end
begin
select * from t2
end
select * from t3
`
	doc := NewSqlDocument(s)
	sql, _ := Parse(doc)
	fmt.Println(sql.PgSql())
}

func TestSqlBlock2(t *testing.T) {
	s := `
select * from t0
begin
select * from t1
begin
select * from t2
end
end
select * from t3
`
	doc := NewSqlDocument(s)
	sql, _ := Parse(doc)
	fmt.Println(sql.PgSql())
}

func TestDocVarOK(t *testing.T) {
	s := `
declare @i int
set @i=1
`
	doc := NewSqlDocument(s)
	sql, _ := Parse(doc)
	fmt.Println(sql.PgSql())
}

//func TestDocVarFail(t *testing.T) {
//	s := `
//declare @i int
//set @j=1
//`
//	doc := NewSqlDocument(s)
//	sql, _ := Parse(doc)
//	fmt.Println(sql.PgSql())
//}

func TestBlkVarOK(t *testing.T) {
	s := `
begin
declare @i int
set @i=1
end
`
	doc := NewSqlDocument(s)
	sql, _ := Parse(doc)
	fmt.Println(sql.PgSql())
}

//func TestBlkVarFail(t *testing.T) {
//	s := `
//begin
//declare @i int
//set @j=1
//end
//`
//	doc := NewSqlDocument(s)
//	sql, _ := Parse(doc)
//	fmt.Println(sql.PgSql())
//}

func TestSet(t *testing.T) {
	s := `
declare @date date
set @date=dateadd(dd, 1, @date)
`
	doc := NewSqlDocument(s)
	sql, _ := Parse(doc)
	fmt.Println(sql.PgSql())
}

func TestBlockParse(t *testing.T) {
	s := `
begin
select *
from (select * from (select * from t1 union all
select * from t1) a)
b select * from t2 insert into t1(name,age)
values('xyz', 1)
select * into t1 from t2 insert into t1 select * from t2
end
`
	doc := NewSqlDocument(s)
	sql, _ := Parse(doc)
	fmt.Println(sql.PgSql())
}
