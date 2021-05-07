package parser

import (
	"errors"
)

type Tokener struct {
	statement  []byte
	pos        int
	prevToken  string
	curToken   string
	flushToken bool
	err        error
}

var ErrInvalidStatement = errors.New("Invalid command")

func NewTokener(statement []byte) *Tokener {
	return &Tokener{
		statement, 0, "", "", true, nil,
	}
}

//查看下一个token, 不弹出
func (tk *Tokener) Peek() (string, error) {
	if tk.err != nil {
		return "", tk.err
	}

	if tk.flushToken == true {
		token, err := tk.next()
		if err != nil {
			tk.err = err
			return "", err
		}
		tk.prevToken = tk.curToken
		tk.curToken = token
		tk.flushToken = false
	}

	return tk.curToken, nil
}

//弹出当前的token
func (tk *Tokener) Pop() {
	tk.flushToken = true
}

func (tk *Tokener) popByte() {
	tk.pos++
	if tk.pos > len(tk.statement) {
		tk.pos = len(tk.statement)
	}
}

func (tk *Tokener) peekByte() (byte, bool) {
	if tk.pos == len(tk.statement) {
		return 0, true
	}
	return tk.statement[tk.pos], false
}

func (tk *Tokener) next() (string, error) {
	if tk.err != nil {
		return "", tk.err
	}
	return tk.nextMetaState()
}

func (tk *Tokener) nextMetaState() (string, error) { //词法分析 得到单词
	for { //skip blank
		b, eof := tk.peekByte()
		if eof == true {
			return "", nil
		}
		if isBlank(b) == false { //\n \t ' '
			break
		}
		tk.popByte() //pos ++
	}
	//get char
	b, _ := tk.peekByte()
	if isSymbol(b) {
		tk.popByte() //pos ++
		return string(b), nil
	} else if b == '"' || b == '\'' {
		return tk.nextQuoteState() //得到“”字符串中的数据，如“123”得到123
	} else {
		return tk.nextTokenState() //得到单词也即标识符名
	}
}

func (tk *Tokener) nextTokenState() (string, error) { //得到单词
	var tmp []byte
	for {
		b, eof := tk.peekByte()
		if eof == true || isBlank(b) || isSymbol(b) { //终止条件
			return string(tmp), nil
		}
		tmp = append(tmp, b) //不停的拼接字符成单词
		tk.popByte()
	}
}

func (tk *Tokener) nextQuoteState() (string, error) {
	quote, _ := tk.peekByte() //引号
	tk.popByte()

	var tmp []byte
	for {
		b, eof := tk.peekByte()
		if eof == true {
			tk.err = ErrInvalidStatement
			return "", tk.err
		}
		if b == quote { //找到匹配的引号 即“”成对，停止找
			tk.popByte()
			break
		}
		tmp = append(tmp, b)
		tk.popByte()
	}

	return string(tmp), nil
}

func (tk *Tokener) ErrStat() []byte {
	tmp := make([]byte, len(tk.statement)+3)
	copy(tmp, tk.statement[:tk.pos])
	copy(tmp[tk.pos:], []byte("<< "))
	copy(tmp[tk.pos+3:], tk.statement[tk.pos:])
	return tmp
}

func isSymbol(b byte) bool {
	return b == '>' || b == '<' || b == '=' || b == '*' ||
		b == ',' || b == '(' || b == ')' || b == '.'
}

func isBlank(b byte) bool {
	return b == '\n' || b == ' ' || b == '\t'
}
