package query

import "fmt"

// ArithOperator - arithmetic operator.
type ArithOperator int8

const (
	// Add operator '+'.
	Add ArithOperator = iota + 1

	// Subtract operator '-'.
	Subtract

	// Multiply operator '*'.
	Multiply

	// Divide operator '/'.
	Divide

	// Modulo operator '%'.
	Modulo
)

func (operator ArithOperator) String() string {
	switch operator {
	case Add:
		return "+"
	case Subtract:
		return "-"
	case Multiply:
		return "*"
	case Divide:
		return "/"
	case Modulo:
		return "%"
	}

	panic(fmt.Errorf("invalid arithmetic operator %v", operator))
}

// ComparisonOperator - comparison operator.
type ComparisonOperator int8

const (
	// Equal operator '='.
	Equal ComparisonOperator = iota + 1

	// NotEqual operator '!=' or '<>'.
	NotEqual

	// LessThan operator '<'.
	LessThan

	// GreaterThan operator '>'.
	GreaterThan

	// LessThanEqual operator '<='.
	LessThanEqual

	// GreaterThanEqual operator '>='.
	GreaterThanEqual

	// Between operator 'BETWEEN'
	Between

	// In operator 'IN'
	In

	// Like operator 'LIKE'
	Like

	// NotBetween operator 'NOT BETWEEN'
	NotBetween

	// NotIn operator 'NOT IN'
	NotIn

	// NotLike operator 'NOT LIKE'
	NotLike

	// IsNull operator 'IS NULL'
	IsNull

	// IsNotNull operator 'IS NOT NULL'
	IsNotNull
)

func (operator ComparisonOperator) String() string {
	switch operator {
	case Equal:
		return "="
	case NotEqual:
		return "!="
	case LessThan:
		return "<"
	case GreaterThan:
		return ">"
	case LessThanEqual:
		return "<="
	case GreaterThanEqual:
		return ">="
	case Between:
		return "BETWEEN"
	case In:
		return "IN"
	case Like:
		return "LIKE"
	case NotBetween:
		return "NOT BETWEEN"
	case NotIn:
		return "NOT IN"
	case NotLike:
		return "NOT LIKE"
	case IsNull:
		return "IS NULL"
	case IsNotNull:
		return "IS NOT NULL"
	}

	panic(fmt.Errorf("invalid comparison operator %v", operator))
}

// LogicalOperator - logical operator.
type LogicalOperator int8

const (
	// AND operator.
	AND LogicalOperator = iota + 1

	// OR operator.
	OR

	// NOT operator.
	NOT
)

func (operator LogicalOperator) String() string {
	switch operator {
	case AND:
		return "AND"
	case OR:
		return "OR"
	case NOT:
		return "NOT"
	}

	panic(fmt.Errorf("invalid logical operator %v", operator))
}
