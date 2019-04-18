package query

import (
	"fmt"

	"github.com/minio/minio-go/pkg/set"
)

// ComparisonExpr - comparison function.
type ComparisonExpr struct {
	name          string
	operator      ComparisonOperator
	left          Expr
	right         Expr
	to            Expr
	aggregateExpr bool
	columnNames   set.StringSet
}

// String - returns string representation of this function.
func (expr *ComparisonExpr) String() string {
	switch expr.operator {
	case Between, NotBetween:
		return fmt.Sprintf("(%v %v %v AND %v)", expr.left, expr.operator, expr.right, expr.to)
	}

	return fmt.Sprintf("(%v %v %v)", expr.left, expr.operator, expr.right)
}

// AggregateValue - returns aggregated value.
func (expr *ComparisonExpr) aggregateValue() (*Value, error) {
	if !expr.aggregateExpr {
		panic(fmt.Errorf("not aggregate expression"))
	}

	leftValue, err := expr.left.aggregateValue()
	if err != nil {
		return nil, err
	}

	rightValue, err := expr.right.aggregateValue()
	if err != nil {
		return nil, err
	}

	var toValue *Value
	if expr.to != nil {
		toValue, err = expr.to.aggregateValue()
		if err != nil {
			return nil, err
		}
	}

	return NewBool(compare(expr.operator, leftValue, rightValue, toValue)), nil
}

func (expr *ComparisonExpr) aliasName() string {
	return expr.name
}

// ColumnNames - returns involved column names.
func (expr *ComparisonExpr) columnsInUse() set.StringSet {
	return expr.columnNames
}

// Eval - evaluates this function for given arg values and returns result as Value.
func (expr *ComparisonExpr) eval(reader *Reader) (*Value, error) {
	leftValue, err := expr.left.eval(reader)
	if err != nil {
		return nil, err
	}

	rightValue, err := expr.right.eval(reader)
	if err != nil {
		return nil, err
	}

	var toValue *Value
	if expr.to != nil {
		if toValue, err = expr.to.eval(reader); err != nil {
			return nil, err
		}
	}

	if expr.aggregateExpr {
		return nil, nil
	}

	return NewBool(compare(expr.operator, leftValue, rightValue, toValue)), nil
}

func (expr *ComparisonExpr) isAggregateExpr() bool {
	return expr.aggregateExpr
}

func (expr *ComparisonExpr) returnType() Type {
	return Bool
}

func (expr *ComparisonExpr) checkDataUsable(reader *Reader) (bool, uint64, error) {
	leftColumn := expr.columnNames.ToSlice()[0]
	statistics, err := reader.GetStatistics(leftColumn)
	if err != nil {
		return false, 0, err
	}

	usable, err := checkDataUsable(expr.operator, statistics, expr.right, expr.to)
	if err != nil {
		return false, 0, err
	}

	return usable, statistics.numRows, nil
}

func (expr *ComparisonExpr) negate() {
	switch expr.operator {
	case Equal:
		expr.operator = NotEqual
	case NotEqual:
		expr.operator = Equal
	case LessThan:
		expr.operator = GreaterThanEqual
	case GreaterThan:
		expr.operator = LessThanEqual
	case LessThanEqual:
		expr.operator = GreaterThan
	case GreaterThanEqual:
		expr.operator = LessThan
	case Between:
		expr.operator = NotBetween
	case In:
		expr.operator = NotIn
	case Like:
		expr.operator = NotLike
	case NotBetween:
		expr.operator = Between
	case NotIn:
		expr.operator = In
	case NotLike:
		expr.operator = Like
	}

	panic(fmt.Errorf("invalid comparison operator %v", expr.operator))
}

func (expr *ComparisonExpr) setName(name string) {
	expr.name = name
}

// Type - returns comparisonFunction or aggregateFunction type.
func (expr *ComparisonExpr) Type() Type {
	return Bool
}

// NewComparisonExpr - creates new comparison expression. The first expression must be a value or column expression and other expressions must be value expressions if any.
func NewComparisonExpr(name string, operator ComparisonOperator, exprs ...Expr) (Expr, error) {
	var columnNames set.StringSet

	left := exprs[0]
	switch {
	case isColumnExpr(left):
		columnNames = left.columnsInUse()
	case isValueExpr(left):
	default:
		panic(fmt.Errorf("first expression must be a column or value expression"))
	}

	for i, expr := range exprs[1:] {
		if !isValueExpr(expr) {
			panic(fmt.Errorf("expression %v must be a value expression", i+2))
		}
	}

	switch operator {
	case Equal, NotEqual:
		if len(exprs) != 2 {
			panic(fmt.Sprintf("operator %v expects exactly two arguments", operator))
		}

		right := exprs[1]
		if left.returnType() != right.returnType() {
			return nil, fmt.Errorf("operator %v: left and right expression evaluate to different return types (%v, %v)",
				operator, left.returnType(), right.returnType())
		}

		if isValueExpr(left) {
			lv, _ := left.aggregateValue()
			rv, _ := right.aggregateValue()
			return NewValueExpr(name, NewBool(compare(operator, lv, rv, nil))), nil
		}

		return &ComparisonExpr{
			name:          name,
			operator:      operator,
			left:          left,
			right:         right,
			aggregateExpr: left.isAggregateExpr() || right.isAggregateExpr(),
			columnNames:   columnNames,
		}, nil

	case LessThan, GreaterThan, LessThanEqual, GreaterThanEqual:
		if len(exprs) != 2 {
			panic(fmt.Sprintf("operator %v expects exactly two arguments", operator))
		}

		if !left.returnType().isNumber() {
			return nil, fmt.Errorf("operator %v: left side expression evaluate to %v, not number", operator, left.returnType())
		}

		right := exprs[1]
		if !right.returnType().isNumber() {
			return nil, fmt.Errorf("operator %v: right side expression evaluate to %v; not number", operator, right.returnType())
		}

		if isValueExpr(left) {
			lv, _ := left.aggregateValue()
			rv, _ := right.aggregateValue()
			return NewValueExpr(name, NewBool(compare(operator, lv, rv, nil))), nil
		}

		return &ComparisonExpr{
			name:          name,
			operator:      operator,
			left:          left,
			right:         right,
			aggregateExpr: left.isAggregateExpr() || right.isAggregateExpr(),
			columnNames:   columnNames,
		}, nil

	case Between, NotBetween:
		if len(exprs) != 3 {
			panic(fmt.Sprintf("operator %v expects exactly three arguments", operator))
		}

		if !left.returnType().isNumber() {
			return nil, fmt.Errorf("operator %v: left expression evaluate to returns %v, not number",
				operator, left.returnType())
		}

		from := exprs[1]
		if !from.returnType().isNumber() {
			return nil, fmt.Errorf("operator %v: from expression evaluate to returns %v, not number",
				operator, from.returnType())
		}

		to := exprs[2]
		if !to.returnType().isNumber() {
			return nil, fmt.Errorf("operator %v: to expression evaluate to returns %v, not number",
				operator, to.returnType())
		}

		if isValueExpr(left) {
			lv, _ := left.aggregateValue()
			fv, _ := from.aggregateValue()
			tv, _ := to.aggregateValue()
			return NewValueExpr(name, NewBool(compare(operator, lv, fv, tv))), nil
		}

		return &ComparisonExpr{
			name:          name,
			operator:      operator,
			left:          left,
			right:         from,
			to:            to,
			aggregateExpr: left.isAggregateExpr() || from.isAggregateExpr() || to.isAggregateExpr(),
			columnNames:   columnNames,
		}, nil

	case In, NotIn:
		if len(exprs) != 2 {
			panic(fmt.Sprintf("operator %v expects exactly two arguments", operator))
		}

		right := exprs[1]
		if right.returnType() != Array {
			return nil, fmt.Errorf("operator %v: right side expression evaluate to %v; not Array", operator, right.returnType())
		}

		rightValue, _ := right.aggregateValue()
		values := rightValue.ArrayValue()
		for i, value := range values {
			if value.Type() != Null && value.Type() != left.returnType() {
				return nil, fmt.Errorf("operator %v: right expression array[%v] return type %v differs with left expression",
					operator, i, value.Type())
			}
		}

		if isValueExpr(left) {
			lv, _ := left.aggregateValue()
			rv, _ := right.aggregateValue()
			return NewValueExpr(name, NewBool(compare(operator, lv, rv, nil))), nil
		}

		return &ComparisonExpr{
			name:          name,
			operator:      operator,
			left:          left,
			right:         right,
			aggregateExpr: left.isAggregateExpr() || right.isAggregateExpr(),
			columnNames:   columnNames,
		}, nil

	case Like, NotLike:
		if len(exprs) != 2 {
			panic(fmt.Sprintf("operator %v expects exactly two arguments", operator))
		}

		if left.returnType() != String {
			return nil, fmt.Errorf("operator %v: left side expression evaluate to %v, not string", operator, left.returnType())
		}

		right := exprs[1]
		if right.returnType() != String {
			return nil, fmt.Errorf("operator %v: right side expression evaluate to %v, not string", operator, right.returnType())
		}

		if isValueExpr(left) {
			lv, _ := left.aggregateValue()
			rv, _ := right.aggregateValue()
			return NewValueExpr(name, NewBool(compare(operator, lv, rv, nil))), nil
		}

		return &ComparisonExpr{
			name:          name,
			operator:      operator,
			left:          left,
			right:         right,
			aggregateExpr: left.isAggregateExpr() || right.isAggregateExpr(),
			columnNames:   columnNames,
		}, nil

	case IsNull, IsNotNull:
		if len(exprs) != 1 {
			panic(fmt.Sprintf("operator %v expects exactly one argument", operator))
		}

		operator = Equal
		if operator == IsNotNull {
			operator = NotEqual
		}

		if isValueExpr(left) {
			lv, _ := left.aggregateValue()
			rv := NewNull()
			return NewValueExpr(name, NewBool(compare(operator, lv, rv, nil))), nil
		}

		return &ComparisonExpr{
			name:          name,
			operator:      operator,
			left:          left,
			right:         NewValueExpr("", NewNull()),
			aggregateExpr: left.isAggregateExpr(),
			columnNames:   columnNames,
		}, nil
	}

	panic(fmt.Errorf("invalid comparison operator %v", operator))
}
