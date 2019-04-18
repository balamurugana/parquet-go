package query

import (
	"fmt"

	"github.com/minio/minio-go/pkg/set"
)

func compute(operator ArithOperator, lv, rv *Value) *Value {
	if lv.IsNull() || rv.IsNull() {
		return NewNull()
	}

	leftValue := lv.FloatValue()
	rightValue := rv.FloatValue()

	var result float64
	switch operator {
	case Add:
		result = leftValue + rightValue
	case Subtract:
		result = leftValue - rightValue
	case Multiply:
		result = leftValue * rightValue
	case Divide:
		result = leftValue / rightValue
	case Modulo:
		result = float64(int64(leftValue) % int64(rightValue))
	default:
		panic(fmt.Errorf("unknown operator %v", operator))
	}

	if lv.Type() == Float || rv.Type() == Float {
		return NewFloat(result)
	}

	return NewInt(int64(result))
}

// ArithExpr - arithmetic function.
type ArithExpr struct {
	name          string
	operator      ArithOperator
	left          Expr
	right         Expr
	aggregateExpr bool
	retType       Type
	columnNames   set.StringSet
}

// String - returns string representation of this function.
func (expr *ArithExpr) String() string {
	return fmt.Sprintf("(%v %v %v)", expr.left, expr.operator, expr.right)
}

// AggregateValue - returns aggregated value.
func (expr *ArithExpr) aggregateValue() (*Value, error) {
	if !expr.aggregateExpr {
		return nil, fmt.Errorf("%v is not aggreate expression", expr)
	}

	lv, err := expr.left.aggregateValue()
	if err != nil {
		return nil, err
	}

	rv, err := expr.right.aggregateValue()
	if err != nil {
		return nil, err
	}

	return compute(expr.operator, lv, rv), nil
}

// Name - returns name of this expression.
func (expr *ArithExpr) aliasName() string {
	return expr.name
}

// ColumnNames - returns involved column names.
func (expr *ArithExpr) columnsInUse() set.StringSet {
	return expr.columnNames
}

// Eval - evaluates this function for given arg values and returns result as Value.
func (expr *ArithExpr) eval(reader *Reader) (*Value, error) {
	leftValue, err := expr.left.eval(reader)
	if err != nil {
		return nil, err
	}

	rightValue, err := expr.right.eval(reader)
	if err != nil {
		return nil, err
	}

	if expr.aggregateExpr {
		return nil, nil
	}

	return compute(expr.operator, leftValue, rightValue), nil
}

// IsAggregateExpr - returns whether it is aggregate expression or not.
func (expr *ArithExpr) isAggregateExpr() bool {
	return expr.aggregateExpr
}

// ReturnType - returns return type.
func (expr *ArithExpr) returnType() Type {
	return expr.retType
}

func (expr *ArithExpr) checkDataUsable(reader *Reader) (bool, uint64, error) {
	return true, 0, nil
}

// Type - returns arithmeticFunction or aggregateFunction type.
func (expr *ArithExpr) Type() Type {
	return expr.retType
}

// NewArithExpr - creates new arithmetic function.
func NewArithExpr(name string, operator ArithOperator, left, right Expr) (Expr, error) {
	if !left.returnType().isNumber() {
		return nil, fmt.Errorf("operator %v: left side expression %v evaluate to %v, not number", operator, left, left.returnType())
	}

	if !right.returnType().isNumber() {
		return nil, fmt.Errorf("operator %v: right side expression %v evaluate to %v, not number", operator, right, right.returnType())
	}

	if isValueExpr(left) && isValueExpr(right) {
		lv, _ := left.aggregateValue()
		rv, _ := right.aggregateValue()
		return NewValueExpr(name, compute(operator, lv, rv)), nil
	}

	returnType := Int
	if left.returnType() == Float || right.returnType() == Float {
		returnType = Float
	}

	var columnNames set.StringSet
	leftColumnNames := left.columnsInUse()
	rightColumnNames := right.columnsInUse()
	switch {
	case leftColumnNames == nil:
		columnNames = rightColumnNames
	case rightColumnNames == nil:
		columnNames = leftColumnNames
	default:
		columnNames = leftColumnNames.Union(rightColumnNames)
	}

	return &ArithExpr{
		name:          name,
		operator:      operator,
		left:          left,
		right:         right,
		aggregateExpr: left.isAggregateExpr() || right.isAggregateExpr(),
		retType:       returnType,
		columnNames:   columnNames,
	}, nil
}
