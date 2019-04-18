package query

import (
	"fmt"

	"github.com/minio/minio-go/pkg/set"
)

type LogicalExpr struct {
	name          string
	operator      LogicalOperator
	left          ConditionExpr
	right         ConditionExpr
	aggregateExpr bool
	columnNames   set.StringSet
}

func (expr *LogicalExpr) aggregateValue() (*Value, error) {
	if !expr.aggregateExpr {
		panic(fmt.Errorf("not aggregate expression"))
	}

	leftValue, err := expr.left.aggregateValue()
	if err != nil {
		return nil, err
	}

	switch expr.operator {
	case AND:
		if !leftValue.BoolValue() {
			return leftValue, nil
		}

		rightValue, err := expr.right.aggregateValue()
		if err != nil {
			return nil, err
		}

		return rightValue, nil

	case OR:
		if leftValue.BoolValue() {
			return leftValue, nil
		}

		rightValue, err := expr.right.aggregateValue()
		if err != nil {
			return nil, err
		}

		return rightValue, nil
	}

	panic(fmt.Errorf("invalid logical operator %v", expr.operator))
}

func (expr *LogicalExpr) aliasName() string {
	return expr.name
}

func (expr *LogicalExpr) columnsInUse() set.StringSet {
	return expr.columnNames
}

func (expr *LogicalExpr) eval(reader *Reader) (*Value, error) {
	leftValue, err := expr.left.eval(reader)
	if err != nil {
		return nil, err
	}

	switch expr.operator {
	case AND:
		if !leftValue.BoolValue() {
			return leftValue, nil
		}

		rightValue, err := expr.right.eval(reader)
		if err != nil {
			return nil, err
		}

		return rightValue, nil

	case OR:
		if leftValue.BoolValue() {
			return leftValue, nil
		}

		rightValue, err := expr.right.eval(reader)
		if err != nil {
			return nil, err
		}

		return rightValue, nil
	}

	panic(fmt.Errorf("invalid logical operator %v", expr.operator))
}

func (expr *LogicalExpr) isAggregateExpr() bool {
	return expr.aggregateExpr
}

func (expr *LogicalExpr) returnType() Type {
	return Bool
}

func (expr *LogicalExpr) checkDataUsable(reader *Reader) (bool, uint64, error) {
	leftUsable, leftNumRows, err := expr.left.checkDataUsable(reader)
	if err != nil {
		return false, 0, err
	}

	rightUsable, rightNumRows, err := expr.right.checkDataUsable(reader)
	if err != nil {
		return false, 0, err
	}

	max := func() uint64 {
		if leftNumRows > rightNumRows {
			return leftNumRows
		}

		return rightNumRows
	}

	if leftUsable && rightUsable {
		return true, max(), nil
	}

	if !leftUsable && !rightUsable {
		return false, max(), nil
	}

	switch expr.operator {
	case AND:
		if leftUsable {
			return false, rightNumRows, nil
		}
		return false, leftNumRows, nil

	case OR:
		if leftUsable {
			return true, leftNumRows, nil
		}

		return true, rightNumRows, nil
	}

	panic(fmt.Errorf("invalid logical operator %v", expr.operator))
}

func (expr *LogicalExpr) negate() {
	expr.left.negate()
	expr.right.negate()

	switch expr.operator {
	case AND:
		expr.operator = OR
	case OR:
		expr.operator = AND
	}

	panic(fmt.Errorf("invalid logical operator %v", expr.operator))
}

func (expr *LogicalExpr) setName(name string) {
	expr.name = name
}

func (expr *LogicalExpr) Type() Type {
	return Bool
}

// NewLogicalExpr - creates new logical expression. Argument expressions must be a value, comparison or logical expression.
func NewLogicalExpr(name string, operator LogicalOperator, exprs ...ConditionExpr) (Expr, error) {
	left := exprs[0]
	if left.returnType() != Bool {
		return nil, fmt.Errorf("operator %v: first expression evaluate to %v, not bool", operator, left.returnType())
	}

	switch operator {
	case AND, OR:
		if len(exprs) != 2 {
			panic(fmt.Sprintf("operator %v expects exactly two arguments", operator))
		}

		right := exprs[1]
		if right.returnType() != Bool {
			return nil, fmt.Errorf("operator %v: second expression evaluate to %v; not bool", operator, right.returnType())
		}

		if isValueExpr(left) {
			if operator == AND {
				if lv, _ := left.aggregateValue(); !lv.BoolValue() {
					// As left expression evaluates to false, no need to have right expression and return false value expression.
					return left, nil
				}
			} else {
				if lv, _ := left.aggregateValue(); lv.BoolValue() {
					// As left expression evaluates to true, no need to have right expression and return true value expression.
					return left, nil
				}
			}

			return right, nil
		}

		if isValueExpr(right) {
			if operator == AND {
				if rv, _ := right.aggregateValue(); !rv.BoolValue() {
					// As right expression evaluates to false, no need to have left expression and return false value expression.
					return right, nil
				}
			} else {
				if rv, _ := right.aggregateValue(); rv.BoolValue() {
					// As right expression evaluates to true, no need to have left expression and return true value expression.
					return right, nil
				}
			}

			return left, nil
		}

		// Here, left and right expression have columns usage.
		columnNames := left.columnsInUse()
		rightColumnNames := right.columnsInUse()
		switch {
		case columnNames == nil:
			columnNames = rightColumnNames
		default:
			columnNames = columnNames.Union(rightColumnNames)
		}

		return &LogicalExpr{
			name:          name,
			operator:      operator,
			left:          left,
			right:         right,
			aggregateExpr: left.isAggregateExpr() || right.isAggregateExpr(),
			columnNames:   columnNames,
		}, nil

	case NOT:
		if len(exprs) != 1 {
			panic(fmt.Sprintf("operator %v expects exactly one argument", operator))
		}

		if isValueExpr(left) {
			lv, _ := left.aggregateValue()
			return NewValueExpr(name, NewBool(!lv.BoolValue())), nil
		}

		left.negate()
		left.setName(name)
		return left, nil
	}

	panic(fmt.Errorf("invalid logical operator %v", operator))
}
