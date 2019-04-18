package query

import (
	"fmt"

	"github.com/minio/minio-go/pkg/set"
)

// AggregateFuncName - SQL function name.
type AggregateFuncName int8

const (
	// Avg - aggregate SQL function AVG().
	Avg AggregateFuncName = iota + 1

	// Count - aggregate SQL function COUNT().
	Count

	// Max - aggregate SQL function MAX().
	Max

	// Min - aggregate SQL function MIN().
	Min

	// Sum - aggregate SQL function SUM().
	Sum
)

func (funcName AggregateFuncName) String() string {
	switch funcName {
	case Avg:
		return "AVG"
	case Count:
		return "COUNT"
	case Max:
		return "MAX"
	case Min:
		return "MIN"
	case Sum:
		return "SUM"
	}

	panic(fmt.Errorf("unknown aggregate function name %v", funcName))
}

// AggregateFunc - SQL function.
type AggregateFunc struct {
	name     string
	funcName AggregateFuncName
	arg      Expr

	sumValue   float64
	countValue int64
	maxValue   float64
	minValue   float64
}

func (expr *AggregateFunc) count(reader *Reader) error {
	value, err := expr.arg.eval(reader)
	if err != nil {
		return err
	}

	if value.Type() != Null {
		expr.countValue++
	}

	return nil
}

func (expr *AggregateFunc) max(reader *Reader) error {
	value, err := expr.arg.eval(reader)
	if err != nil {
		return err
	}

	v := value.FloatValue()
	if v > expr.maxValue {
		expr.maxValue = v
	}

	return nil
}

func (expr *AggregateFunc) min(reader *Reader) error {
	value, err := expr.arg.eval(reader)
	if err != nil {
		return err
	}

	v := value.FloatValue()
	if v < expr.minValue {
		expr.minValue = v
	}

	return nil
}

func (expr *AggregateFunc) sum(reader *Reader) error {
	value, err := expr.arg.eval(reader)
	if err != nil {
		return err
	}

	expr.sumValue += value.FloatValue()
	expr.countValue++
	return nil
}

// String - returns string representation of this function.
func (expr *AggregateFunc) String() string {
	return fmt.Sprintf("%v(%v)", expr.funcName, expr.arg)
}

// AggregateValue - returns aggregated value.
func (expr *AggregateFunc) aggregateValue() (*Value, error) {
	switch expr.funcName {
	case Avg:
		return NewFloat(expr.sumValue / float64(expr.countValue)), nil
	case Count:
		return NewInt(expr.countValue), nil
	case Max:
		return NewFloat(expr.maxValue), nil
	case Min:
		return NewFloat(expr.minValue), nil
	case Sum:
		return NewFloat(expr.sumValue), nil
	}

	return nil, fmt.Errorf("%v is not aggreate function", expr)
}

func (expr *AggregateFunc) aliasName() string {
	return expr.name
}

// ColumnNames - returns involved column names.
func (expr *AggregateFunc) columnsInUse() set.StringSet {
	return expr.arg.columnsInUse()
}

// Eval - evaluates this function for given arg values and returns result as Value.
func (expr *AggregateFunc) eval(reader *Reader) (*Value, error) {
	switch expr.funcName {
	case Avg, Sum:
		return nil, expr.sum(reader)
	case Count:
		return nil, expr.count(reader)
	case Max:
		return nil, expr.max(reader)
	case Min:
		return nil, expr.min(reader)
	}

	panic(fmt.Sprintf("unsupported aggregate function %v", expr.funcName))
}

func (expr *AggregateFunc) isAggregateExpr() bool {
	return true
}

// ReturnType - returns respective primitive type depending on SQL function.
func (expr *AggregateFunc) returnType() Type {
	switch expr.funcName {
	case Avg, Max, Min, Sum:
		return Float
	case Count:
		return Int
	}

	panic(fmt.Errorf("invalid aggregate function"))
}

func (expr *AggregateFunc) checkDataUsable(reader *Reader) (bool, uint64, error) {
	return true, 0, nil
}

// Type - returns Function or aggregateFunction type.
func (expr *AggregateFunc) Type() Type {
	return expr.returnType()
}

// NewAggregateFunc - creates new aggregate function expression.
func NewAggregateFunc(name string, funcName AggregateFuncName, arg Expr) (*AggregateFunc, error) {
	switch funcName {
	case Avg, Max, Min, Sum:
		if !arg.returnType().isNumber() {
			return nil, fmt.Errorf("%v(): argument %v evaluate to %v, not number", funcName, arg, arg.returnType())
		}
	}

	return &AggregateFunc{
		name:     name,
		funcName: funcName,
		arg:      arg,
	}, nil
}
