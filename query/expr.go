package query

import "github.com/minio/minio-go/pkg/set"

// Expr - a SQL expression type.
type Expr interface {
	aggregateValue() (*Value, error)
	aliasName() string
	columnsInUse() set.StringSet
	eval(reader *Reader) (*Value, error)
	isAggregateExpr() bool
	returnType() Type
	checkDataUsable(*Reader) (bool, uint64, error)
	Type() Type
}

type ConditionExpr interface {
	Expr
	negate()
	setName(string)
}

// StarExpr - asterisk (*) expression.
type StarExpr struct {
}

// String - returns string representation of this expression.
func (expr *StarExpr) String() string {
	return "*"
}

// AggregateValue - returns nil value.
func (expr *StarExpr) aggregateValue() (*Value, error) {
	panic("star expression does not support aggregate value")
}

// Name - returns name of this expression.
func (expr *StarExpr) aliasName() string {
	return ""
}

// ColumnNames - returns involved column names.
func (expr *StarExpr) columnsInUse() set.StringSet {
	return set.CreateStringSet("*")
}

// Eval - returns given args as map value.
func (expr *StarExpr) eval(reader *Reader) (*Value, error) {
	return nil, nil
}

// IsAggregateExpr - returns whether it is aggregate expression or not.
func (expr *StarExpr) isAggregateExpr() bool {
	return false
}

func (expr *StarExpr) checkDataUsable(reader *Reader) (bool, uint64, error) {
	return true, 0, nil
}

// ReturnType - returns record as return type.
func (expr *StarExpr) returnType() Type {
	return Record
}

// Type - returns record type.
func (expr *StarExpr) Type() Type {
	return Record
}

// NewStarExpr - returns new asterisk (*) expression.
func NewStarExpr() *StarExpr {
	return &StarExpr{}
}

type ValueExpr struct {
	value *Value
	name  string
}

func (expr *ValueExpr) String() string {
	return expr.value.String()
}

func (expr *ValueExpr) aggregateValue() (*Value, error) {
	return expr.value, nil
}

// Name - returns name of this expression.
func (expr *ValueExpr) aliasName() string {
	return expr.name
}

// ColumnNames - returns involved column names.
func (expr *ValueExpr) columnsInUse() set.StringSet {
	return nil
}

func (expr *ValueExpr) eval(reader *Reader) (*Value, error) {
	return expr.value, nil
}

// IsAggregateExpr - returns whether it is aggregate expression or not.
func (expr *ValueExpr) isAggregateExpr() bool {
	return false
}

func (expr *ValueExpr) returnType() Type {
	return expr.value.Type()
}

func (expr *ValueExpr) checkDataUsable(reader *Reader) (bool, uint64, error) {
	return true, 0, nil
}

func (expr *ValueExpr) negate() {
}

func (expr *ValueExpr) Type() Type {
	return expr.value.Type()
}

func NewValueExpr(name string, value *Value) *ValueExpr {
	return &ValueExpr{
		name:  name,
		value: value,
	}
}

type ColumnExpr struct {
	name       string
	columnName string
	columnType Type
}

func (expr *ColumnExpr) String() string {
	return expr.columnName
}

func (expr *ColumnExpr) aggregateValue() (*Value, error) {
	panic("column expression does not support aggregate value")
}

// Name - returns name of this expression.
func (expr *ColumnExpr) aliasName() string {
	return expr.name
}

// ColumnNames - returns involved column names.
func (expr *ColumnExpr) columnsInUse() set.StringSet {
	return set.CreateStringSet(expr.columnName)
}

func (expr *ColumnExpr) eval(reader *Reader) (*Value, error) {
	return nil, nil
}

// IsAggregateExpr - returns whether it is aggregate expression or not.
func (expr *ColumnExpr) isAggregateExpr() bool {
	return false
}

func (expr *ColumnExpr) returnType() Type {
	return expr.columnType
}

func (expr *ColumnExpr) checkDataUsable(reader *Reader) (bool, uint64, error) {
	return true, 0, nil
}

func (expr *ColumnExpr) Type() Type {
	return expr.columnType
}

func NewColumnExpr(name, columnName string, columnType Type) *ColumnExpr {
	return &ColumnExpr{
		name:       name,
		columnName: columnName,
		columnType: columnType,
	}
}
