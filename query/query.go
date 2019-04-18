package query

import (
	"fmt"
	"io"

	"github.com/minio/minio-go/pkg/set"
)

type Result struct {
	Name  string
	Value interface{}
}

type Query struct {
	selections  []Expr
	condition   Expr
	eof         bool
	columnNames set.StringSet
	reader      *Reader
}

func (query *Query) Execute(reader *Reader) ([]Result, error) {
	if query.eof {
		return nil, io.EOF
	}

	if query.condition == nil && len(query.selections) == 1 && isCountFunc(query.selections[0]) {
		query.eof = true
		return []Result{{query.selections[0].aliasName(), reader.fileMetadata.NumRows}}, nil
	}

	usableRows := uint64(0)
	if query.condition != nil {
		// Move reader to suitable row group.
		for {
			usable, _, err := query.condition.checkDataUsable(query.reader)
			if err != nil {
				return nil, err
			}

			if usable {
				break
			}

			if err := query.reader.SkipRowGroup(); err != nil {
				return nil, err
			}
		}

		// Move reader to suitable data page.
		if err := query.reader.ReadColumns(); err != nil {
			return nil, err
		}

		for {
			usable, numRows, err := query.condition.checkDataUsable(query.reader)
			if err != nil {
				return nil, err
			}

			if usable {
				usableRows = numRows
				break
			}

			if err := query.reader.SkipRows(numRows); err != nil {
				return nil, err
			}
		}
	}

	// Decode values in all column pages.
	if err := query.reader.DecodeColumns(); err != nil {
		return nil, err
	}

	// Iterate values in all columns and eval query.
	rows, err := query.reader.Pop(usableRows)
	for i := uint64(0); i < usableRows; i++ {
	}

	panic("FIXME")
}

func NewQuery(selections []Expr, condition Expr, reader *Reader) *Query {
	eof := false
	if condition != nil {
		if !isConditionExpr(condition) {
			panic(fmt.Errorf("condition expression must evaluate to bool and not aggregate expression"))
		}

		if isValueExpr(condition) {
			condition = nil
			if value, _ := condition.aggregateValue(); !value.BoolValue() {
				eof = true
			}
		}
	}

	var columnNames set.StringSet
	if condition != nil {
		columnNames = condition.columnsInUse()
	}

	if len(selections) == 0 {
		panic(fmt.Errorf("empty selection expressions"))
	}

	starExprFound := false
	aggregateExpr := false
	for i, expr := range selections {
		if starExprFound {
			panic(fmt.Errorf("only one star expression must be defined and must not mixed with other expression"))
		}

		if isStarExpr(expr) {
			starExprFound = true
			continue
		}

		if expr.aliasName() == "" {
			panic(fmt.Errorf("selection expression %v must have alias name", i+1))
		}

		if aggregateExpr && !expr.isAggregateExpr() {
			panic(fmt.Errorf("mixed aggregate and non-aggregate selection expressions"))
		}

		if expr.isAggregateExpr() && !aggregateExpr {
			aggregateExpr = true
		}
	}

	for _, expr := range selections {
		argColumnNames := expr.columnsInUse()
		if argColumnNames == nil {
			continue
		}

		if argColumnNames.Contains("*") {
			columnNames = set.CreateStringSet("*")
			break
		}

		if columnNames == nil {
			columnNames = argColumnNames
			continue
		}

		columnNames = columnNames.Union(argColumnNames)
	}

	reader.SetRequiredColumns(columnNames)

	return &Query{
		selections:  selections,
		condition:   condition,
		eof:         eof,
		columnNames: columnNames,
		reader:      reader,
	}
}
