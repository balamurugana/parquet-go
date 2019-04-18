package query

func isStarExpr(expr Expr) bool {
	_, ok := expr.(*StarExpr)
	return ok
}

func isValueExpr(expr Expr) bool {
	_, ok := expr.(*ValueExpr)
	return ok
}

func isColumnExpr(expr Expr) bool {
	_, ok := expr.(*ColumnExpr)
	return ok
}

func isCountFunc(expr Expr) bool {
	aggregateFunc, ok := expr.(*AggregateFunc)
	if ok {
		ok = aggregateFunc.funcName == Count
	}

	return ok
}

func isConditionExpr(expr Expr) bool {
	return expr.returnType() == Bool && !expr.isAggregateExpr()
}
