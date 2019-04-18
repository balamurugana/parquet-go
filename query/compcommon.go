package query

import (
	"fmt"
	"regexp"
)

func equal(leftValue, rightValue *Value) bool {
	if leftValue.IsNull() {
		return rightValue.IsNull()
	}

	if rightValue.IsNull() {
		return false
	}

	switch leftValue.Type() {
	case Bool:
		return leftValue.BoolValue() == rightValue.BoolValue()
	case Int, Float:
		return leftValue.FloatValue() == rightValue.FloatValue()
	case String:
		return leftValue.StringValue() == rightValue.StringValue()
	}

	panic(fmt.Errorf("unsupported left value type %v", leftValue.Type()))
}

func lessThan(leftValue, rightValue *Value) bool {
	if leftValue.IsNull() || rightValue.IsNull() {
		return false
	}

	switch leftValue.Type() {
	case Int, Float:
		return leftValue.FloatValue() < rightValue.FloatValue()
	}

	panic(fmt.Errorf("unsupported left value type %v", leftValue.Type()))
}

func greaterThan(leftValue, rightValue *Value) bool {
	if leftValue.IsNull() || rightValue.IsNull() {
		return false
	}

	switch leftValue.Type() {
	case Int, Float:
		return leftValue.FloatValue() > rightValue.FloatValue()
	}

	panic(fmt.Errorf("unsupported left value type %v", leftValue.Type()))
}

func lessThanEqual(leftValue, rightValue *Value) bool {
	if leftValue.IsNull() || rightValue.IsNull() {
		return false
	}

	switch leftValue.Type() {
	case Int, Float:
		return leftValue.FloatValue() <= rightValue.FloatValue()
	}

	panic(fmt.Errorf("unsupported left value type %v", leftValue.Type()))
}

func greaterThanEqual(leftValue, rightValue *Value) bool {
	if leftValue.IsNull() || rightValue.IsNull() {
		return false
	}

	switch leftValue.Type() {
	case Int, Float:
		return leftValue.FloatValue() >= rightValue.FloatValue()
	}

	panic(fmt.Errorf("unsupported left value type %v", leftValue.Type()))
}

func between(leftValue, fromValue, toValue *Value) bool {
	if leftValue.IsNull() {
		return false
	}

	switch leftValue.Type() {
	case Int, Float:
		return leftValue.FloatValue() >= fromValue.FloatValue() &&
			leftValue.FloatValue() <= toValue.FloatValue()
	}

	panic(fmt.Errorf("unsupported left value type %v", leftValue.Type()))
}

func in(leftValue, rightValue *Value) bool {
	values := rightValue.ArrayValue()

	for _, value := range values {
		if equal(leftValue, value) {
			return true
		}
	}

	return false
}

func like(leftValue, rightValue *Value) bool {
	if leftValue.IsNull() {
		return false
	}

	matched, err := regexp.MatchString(rightValue.StringValue(), leftValue.StringValue())
	if err != nil {
		panic(fmt.Errorf("regexp.MatchString() failed, err = %v", err))
	}

	return matched
}

func compare(operator ComparisonOperator, leftValue, rightValue, toValue *Value) bool {
	switch operator {
	case Equal:
		return equal(leftValue, rightValue)
	case NotEqual:
		return !equal(leftValue, rightValue)
	case LessThan:
		return lessThan(leftValue, rightValue)
	case GreaterThan:
		return greaterThan(leftValue, rightValue)
	case LessThanEqual:
		return lessThanEqual(leftValue, rightValue)
	case GreaterThanEqual:
		return greaterThanEqual(leftValue, rightValue)
	case Between:
		return between(leftValue, rightValue, toValue)
	case NotBetween:
		return !between(leftValue, rightValue, toValue)
	case In:
		return in(leftValue, rightValue)
	case NotIn:
		return !in(leftValue, rightValue)
	case Like:
		return like(leftValue, rightValue)
	case NotLike:
		return !like(leftValue, rightValue)
	}

	panic(fmt.Errorf("invalid comparison operator %v", operator))
}

func checkDataUsableEqual(rightValue, minValue, maxValue *Value) (bool, error) {
	switch rightValue.Type() {
	case Bool:
		return rightValue.BoolValue() == maxValue.BoolValue(), nil

	case Int, Float:
		return greaterThanEqual(rightValue, minValue) && lessThanEqual(rightValue, maxValue), nil

	case String:
		rightValueBytes := []byte(rightValue.StringValue())
		minBytes := []byte(minValue.StringValue())
		maxBytes := []byte(maxValue.StringValue())

		if len(rightValueBytes) > 0 {
			if len(maxBytes) > 0 {
				if len(minBytes) > 0 {
					return rightValueBytes[0] >= minBytes[0] && rightValueBytes[0] <= maxBytes[0], nil
				}

				return rightValueBytes[0] <= maxBytes[0], nil
			}

			return false, nil
		}

		// Right value length is zero here.
		return len(minBytes) == 0 || len(maxBytes) == 0, nil
	}

	panic(fmt.Errorf("invalid value type %v", rightValue.Type()))
}

func checkDataUsable(operator ComparisonOperator, statistics *Statistics, rightExpr, toExpr Expr) (bool, error) {
	// If statistics is not nil and max/min values are nil, this means the column does not have statistics value.
	// Hence use this data.
	if statistics != nil && (statistics.max == nil || statistics.min == nil) {
		return true, nil
	}

	rightValue, err := rightExpr.aggregateValue()
	if err != nil {
		return false, err
	}

	// If statistics is nil, all values of the column are nil.
	// Conditions 'column = NULL' and 'column IN [NULL, ...]' make this data usable.
	if statistics == nil {
		isRightValueNull := false
		switch {
		case rightValue.IsNull():
			isRightValueNull = true

		case rightValue.Type() == Array:
			for _, value := range rightValue.ArrayValue() {
				if value.IsNull() {
					isRightValueNull = true
				}
			}
		}

		if isRightValueNull && operator == Equal || operator == In {
			return true, nil
		}

		return false, nil
	}

	maxValue := statistics.max
	minValue := statistics.min

	switch operator {
	case Equal:
		return checkDataUsableEqual(rightValue, minValue, maxValue)

	case GreaterThan:
		return greaterThan(minValue, rightValue) || greaterThan(maxValue, rightValue), nil

	case GreaterThanEqual:
		return greaterThanEqual(minValue, rightValue) || greaterThanEqual(maxValue, rightValue), nil

	case LessThan:
		return lessThan(minValue, rightValue) || lessThan(maxValue, rightValue), nil

	case LessThanEqual:
		return lessThanEqual(minValue, rightValue) || lessThanEqual(maxValue, rightValue), nil

	case Between:
		toValue, err := toExpr.aggregateValue()
		if err != nil {
			return false, err
		}

		return lessThanEqual(rightValue, maxValue) && greaterThanEqual(toValue, minValue), nil

	case In:
		for _, value := range rightValue.ArrayValue() {
			usable, err := checkDataUsableEqual(value, minValue, maxValue)
			if err != nil {
				return false, err
			}

			if usable {
				return true, nil
			}
		}

		return false, nil

	case Like:
		rightValueBytes := []byte(rightValue.StringValue())
		minBytes := []byte(minValue.StringValue())
		maxBytes := []byte(maxValue.StringValue())

		if len(rightValueBytes) > 0 {
			switch rightValueBytes[0] {
			case '*', '?', '[', '{':
				return true, nil
			}

			if len(maxBytes) > 0 {
				if len(minBytes) > 0 {
					return rightValueBytes[0] >= minBytes[0] && rightValueBytes[0] <= maxBytes[0], nil
				}

				return rightValueBytes[0] <= maxBytes[0], nil
			}

			return false, nil
		}

		// Right value length is zero here.
		return len(minBytes) == 0 || len(maxBytes) == 0, nil

	case NotEqual:
		return !(equal(rightValue, minValue) && equal(rightValue, maxValue)), nil

	case NotBetween:
		toValue, err := toExpr.aggregateValue()
		if err != nil {
			return false, err
		}

		return !(equal(rightValue, minValue) && equal(toValue, maxValue)), nil

	case NotIn:
		return true, nil

	case NotLike:
		return true, nil
	}

	return true, nil
}
