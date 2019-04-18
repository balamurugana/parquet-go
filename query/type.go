package query

// Type - value type.
type Type int8

const (
	// Null - represents NULL value type.
	Null Type = iota + 1

	// Bool - represents boolean value type.
	Bool

	// Int - represents integer value type.
	Int

	// Float - represents floating point value type.
	Float

	// String - represents string value type.
	String

	// Array - represents array of values where each value type is one of this type.
	Array

	// Record - represents map where key is String and value is one of this type.
	Record
)

func (t Type) isNumber() bool {
	switch t {
	case Int, Float:
		return true
	}

	return false
}
