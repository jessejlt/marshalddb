package marshalddb

import "errors"

var (
	// ErrNilTarget if a target interface is a nil pointer
	ErrNilTarget = errors.New("Target interface must be not be nil")
	// ErrInvalidFloat if an AttributeValue is marked as a float but
	// is not a number or is infinite
	ErrInvalidFloat = errors.New("Invalid float")
	// ErrInvalidJSON if reflection and subsequent marshel to JSON fails
	ErrInvalidJSON = errors.New("Invalid JSON")
	// ErrInvalidStringForNumber if unable to reflect from a string to an number
	ErrInvalidStringForNumber = errors.New("Invalid String Conversion")
	// ErrNumericOverflow if conversion to target numeric type will cause overflow
	ErrNumericOverflow = errors.New("Numeric Overflow")
	// ErrConversionNotSupported if a conversion from an AttributeValue
	// isn't yet supported
	ErrConversionNotSupported = errors.New("Unsupported Conversion")
	// ErrInvalidConversion if an AttributeValue type reflection is not possible
	ErrInvalidConversion = errors.New("Invalid Conversion")
)
