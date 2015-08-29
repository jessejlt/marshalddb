package marshalddb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

func setFieldWithKind(kind reflect.Kind, fromField reflect.Value, toField *reflect.Value) error {

	var err error
	switch kind {

	case reflect.String:
		toField.SetString(fromField.String())

	case reflect.Bool:
		err = setBool(fromField, toField)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		err = setInt(fromField, toField)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		err = setUint(fromField, toField)

	case reflect.Float32, reflect.Float64:
		err = setFloat(fromField, toField)

	case reflect.Slice:
		err = errors.New("ConvertFromAttributes: TODO convert from slice")

	case reflect.Array, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Struct:
		err = setJSON(fromField, toField)

	default:
		err = fmt.Errorf("ConvertFromAttributes: Conversion from=string to=%s", toField.Kind().String())

	}

	return err
}

func setBool(fieldEl reflect.Value, toField *reflect.Value) error {

	fromVal := fieldEl.String()
	n, err := strconv.ParseInt(fromVal, 10, 64)
	if err != nil {
		return fmt.Errorf("ConvertFromAttributes: Failed to convert string=%s to int", fromVal)
	}
	toField.SetBool(n != 0)
	return nil
}

func setInt(fieldEl reflect.Value, toField *reflect.Value) error {

	fromVal := fieldEl.String()
	n, err := strconv.ParseInt(fromVal, 10, 64)
	if err != nil {
		return fmt.Errorf("ConvertFromAttributes: Failed to convert string=%s to int", fromVal)
	}
	if toField.OverflowInt(n) {
		return fmt.Errorf("ConvertFromAttributes: Int conversion from value=%d will cause overflow", n)
	}
	toField.SetInt(n)
	return nil
}

func setUint(fieldEl reflect.Value, toField *reflect.Value) error {

	var (
		n   uint64
		err error
	)

	switch fieldEl.Kind() {

	case reflect.String:
		fromVal := fieldEl.String()
		n, err = strconv.ParseUint(fromVal, 10, 64)

	case reflect.Bool:
		if fieldEl.Bool() {
			n = 1
		} else {
			n = 0
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n = uint64(fieldEl.Int())

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n = fieldEl.Uint()

	case reflect.Float32, reflect.Float64:
		n = uint64(fieldEl.Float())

	default:
		err = errors.New("")
	}

	if err != nil {
		return fmt.Errorf("ConvertFromAttributes: Failed to convert from=%v to uint", fieldEl.Kind())
	}

	if toField.OverflowUint(n) {
		return fmt.Errorf("ConvertFromAttributes: Uint conversion from value=%d will cause overflow", n)
	}
	toField.SetUint(n)
	return nil
}

func setFloat(fieldEl reflect.Value, toField *reflect.Value) error {

	fromVal := fieldEl.String()
	n, err := strconv.ParseFloat(fromVal, toField.Type().Bits())
	if err != nil {
		return fmt.Errorf("ConvertFromAttributes: Failed to convert string=%s to float", fromVal)
	}
	if toField.OverflowFloat(n) {
		return fmt.Errorf("ConvertFromAttributes: Float conversion from value=%d will cause overflow", n)
	}
	toField.SetFloat(n)
	return nil
}

func setJSON(fieldEl reflect.Value, toField *reflect.Value) error {

	fromVal := fieldEl.String()
	// create a new instance of the target
	newTarget := reflect.New(toField.Type())
	// unmarshal our AttributeValue's value into our new target
	if err := json.Unmarshal([]byte(fromVal), newTarget.Interface()); err != nil {
		return fmt.Errorf("ConvertFromAttributes: Failed to unmarshal field=%s, into=%v", fromVal, toField.Type())
	}
	// set our target field with the unmarshaled result
	toField.Set(newTarget.Elem())
	return nil
}
