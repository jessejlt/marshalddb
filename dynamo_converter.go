package marshalddb

import (
	"math"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/awslabs/aws-sdk-go/aws"
)

// ConvertFromAttributes maps a DB returned map[string]*dynamodb.AttributeValue into a specified struct.
func ConvertFromAttributes(item map[string]*dynamodb.AttributeValue, v interface{}) error {

	to := reflect.ValueOf(v)
	if to.Kind() != reflect.Ptr || to.IsNil() {
		return ErrNilTarget
	}
	toEl := to.Elem()

	for key, attrValue := range item {

		attrValueV := reflect.ValueOf(attrValue)
		// This check is questionable since the compiler guarantees it
		// since we're checking that `attrValueV is a *dynamodb.AttributeValue`,
		// But for correctness the check is valid. Possibly remove for *performance*
		if attrValueV.Kind() == reflect.Ptr && !attrValueV.IsNil() {

			// At this point we have a `*dynamodb.AttributeValue` and
			// need to iterate through its elements as described by
			// https://github.com/awslabs/aws-sdk-go/blob/master/service/dynamodb/api.go#L726
			// and find the first non-nil field, which will then become
			// our target for conversion
			attrValueEl := attrValueV.Elem()
			numFields := attrValueEl.NumField()
			for i := 0; i < numFields; i++ {

				field := attrValueEl.Field(i)
				fk := field.Kind()
				if fk == reflect.Struct ||
					(fk == reflect.Ptr && field.IsNil()) ||
					(fk == reflect.Slice && field.IsNil()) ||
					(fk == reflect.Map && field.Len() == 0) {
					continue
				}

				// `field` is our target for conversion, now we
				// need to find a field by the same name in our
				// target struct and then make sure we can set
				// a value on said field

				// toField := toEl.FieldByName(key)
				toField := fieldByName(toEl, key)
				if toField.CanSet() {

					var fieldEl reflect.Value
					if fk == reflect.Slice || fk == reflect.Map {

						fieldEl = field
					} else {

						fieldEl = field.Elem()
					}

					typeOfAttrValue := attrValueEl.Type()
					attrValueName := typeOfAttrValue.Field(i).Name

					err := setFieldVal(
						attrValueName,
						fieldEl,
						&toField,
						typeOfAttrValue,
					)
					if err != nil {
						return err
					}

				}

				break
			}
		}
	}

	return nil
}

// ConvertToAttributes converts a struct into a dynamodb representation
func ConvertToAttributes(v interface{}) (map[string]*dynamodb.AttributeValue, error) {

	to := make(map[string]*dynamodb.AttributeValue)
	ev := reflect.ValueOf(v)
	if ev.Kind() == reflect.Ptr || ev.Kind() == reflect.Interface {
		ev = ev.Elem()
	}
	et := ev.Type()

	switch ev.Kind() {

	case reflect.Struct:

		for i := 0; i < ev.NumField(); i++ {

			f := ev.Field(i)
			fk := f.Kind()
			if fk == reflect.Ptr {
				f = f.Elem()
				fk = f.Kind()
			}

			if !f.IsValid() || reflect.Zero(f.Type()) == f {
				continue
			}

			fieldName := et.Field(i).Name
			if tn := et.Field(i).Tag.Get("json"); tn != "" {
				// prefer to use the struct tag name over the field name
				fieldName = tn
			}

			switch fk {

			case reflect.String:
				// Dynamo does not allow setting empty strings
				// http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
				if f.String() != "" {

					to[fieldName] = &dynamodb.AttributeValue{
						S: aws.String(f.String()),
					}
				}

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				to[fieldName] = &dynamodb.AttributeValue{
					N: aws.String(strconv.FormatInt(f.Int(), 10)),
				}

			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				to[fieldName] = &dynamodb.AttributeValue{
					N: aws.String(strconv.FormatUint(f.Uint(), 10)),
				}

			case reflect.Float32, reflect.Float64:
				ff := f.Float()
				if math.IsInf(ff, 0) || math.IsNaN(ff) {
					return to, ErrInvalidFloat
				}
				to[fieldName] = &dynamodb.AttributeValue{
					N: aws.String(strconv.FormatFloat(ff, 'g', -1, f.Type().Bits())),
				}

			case reflect.Bool:
				to[fieldName] = &dynamodb.AttributeValue{
					BOOL: aws.Boolean(f.Bool()),
				}

			case reflect.Slice, reflect.Array:

				if f.Len() == 0 {
					continue
				}

				switch f.Index(0).Kind() {

				case reflect.String:
					to[fieldName] = createSS(f)

				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
					reflect.Float32, reflect.Float64,
					reflect.Bool:
					fi, err := createNS(f)
					if err != nil {
						return to, err
					}
					to[fieldName] = fi

				case reflect.Slice:
					to[fieldName] = createBS(f)

				default:
					return to, ErrConversionNotSupported

				}

			case reflect.Struct, reflect.Map:

				fi, err := createSJSON(f)
				if err != nil {
					return to, err
				}
				to[fieldName] = fi

			default:
				return to, ErrConversionNotSupported
			}
		}

	default:
		return to, ErrConversionNotSupported
	}

	return to, nil
}

func setFieldVal(attributeValueName string, fieldEl reflect.Value, toField *reflect.Value, typeOfAttrValue reflect.Type) error {

	var err error

	switch attributeValueName {

	case "S", "N":
		err = setFieldWithKind(toField.Kind(), fieldEl, toField)

	case "BOOL", "NULL":

		fromVal := fieldEl.Bool()
		switch toField.Kind() {

		case reflect.String:
			toField.SetString(strconv.FormatBool(fromVal))

		case reflect.Bool:
			toField.SetBool(fromVal)

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if fromVal {
				toField.SetInt(1)
			} else {
				toField.SetInt(0)
			}

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			if fromVal {
				toField.SetUint(1)
			} else {
				toField.SetUint(0)
			}

		case reflect.Float32, reflect.Float64:
			if fromVal {
				toField.SetFloat(1)
			} else {
				toField.SetFloat(0)
			}

		default:
			err = ErrInvalidConversion
		}

	case "B":

		fromVal := fieldEl.Bytes()
		switch toField.Kind() {

		case reflect.String:
			toField.SetString(string(fromVal))

		case reflect.Slice:
			toField.SetBytes(fromVal)

		default:
			err = ErrInvalidConversion

		}

	case "SS", "NS":

		fromLen := fieldEl.Len()
		arr := reflect.MakeSlice(toField.Type(), fromLen, fromLen)

		switch toField.Type().Elem().Kind() {

		case reflect.String:

			for i := 0; i < fromLen; i++ {
				arr.Index(i).SetString(fieldEl.Index(i).Elem().String())
			}
			toField.Set(arr)

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:

			for i := 0; i < fromLen; i++ {

				toFieldAtIndex := arr.Index(i)
				if err := setInt(fieldEl.Index(i).Elem(), &toFieldAtIndex); err != nil {
					return err
				}
			}
			toField.Set(arr)

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:

			for i := 0; i < fromLen; i++ {

				toFieldAtIndex := arr.Index(i)
				if err := setUint(fieldEl.Index(i).Elem(), &toFieldAtIndex); err != nil {
					return err
				}
			}
			toField.Set(arr)

		case reflect.Float32, reflect.Float64:

			for i := 0; i < fromLen; i++ {

				toFieldAtIndex := arr.Index(i)
				if err := setFloat(fieldEl.Index(i).Elem(), &toFieldAtIndex); err != nil {
					return err
				}
			}
			toField.Set(arr)

		default:
			return ErrInvalidConversion

		}

	case "L":

		fromLen := fieldEl.Len()
		arr := reflect.MakeSlice(toField.Type(), fromLen, fromLen)

		// iterate through the slice
		for i := 0; i < fromLen; i++ {

			_, v := extractAttributeFromValue(fieldEl.Index(i).Elem())
			toFieldAtIndex := arr.Index(i)
			if err = setFieldWithKind(v.Kind(), v, &toFieldAtIndex); err != nil {
				break
			}
		}
		toField.Set(arr)

	case "BS":

		// Only covering the case of [][]byte to [][]byte
		if toField.Type().String() != "[][]uint8" {
			return ErrConversionNotSupported
		}

		fromLen := fieldEl.Len()
		arr := reflect.MakeSlice(toField.Type(), fromLen, fromLen)
		for i := 0; i < fromLen; i++ {

			f := fieldEl.Index(i)
			if f.Kind() == reflect.Ptr {
				f = f.Elem()
			}

			fl := f.Len()
			tarr := reflect.MakeSlice(arr.Index(i).Type(), fl, fl)
			for j := 0; j < fl; j++ {

				toFieldAtIndex := tarr.Index(j)
				if err = setFieldWithKind(toFieldAtIndex.Kind(), f.Index(j), &toFieldAtIndex); err != nil {
					break
				}
			}
			arr.Index(i).Set(tarr)
		}
		toField.Set(arr)

	case "M":
		return ErrConversionNotSupported

	default:
		return ErrConversionNotSupported
	}

	return err
}
