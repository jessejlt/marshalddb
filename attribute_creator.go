package marshalddb

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func createSS(from reflect.Value) *dynamodb.AttributeValue {

	flen := from.Len()
	dst := make([]*string, flen)
	for i := 0; i < flen; i++ {
		dst[i] = aws.String(from.Index(i).String())
	}

	return &dynamodb.AttributeValue{
		SS: dst,
	}
}

func createNS(from reflect.Value) (*dynamodb.AttributeValue, error) {

	flen := from.Len()
	dst := make([]*string, flen)

	var fk reflect.Kind
	if flen != 0 {
		fk = from.Index(0).Kind()
	}

	for i := 0; i < flen; i++ {

		e := from.Index(i)
		if e.Kind() == reflect.Ptr {
			e = e.Elem()
		}

		switch fk {

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dst[i] = aws.String(strconv.FormatInt(e.Int(), 10))

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			dst[i] = aws.String(strconv.FormatUint(e.Uint(), 10))

		case reflect.Float32, reflect.Float64:
			ff := e.Float()
			if math.IsInf(ff, 0) || math.IsNaN(ff) {
				return nil, ErrInvalidFloat
			}
			dst[i] = aws.String(strconv.FormatFloat(ff, 'g', -1, e.Type().Bits()))

		case reflect.Bool:
			if e.Bool() {
				dst[i] = aws.String("1")
			} else {
				dst[i] = aws.String("0")
			}
		}
	}

	return &dynamodb.AttributeValue{
		NS: dst,
	}, nil
}

func createBS(from reflect.Value) *dynamodb.AttributeValue {

	flen := from.Len()
	dst := make([][]byte, flen)

	var fk reflect.Kind
	if flen != 0 && from.Index(0).Len() != 0 {
		fk = from.Index(0).Index(0).Kind()
	}

	for i := 0; i < flen; i++ {

		e := from.Index(i)
		if e.Kind() == reflect.Ptr {
			e = e.Elem()
		}

		elen := e.Len()
		dst[i] = make([]byte, elen)
		for j := 0; j < elen; j++ {

			switch fk {

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				dst[i][j] = uint8(e.Index(j).Int())

			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				dst[i][j] = uint8(e.Index(j).Uint())

			case reflect.Float32, reflect.Float64:
				dst[i][j] = uint8(e.Index(j).Float())

			case reflect.Bool:
				if e.Index(j).Bool() {
					dst[i][j] = 1
				} else {
					dst[i][j] = 0
				}
			}
		}

	}

	return &dynamodb.AttributeValue{
		BS: dst,
	}
}

func createSJSON(from reflect.Value) (*dynamodb.AttributeValue, error) {

	j, err := json.Marshal(from.Interface())
	if err != nil {
		return nil, ErrInvalidJSON
	}

	return &dynamodb.AttributeValue{
		S: aws.String(string(j)),
	}, nil
}

func fieldByName(v reflect.Value, name string) reflect.Value {

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return reflect.Value{}
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {

		if t.Field(i).Name == name || t.Field(i).Tag.Get("json") == name {
			return v.Field(i)
		}
	}

	return reflect.Value{}
}
