package marshalddb

import (
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

/*
Attempts to extract the first non-zero value within an AttributeValue.

So given &dynamodb.AttributeValue{ S: aws.String("") } will return
("S", reflect.ValueOf(dynamodb.AttributeValue.S))
*/

func extractAttribute(attr *dynamodb.AttributeValue) (string, reflect.Value) {

	return extractAttributeFromValue(reflect.ValueOf(attr).Elem())
}

func extractAttributeFromValue(attrVal reflect.Value) (string, reflect.Value) {

	var (
		fieldName    string
		zeroFieldVal = reflect.Zero(reflect.TypeOf(fieldName))
	)

	attrType := attrVal.Type()
	for i := 0; i < attrVal.NumField(); i++ {

		f := attrVal.Field(i)
		if f.Kind() == reflect.Ptr {
			f = f.Elem()
		}

		if !f.IsValid() {
			continue
		}

		fieldName = attrType.Field(i).Name
		// Find first non-zero value
		switch fieldName {

		case "B":
			if f.Bytes() != nil {
				return fieldName, f
			}

		case "BOOL", "N", "S", "NULL":
			return fieldName, f

		case "L", "M", "NS", "SS", "BS":
			if f.Len() != 0 {
				return fieldName, f
			}
		}
	}

	return "", zeroFieldVal
}
