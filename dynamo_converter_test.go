package marshalddb

import (
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestExtractAttribute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		Attr       *dynamodb.AttributeValue
		ExpectName string
		ExpectKind reflect.Kind
	}{
		{
			Attr: &dynamodb.AttributeValue{
				B: []byte(`Some Bytes`),
			},
			ExpectName: "B",
			ExpectKind: reflect.Slice,
		},
		{
			Attr: &dynamodb.AttributeValue{
				BOOL: aws.Bool(true),
			},
			ExpectName: "BOOL",
			ExpectKind: reflect.Bool,
		},
		{
			Attr: &dynamodb.AttributeValue{
				BS: make([][]byte, 1),
			},
			ExpectName: "BS",
			ExpectKind: reflect.Slice,
		},
		{
			Attr: &dynamodb.AttributeValue{
				L: make([]*dynamodb.AttributeValue, 1),
			},
			ExpectName: "L",
			ExpectKind: reflect.Slice,
		},
		{
			Attr: &dynamodb.AttributeValue{
				M: map[string]*dynamodb.AttributeValue{
					"KeyKey": &dynamodb.AttributeValue{
						S: aws.String("StringString"),
					},
				},
			},
			ExpectName: "M",
			ExpectKind: reflect.Map,
		},
		{
			Attr: &dynamodb.AttributeValue{
				N: aws.String("-1"),
			},
			ExpectName: "N",
			ExpectKind: reflect.String,
		},
		{
			Attr: &dynamodb.AttributeValue{
				NS: []*string{aws.String("-1")},
			},
			ExpectName: "NS",
			ExpectKind: reflect.Slice,
		},
		{
			Attr: &dynamodb.AttributeValue{
				NULL: aws.Bool(false),
			},
			ExpectName: "NULL",
			ExpectKind: reflect.Bool,
		},
		{
			Attr: &dynamodb.AttributeValue{
				S: aws.String("StringString"),
			},
			ExpectName: "S",
			ExpectKind: reflect.String,
		},
		{
			Attr: &dynamodb.AttributeValue{
				SS: []*string{aws.String("StringString")},
			},
			ExpectName: "SS",
			ExpectKind: reflect.Slice,
		},
	}

	for _, tt := range tests {

		name, val := extractAttribute(tt.Attr)
		if name != tt.ExpectName {
			t.Errorf("Name: Expect=%s, Recieved=%s", tt.ExpectName, name)
		}
		if val.Kind() != tt.ExpectKind {
			t.Errorf("Kind: Expect=%v, Recieved=%v", tt.ExpectKind, val.Kind())
		}
	}
}

func TestConvertFromAttributesPrimitives(t *testing.T) {
	t.Parallel()

	ss := make([][]byte, 1)
	ss[0] = []byte(`ByteByte`)

	expect := &primitivesStruct{
		TString:      "StringString",
		TBool:        true,
		TInt:         -100,
		TInt8:        -101,
		TInt16:       -102,
		TInt32:       -103,
		TInt64:       -104,
		TUint:        100,
		TUint16:      101,
		TUint32:      102,
		TUint64:      103,
		TFloat32:     1.314,
		TFloat64:     1.314159,
		TStringSet:   []string{"StringString"},
		TIntSet:      []int{-1},
		TSliceString: []string{"a", "b"},
		TBS:          ss,
	}

	from := map[string]*dynamodb.AttributeValue{
		"TString": &dynamodb.AttributeValue{
			S: aws.String("StringString"),
		},
		"TBool": &dynamodb.AttributeValue{
			BOOL: aws.Bool(true),
		},
		"TInt": &dynamodb.AttributeValue{
			N: aws.String("-100"),
		},
		"TInt8": &dynamodb.AttributeValue{
			N: aws.String("-101"),
		},
		"TInt16": &dynamodb.AttributeValue{
			N: aws.String("-102"),
		},
		"TInt32": &dynamodb.AttributeValue{
			N: aws.String("-103"),
		},
		"TInt64": &dynamodb.AttributeValue{
			N: aws.String("-104"),
		},
		"TUint": &dynamodb.AttributeValue{
			N: aws.String("100"),
		},
		"TUint16": &dynamodb.AttributeValue{
			N: aws.String("101"),
		},
		"TUint32": &dynamodb.AttributeValue{
			N: aws.String("102"),
		},
		"TUint64": &dynamodb.AttributeValue{
			N: aws.String("103"),
		},
		"TFloat32": &dynamodb.AttributeValue{
			N: aws.String("1.314"),
		},
		"TFloat64": &dynamodb.AttributeValue{
			N: aws.String("1.314159"),
		},
		"TStringSet": &dynamodb.AttributeValue{
			SS: []*string{aws.String("StringString")},
		},
		"TIntSet": &dynamodb.AttributeValue{
			NS: []*string{aws.String("-1")},
		},
		"TSliceString": &dynamodb.AttributeValue{
			L: []*dynamodb.AttributeValue{
				&dynamodb.AttributeValue{
					S: aws.String("a"),
				},
				&dynamodb.AttributeValue{
					S: aws.String("b"),
				},
			},
		},
		"TBS": &dynamodb.AttributeValue{
			BS: ss,
		},
	}

	to := new(primitivesStruct)
	if err := ConvertFromAttributes(from, to); err != nil {
		t.Fatal(err)
	}

	verify(to, expect, t)
}

func TestConvertFromAttributesNestedStructs(t *testing.T) {
	t.Parallel()

	expect := &nestedStruct{
		TString: "StringString",
		TBool:   false,
		TStruct: &subNestedStruct{
			TInt:     -1234,
			TFloat32: 3.14,
		},
	}

	from := map[string]*dynamodb.AttributeValue{
		"TString": &dynamodb.AttributeValue{
			S: aws.String("StringString"),
		},
		"TBool": &dynamodb.AttributeValue{
			BOOL: aws.Bool(false),
		},
		"TStruct": &dynamodb.AttributeValue{
			S: aws.String(`{"TInt":-1234,"TFloat32":3.14}`),
		},
	}

	to := new(nestedStruct)
	if err := ConvertFromAttributes(from, to); err != nil {
		t.Fatal(err)
	}

	verify(to, expect, t)
}

func TestConvertFromAttributesOverflow(t *testing.T) {
	t.Parallel()

	from := map[string]*dynamodb.AttributeValue{
		"TInt8": &dynamodb.AttributeValue{
			N: aws.String(strconv.Itoa(math.MaxInt64)),
		},
	}

	to := new(primitivesStruct)
	if err := ConvertFromAttributes(from, to); err == nil {
		t.Fatal("Expected overflow error")
	}
}

func TestConvertToAttributesPrimitives(t *testing.T) {
	t.Parallel()

	ss := make([][]byte, 1)
	ss[0] = []byte(`ByteByte`)

	from := &primitivesStruct{
		TString:    "StringString",
		TBool:      true,
		TInt:       -100,
		TInt8:      -101,
		TInt16:     -102,
		TInt32:     -103,
		TInt64:     -104,
		TUint:      100,
		TUint16:    101,
		TUint32:    102,
		TUint64:    103,
		TFloat32:   1.314,
		TFloat64:   1.314159,
		TStringSet: []string{"StringString"},
		TIntSet:    []int{-1},
		TBS:        ss,
	}

	expect := map[string]*dynamodb.AttributeValue{
		"TString": &dynamodb.AttributeValue{
			S: aws.String("StringString"),
		},
		"TBool": &dynamodb.AttributeValue{
			BOOL: aws.Bool(true),
		},
		"TInt": &dynamodb.AttributeValue{
			N: aws.String("-100"),
		},
		"TInt8": &dynamodb.AttributeValue{
			N: aws.String("-101"),
		},
		"TInt16": &dynamodb.AttributeValue{
			N: aws.String("-102"),
		},
		"TInt32": &dynamodb.AttributeValue{
			N: aws.String("-103"),
		},
		"TInt64": &dynamodb.AttributeValue{
			N: aws.String("-104"),
		},
		"TUint": &dynamodb.AttributeValue{
			N: aws.String("100"),
		},
		"TUint16": &dynamodb.AttributeValue{
			N: aws.String("101"),
		},
		"TUint32": &dynamodb.AttributeValue{
			N: aws.String("102"),
		},
		"TUint64": &dynamodb.AttributeValue{
			N: aws.String("103"),
		},
		"TFloat32": &dynamodb.AttributeValue{
			N: aws.String("1.314"),
		},
		"TFloat64": &dynamodb.AttributeValue{
			N: aws.String("1.314159"),
		},
		"TStringSet": &dynamodb.AttributeValue{
			SS: []*string{aws.String("StringString")},
		},
		"TIntSet": &dynamodb.AttributeValue{
			NS: []*string{aws.String("-1")},
		},
		"TBS": &dynamodb.AttributeValue{
			BS: ss,
		},
	}

	have, err := ConvertToAttributes(from)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range expect {

		haveValue, ok := have[k]
		if !ok {
			t.Errorf("Missing field=%s", k)
			continue
		}

		if !reflect.DeepEqual(v, haveValue) {

			for i := 0; i < len(v.SS); i++ {

				t.Logf("Have=%s, Expect=%s", *haveValue.SS[i], *v.SS[i])
			}
			t.Errorf("Field=%s not equal", k)
		}
	}
}

func TestConvertToAttributesNestedStructs(t *testing.T) {
	t.Parallel()

	from := &nestedStruct{
		TString: "StringString",
		TBool:   false,
		TStruct: &subNestedStruct{
			TInt:     -1234,
			TFloat32: 3.14,
		},
	}

	expect := map[string]*dynamodb.AttributeValue{
		"TString": &dynamodb.AttributeValue{
			S: aws.String("StringString"),
		},
		"TBool": &dynamodb.AttributeValue{
			BOOL: aws.Bool(false),
		},
		"TStruct": &dynamodb.AttributeValue{
			S: aws.String(`{"TInt":-1234,"TFloat32":3.14}`),
		},
	}

	have, err := ConvertToAttributes(from)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range expect {

		haveValue, ok := have[k]
		if !ok {
			t.Errorf("Missing field=%s", k)
			continue
		}

		if !reflect.DeepEqual(v, haveValue) {
			t.Errorf("Field=%s not equal", k)
		}
	}
}

func TestFieldByName(t *testing.T) {
	t.Parallel()

	have := &taggedStruct{
		Tag1: "tag1",
		Tag2: "tag2",
	}
	tests := []struct {
		Tag    string
		Expect string
	}{
		{
			Tag:    "Tag1",
			Expect: "tag1",
		},
		{
			Tag:    "untag1",
			Expect: "tag1",
		},
	}

	for _, tt := range tests {

		v := fieldByName(reflect.ValueOf(have), tt.Tag)
		if v.String() != tt.Expect {
			t.Errorf("Tag=%s, Expect=%s, Have=%s", tt.Tag, tt.Expect, v.String())
		}
	}
}

func verify(to, expect interface{}, t *testing.T) {

	ev := reflect.ValueOf(expect)
	if ev.Kind() == reflect.Ptr || ev.Kind() == reflect.Interface {
		ev = ev.Elem()
	}
	et := ev.Type()

	tv := reflect.ValueOf(to)
	if tv.Kind() == reflect.Ptr || tv.Kind() == reflect.Interface {
		tv = tv.Elem()
	}

	for i := 0; i < ev.NumField(); i++ {

		ef := ev.Field(i)
		tf := tv.FieldByName(et.Field(i).Name)
		ek := ef.Kind()
		if ek == reflect.Ptr {
			ek = ef.Elem().Kind()
		}

		switch ek {

		case reflect.Struct:
			verify(tf.Interface(), ef.Interface(), t)

		case reflect.Slice:

			if ef.Len() != tf.Len() {

				t.Errorf(
					"%s: Len Expect=%d, Received=%d",
					et.Field(i).Name,
					ef.Len(),
					tf.Len(),
				)
				continue
			}
			for i := 0; i < ef.Len(); i++ {

				efi := ef.Index(i)
				tfi := tf.Index(i)

				if efi.Kind() == reflect.Slice {

					if !reflect.DeepEqual(efi.Interface(), tfi.Interface()) {
						t.Errorf(
							"%s: Expect=%v, Received=%v",
							et.Field(i).Name,
							efi.Interface(),
							tfi.Interface(),
						)
						continue
					}

				} else {

					if efi.Interface() != tfi.Interface() {

						t.Errorf(
							"%s: Expect=%v, Received=%v",
							et.Field(i).Name,
							efi.Interface(),
							tfi.Interface(),
						)
					}
				}
			}

		default:
			if ef.Interface() != tf.Interface() {

				t.Errorf(
					"%s: Expect=%v, Received=%v",
					et.Field(i).Name,
					ef.Interface(),
					tf.Interface(),
				)
			}
		}
	}
}

type primitivesStruct struct {
	TString      string
	TBool        bool
	TInt         int
	TInt8        int8
	TInt16       int16
	TInt32       int32
	TInt64       int64
	TUint        uint
	TUint16      uint16
	TUint32      uint32
	TUint64      uint64
	TFloat32     float32
	TFloat64     float64
	TSliceString []string
	TStringSet   []string
	TIntSet      []int
	TBS          [][]byte
}

type nestedStruct struct {
	TString string
	TBool   bool
	TStruct *subNestedStruct
}

type subNestedStruct struct {
	TInt     int
	TFloat32 float32
}

type taggedStruct struct {
	Tag1 string `json:"untag1"`
	Tag2 string `json:"untag2"`
}
