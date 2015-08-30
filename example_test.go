package marshalddb_test

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/jessejlt/marshalddb"
)

func ExampleConvertFromAttributes() {

	var (
		to   = new(address)
		from = map[string]*dynamodb.AttributeValue{
			"ID":        &dynamodb.AttributeValue{S: aws.String("abc")},
			"Attention": &dynamodb.AttributeValue{S: aws.String("JANE L MILLER")},
			"Company":   &dynamodb.AttributeValue{S: aws.String("MILLER ASSOCIATES")},
			"Delivery":  &dynamodb.AttributeValue{S: aws.String("1960 W CHELSEA AVE STE 2006")},
			"City":      &dynamodb.AttributeValue{S: aws.String("ALLENTOWN")},
			"State":     &dynamodb.AttributeValue{S: aws.String("PA")},
			"Zip":       &dynamodb.AttributeValue{N: aws.String("18104")},
			"Validated": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		}
	)
	err := marshalddb.ConvertFromAttributes(from, to)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(to.ID)
		// Output: abc
	}
}

func ExampleConvertToAttributes() {

	from := &address{
		ID:        "abc",
		To:        "JANE L MILLER",
		Company:   "MILLER ASSOCIATES",
		Delivery:  "1960 W CHELSEA AVE STE 2006",
		City:      "ALLENTOWN",
		State:     "PA",
		Zip:       18104,
		Validated: true,
	}

	to, err := marshalddb.ConvertToAttributes(from)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(*to["id"].S)
		// Output: abc
	}
}

type address struct {
	ID        string `json:"id"`
	To        string `json:"attention"`
	Company   string `json:"company"`
	Delivery  string `json:"delivery"`
	City      string `json:"city"`
	State     string `json:"state"`
	Zip       int    `json:"zip"`
	Validated bool   `json:"validated"`
}
