package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/minio/parquet-go/gen-go/parquet"
	"github.com/minio/parquet-go/schema"
)

var schemaTree schema.Tree

func init() {
	schemaTree = schema.Tree(map[string]*schema.Element{
		"a": &schema.Element{
			Name:       "a",
			Repetition: parquet.FieldRepetitionType_OPTIONAL,
			SchemaTree: map[string]*schema.Element{
				"b": &schema.Element{
					Name:       "b",
					Repetition: parquet.FieldRepetitionType_OPTIONAL,
					SchemaTree: map[string]*schema.Element{
						"c": &schema.Element{
							Name:          "c",
							Repetition:    parquet.FieldRepetitionType_OPTIONAL,
							Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
							ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
						},
					},
				},
			},
		},
	})

	// Below slice with FieldRepetitionType_REPEATED
	// names []string (or) names []*string
	schemaTree = schema.Tree(map[string]*schema.Element{
		"names": &schema.Element{
			Name:          "names",
			Repetition:    parquet.FieldRepetitionType_REPEATED,
			Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
		},
	})

	schemaTree = schema.Tree(map[string]*schema.Element{
		"names": &schema.Element{
			Name:          "names",
			Repetition:    parquet.FieldRepetitionType_REQUIRED,
			ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_LIST),
			SchemaTree: map[string]*schema.Element{
				"list": &schema.Element{
					Name:          "list",
					Repetition:    parquet.FieldRepetitionType_REPEATED,
					Type:          parquet.TypePtr(parquet.Type_BYTE_ARRAY),
					ConvertedType: parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
				},
			},
		},
	})

}

func main() {
	if err := schemaTree.Validate(); err != nil {
		panic(fmt.Errorf("schema.Validate(): %v", err))
	}

	if err := schemaTree.SetMaxDefinitionLevel(); err != nil {
		panic(fmt.Errorf("schema.SetMaxDefinitionLevel(): %v", err))
	}

	if err := schemaTree.SetMaxRepetitionLevel(); err != nil {
		panic(fmt.Errorf("schema.SetMaxRepetitionLevel(): %v", err))
	}

	schemaList := schemaTree.ToParquetSchema()
	data, err := json.MarshalIndent(schemaList, "", "    ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))

	// s := "foo"
	// records := []map[string]*string{
	// 	{"a": nil},
	// 	{"a.b": nil},
	// 	{"a.b.c": nil},
	// 	{"a.b.c": &s},
	// }

	records := []map[string][]string{
		{"names.list": []string{"foo", "bar"}},
		{"names.list": []string{}},
		{"names.list": nil},
	}

	values := []interface{}{}
	DLs := []int32{}
	RLs := []int32{}
	for _, record := range records {
		valueColumns := []string{}
		for name, value := range record {
			element := schemaTree.GetElement(name)
			if element == nil {
				panic(fmt.Errorf("%v not found", name))
			}

			for _, valueColumn := range valueColumns {
				if strings.HasPrefix(valueColumn, name+".") || strings.HasPrefix(name, valueColumn+".") {
					panic(fmt.Errorf("%v: value to nested column %v already set", name, valueColumn))
				}
			}

			if value == nil && element.Repetition == parquet.FieldRepetitionType_REQUIRED {
				panic(fmt.Errorf("%v: value must not be nil for REQUIRED type", name))
			}

			if element.Type == nil && value != nil {
				panic(fmt.Errorf("%v: value must be nil for group field", name))
			}

			valueColumns = append(valueColumns, name)

			DL := int32(element.MaxDefinitionLevel)
			if value == nil && DL != 0 {
				DL--
			}

			isSlice := true
			if isSlice {
				if element.Repetition != parquet.FieldRepetitionType_REPEATED {
					panic(fmt.Errorf("%v: value must not be slice for %v", name, element.Repetition))
				}

				switch {
				case value == nil || len(value) == 0:
					values = append(values, nil)
					DLs = append(DLs, DL)
					RLs = append(RLs, 0)
				default:
					for i, v := range value {
						values = append(values, v)
						DLs = append(DLs, DL)
						RL := int32(element.MaxRepetitionLevel)
						if i == 0 {
							RL = 0
						}
						RLs = append(RLs, RL)
					}
				}
			} else {
				if element.Repetition == parquet.FieldRepetitionType_REPEATED {
					panic(fmt.Errorf("%v: value must be slice for %v", name, element.Repetition))
				}

				values = append(values, value)
				DLs = append(DLs, DL)
				RLs = append(RLs, int32(element.MaxRepetitionLevel))
			}
		}
	}

	fmt.Println("values:", values)
	fmt.Println("DLs:", DLs)
	fmt.Println("RLs:", RLs)
}
