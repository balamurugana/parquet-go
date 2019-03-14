/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package parquet

import (
	"errors"
	"reflect"
	"strings"

	"github.com/minio/parquet-go/gen-go/parquet"
	"github.com/xitongsys/parquet-go/Common"
)

/*
PathMap Example
            root(a dummy root)  (Path: "root", Children: A)
             |
             A  (Path:"root/A", Childend: B,C)
        /           \
B(Path:"root/A/B")   C(Path:"root/A/C")
*/

// PathMap records the path and its children; This is used in Marshal for improve performance.
type PathMap struct {
	Path     string
	Children map[string]*PathMap
}

func (pathMap *PathMap) Add(path []string) {
	if len(path) < 2 {
		return
	}

	c := path[1]
	if _, ok := pathMap.Children[c]; !ok {
		pathMap.Children[c] = NewPathMap(pathMap.Path + "." + c)
	}

	pathMap.Children[c].Add(path[1:])
}

func NewPathMap(path string) *PathMap {
	return &PathMap{
		Path:     path,
		Children: make(map[string]*PathMap),
	}
}

// SchemaHandler stores the schema data
type SchemaHandler struct {
	SchemaElements []*parquet.SchemaElement
	PathIndexMap   map[string]int
	IndexPathMap   map[int]string
	PathMap        *PathMap
	Infos          []*Common.Tag

	InPathToExPath map[string]string
	ExPathToInPath map[string]string

	ValueColumns []string
}

// GetRootName - Get root name from the schema handler
func (handler *SchemaHandler) GetRootName() string {
	if len(handler.SchemaElements) == 0 {
		return ""
	}

	return handler.SchemaElements[0].GetName()
}

// setValueColumns collects leaf nodes' full path in SchemaHandler.ValueColumns
func (handler *SchemaHandler) setValueColumns() {
	for i, schema := range handler.SchemaElements {
		if schema.GetNumChildren() == 0 {
			handler.ValueColumns = append(handler.ValueColumns, handler.IndexPathMap[i])
		}
	}
}

// setPathMap builds the PathMap from leaf SchemaElement
func (handler *SchemaHandler) setPathMap() {
	handler.PathMap = NewPathMap(handler.GetRootName())
	for i, schema := range handler.SchemaElements {
		if schema.GetNumChildren() == 0 {
			handler.PathMap.Add(strings.Split(handler.IndexPathMap[i], "."))
		}
	}
}

// GetRepetitionType returns the repetition type of a column by it's schema path
func (handler *SchemaHandler) GetRepetitionType(path []string) (parquet.FieldRepetitionType, error) {
	if index, ok := handler.PathIndexMap[strings.Join(path, ".")]; ok {
		return handler.SchemaElements[index].GetRepetitionType(), nil
	}

	return 0, errors.New("Name Not In Schema")
}

// MaxDefinitionLevel returns the max definition level type of a column by it's schema path
func (handler *SchemaHandler) MaxDefinitionLevel(path []string) (result int32, err error) {
	for i := 2; i <= len(path); i++ {
		rt, err := handler.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}

		if rt != parquet.FieldRepetitionType_REQUIRED {
			result++
		}
	}

	return result, nil
}

// MaxRepetitionLevel returns the max repetition level type of a column by it's schema path
func (handler *SchemaHandler) MaxRepetitionLevel(path []string) (result int32, err error) {
	for i := 2; i <= len(path); i++ {
		rt, err := handler.GetRepetitionType(path[:i])
		if err != nil {
			return 0, err
		}

		if rt == parquet.FieldRepetitionType_REPEATED {
			result++
		}
	}

	return result, nil
}

// CreateInExMap -
func (handler *SchemaHandler) CreateInExMap() {
	stack := [][2]int{} // stack[][0]: index of schemas; stack[][1]: numChildren
	for pos := 0; pos < len(handler.SchemaElements) || len(stack) > 0; {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}

			stack = append(stack, [2]int{pos, int(handler.SchemaElements[pos].GetNumChildren())})
			pos++
		} else { // leaf node
			inName := []string{}
			exName := []string{}
			for i := 0; i < len(stack); i++ {
				inName = append(inName, handler.Infos[stack[i][0]].InName)
				exName = append(exName, handler.Infos[stack[i][0]].ExName)
			}

			inPath := strings.Join(inName, ".")
			exPath := strings.Join(exName, ".")
			handler.ExPathToInPath[exPath] = inPath
			handler.InPathToExPath[inPath] = exPath
			stack = stack[:len(stack)-1]
		}
	}
}

type Item struct {
	GoType reflect.Type
	Info   *Common.Tag
}

func NewItem() *Item {
	item := new(Item)
	item.Info = Common.NewTag()
	return item
}

// NewSchemaHandlerFromSchemaList creates schema handler from schema list
func NewSchemaHandlerFromSchemaList(schemas []*parquet.SchemaElement) (handler *SchemaHandler) {
	handler = &SchemaHandler{
		SchemaElements: schemas,
		PathIndexMap:   make(map[string]int),
		IndexPathMap:   make(map[int]string),
		InPathToExPath: make(map[string]string),
		ExPathToInPath: make(map[string]string),
	}

	stack := [][2]int{} // stack[][0]: index of schemas; stack[][1]: numChildren
	for pos := 0; pos < len(handler.SchemaElements) || len(stack) > 0; {
		if len(stack) == 0 || stack[len(stack)-1][1] > 0 {
			if len(stack) > 0 {
				stack[len(stack)-1][1]--
			}

			stack = append(stack, [2]int{pos, int(handler.SchemaElements[pos].GetNumChildren())})
			pos++
		} else {
			path := []string{}
			for i := 0; i < len(stack); i++ {
				path = append(path, schemas[stack[i][0]].GetName())
			}

			topPos := stack[len(stack)-1][0]
			handler.PathIndexMap[strings.Join(path, ".")] = topPos
			handler.IndexPathMap[topPos] = strings.Join(path, ".")
			stack = stack[:len(stack)-1]
		}
	}

	handler.setPathMap()
	handler.setValueColumns()

	return handler
}

func NewSchemaElementFromTagMap(info *Common.Tag) *parquet.SchemaElement {
	schema := parquet.NewSchemaElement()
	schema.Name = info.InName
	schema.TypeLength = &info.Length
	schema.Scale = &info.Scale
	schema.Precision = &info.Precision
	schema.FieldID = &info.FieldID
	schema.RepetitionType = &info.RepetitionType
	schema.NumChildren = nil

	t, err := parquet.TypeFromString(info.Type)
	if err != nil {
		ct, _ := parquet.ConvertedTypeFromString(info.Type)
		schema.ConvertedType = &ct
		switch info.Type {
		case "INT_8", "INT_16", "INT_32", "UINT_8", "UINT_16", "UINT_32", "DATE", "TIME_MILLIS":
			schema.Type = parquet.TypePtr(parquet.Type_INT32)
		case "INT_64", "UINT_64", "TIME_MICROS", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS":
			schema.Type = parquet.TypePtr(parquet.Type_INT64)
		case "UTF8":
			schema.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		case "INTERVAL":
			schema.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
			var ln int32 = 12
			schema.TypeLength = &ln
		case "DECIMAL":
			t, _ = parquet.TypeFromString(info.BaseType)
			schema.Type = &t
		}
	}

	return schema
}

// NewSchemaHandlerFromStruct - Create schema handler from a object
func NewSchemaHandlerFromStruct(obj interface{}) (sh *SchemaHandler, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("error occurred")
			}
		}
	}()

	ot := reflect.TypeOf(obj).Elem()
	item := NewItem()
	item.GoType = ot
	item.Info.InName = "parquet_go_root"
	item.Info.ExName = "parquet_go_root"
	item.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED

	stack := make([]*Item, 1)
	stack[0] = item
	schemaElements := make([]*parquet.SchemaElement, 0)
	infos := make([]*Common.Tag, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item = stack[ln-1]
		stack = stack[:ln-1]
		var newInfo *Common.Tag

		if item.GoType.Kind() == reflect.Struct {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			schema.RepetitionType = &item.Info.RepetitionType
			numField := int32(item.GoType.NumField())
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)

			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)

			for i := int(numField - 1); i >= 0; i-- {
				f := item.GoType.Field(i)
				tagStr := f.Tag.Get("parquet")

				//ignore item without parquet tag
				if len(tagStr) <= 0 {
					numField--
					continue
				}

				newItem := NewItem()
				newItem.Info = Common.StringToTag(tagStr)
				newItem.Info.InName = f.Name
				newItem.GoType = f.Type
				if f.Type.Kind() == reflect.Ptr {
					newItem.GoType = f.Type.Elem()
					newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
				}
				stack = append(stack, newItem)
			}
		} else if item.GoType.Kind() == reflect.Slice &&
			item.Info.RepetitionType != parquet.FieldRepetitionType_REPEATED {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			rt1 := item.Info.RepetitionType
			schema.RepetitionType = &rt1
			var numField int32 = 1
			schema.NumChildren = &numField
			ct1 := parquet.ConvertedType_LIST
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "list"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			newInfo.InName, newInfo.ExName = "list", "list"
			infos = append(infos, newInfo)

			newItem := NewItem()
			newItem.Info = Common.GetValueTagMap(item.Info)
			newItem.Info.InName = "element"
			newItem.Info.ExName = "element"
			newItem.GoType = item.GoType.Elem()
			if newItem.GoType.Kind() == reflect.Ptr {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
				newItem.GoType = item.GoType.Elem().Elem()
			} else {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			}
			stack = append(stack, newItem)

		} else if item.GoType.Kind() == reflect.Slice &&
			item.Info.RepetitionType == parquet.FieldRepetitionType_REPEATED {
			newItem := NewItem()
			newItem.Info = item.Info
			newItem.GoType = item.GoType.Elem()
			stack = append(stack, newItem)

		} else if item.GoType.Kind() == reflect.Map {
			schema := parquet.NewSchemaElement()
			schema.Name = item.Info.InName
			rt1 := item.Info.RepetitionType
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			ct1 := parquet.ConvertedType_MAP
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "key_value"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			var numField2 int32 = 2
			schema.NumChildren = &numField2
			ct2 := parquet.ConvertedType_MAP_KEY_VALUE
			schema.ConvertedType = &ct2
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			newInfo.InName, newInfo.ExName = "key_value", "key_value"
			infos = append(infos, newInfo)

			newItem := NewItem()
			newItem.Info = Common.GetValueTagMap(item.Info)
			newItem.GoType = item.GoType.Elem()
			if newItem.GoType.Kind() == reflect.Ptr {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_OPTIONAL
				newItem.GoType = item.GoType.Elem().Elem()
			} else {
				newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			}
			stack = append(stack, newItem)

			newItem = NewItem()
			newItem.Info = Common.GetKeyTagMap(item.Info)
			newItem.GoType = item.GoType.Key()
			newItem.Info.RepetitionType = parquet.FieldRepetitionType_REQUIRED
			stack = append(stack, newItem)

		} else {
			schema := Common.NewSchemaElementFromTagMap(item.Info)
			schemaElements = append(schemaElements, schema)
			newInfo = Common.NewTag()
			Common.DeepCopy(item.Info, newInfo)
			infos = append(infos, newInfo)
		}
	}

	res := NewSchemaHandlerFromSchemaList(schemaElements)
	res.Infos = infos
	res.CreateInExMap()
	return res, nil
}
