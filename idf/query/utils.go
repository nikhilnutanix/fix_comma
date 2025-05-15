package query

// All utility functions required for data-sync service
// Corresponds to IdfQueryUtils, IDFRPCUtils

// Use functions, types, or variables from the imported package

import (
	"reflect"
	"strings"

	"fix_comma/rpc/categoriesproto"
	idf "fix_comma/types/idf"
	"fix_comma/util"
	"github.com/golang/glog"
	"github.com/nutanix-core/go-cache/insights/insights_interface"
	"google.golang.org/protobuf/proto"
)

// CheckValueTypeAndSet checks the type of value and sets the value to the builder
//
// Parameters:
//   - filterValue: interface{}
//
// Returns:
//   - insights_interface.DataValue
func CheckValueTypeAndSet(filterValue interface{}) *insights_interface.DataValue {
	builder := &insights_interface.DataValue{}
	// check the type of value
	// get simple name of the type
	var filterValueType = reflect.TypeOf(filterValue).String()
	switch filterValueType {
	case "int64":
		builder.ValueType = &insights_interface.DataValue_Int64Value{
			Int64Value: filterValue.(int64),
		}
		return builder
	case "bool":
		builder.ValueType = &insights_interface.DataValue_BoolValue{
			BoolValue: filterValue.(bool),
		}
		return builder
	case "uint64":
		builder.ValueType = &insights_interface.DataValue_Uint64Value{
			Uint64Value: filterValue.(uint64),
		}
		return builder
	case "string":
		value := filterValue.(string)
		builder.ValueType = &insights_interface.DataValue_StrValue{
			StrValue: value,
		}
		// singleton list
	case "[]string":
		arrayList := filterValue.([]string)
		// if arraylist is non-empty
		if len(arrayList) > 0 {
			builder.ValueType = &insights_interface.DataValue_StrList_{
				StrList: &insights_interface.DataValue_StrList{
					ValueList: arrayList,
				},
			}
		}
	case "[]int64":
		arrayList := filterValue.([]int64)
		// if arraylist is non-empty
		if len(arrayList) > 0 {
			builder.ValueType = &insights_interface.DataValue_Int64List_{
				Int64List: &insights_interface.DataValue_Int64List{
					ValueList: arrayList,
				},
			}
		}
	case "[]byte", "[]uint8":
		builder.ValueType = &insights_interface.DataValue_BytesValue{
			BytesValue: filterValue.([]byte),
		}

	default:
		glog.Warning("Unsupported type for filter value")
	}
	return builder
}

// GetBuilderForValue returns the attribute data arg for the given value and attribute
//
// Parameters:
//   - value: insights_interface.DataValue
//   - attr: string
//
// Returns:
//   - insights_interface.AttributeDataArg
func GetBuilderForValue(value *insights_interface.DataValue, attr string) insights_interface.AttributeDataArg {
	builder := insights_interface.AttributeDataArg{
		AttributeData: &insights_interface.AttributeData{
			Name:  &attr,
			Value: value,
		},
	}
	return builder
}

// GetEntityGuid returns the entity guid for the given entity type and entity id
//
// Parameters:
//   - entityType: string
//   - entityID: string
//
// Returns:
//   - insights_interface.EntityGuid
func GetEntityGUID(entityType string, entityID string) *insights_interface.EntityGuid {
	return &insights_interface.EntityGuid{
		EntityTypeName: &entityType,
		EntityId:       &entityID,
	}
}

// GetAttrToDataMap returns the map of attribute to data value for the given entity
//
// Parameters:
//   - current: insights_interface.Entity
//
// Returns:
//   - map[string]insights_interface.DataValue
func GetAttrToDataMap(current *insights_interface.Entity) map[string]insights_interface.DataValue {
	attrToDataMap := make(map[string]insights_interface.DataValue)
	for _, attrData := range current.GetAttributeDataMap() {
		attrToDataMap[attrData.GetName()] = *attrData.GetValue()
	}
	return attrToDataMap
}

// GetStringFieldValue returns the string field value for the given field name and fields
//
// Parameters:
//   - fieldName: string
//   - fields: []insights_interface.NameTimeValuePair
//
// Returns:
//   - string
func GetStringFieldValue(fieldName string, fields []insights_interface.NameTimeValuePair) string {
	for _, pair := range fields {
		if pair.GetName() == fieldName {
			return pair.GetValue().GetStrValue()
		}
	}
	glog.V(2).Infof("Field %v not found in fields", fieldName)
	return ""
}

// EntitiesWithMetricsRetToEntityList returns the entity list for the given entities
//
// Parameters:
//   - GetEntitiesWithMetricsRet: insights_interface.GetEntitiesWithMetricsRet
//
// Returns:
//   - []insights_interface.Entity
func EntitiesWithMetricsRetToEntityList(ret *insights_interface.GetEntitiesWithMetricsRet) []*insights_interface.Entity {
	entityList := make([]*insights_interface.Entity, 0)
	if len(ret.GetGroupResultsList()) != 0 {
		queryGroupResultList := ret.GetGroupResultsList()[0]
		rawResultsList := queryGroupResultList.GetRawResults()
		for _, entityWithMetric := range rawResultsList {
			entity := &insights_interface.Entity{}
			entity.EntityGuid = entityWithMetric.GetEntityGuid()
			metricDataList := entityWithMetric.GetMetricDataList()
			for _, metricData := range metricDataList {
				if len(metricData.GetValueList()) != 0 {
					ntvpBuilder := &insights_interface.NameTimeValuePair{}
					name := metricData.GetName()
					ntvpBuilder.Name = &name
					value := metricData.GetValueList()[0].GetValue()
					ntvpBuilder.Value = value
					entity.AttributeDataMap = append(entity.AttributeDataMap, ntvpBuilder)
				}
			}
			entityList = append(entityList, entity)
		}
	} else {
		glog.V(2).Infof("GetGroupResultsList is empty")
	}
	return entityList
}

func MakeQueryMetrics(entityType string, attrs []string) *insights_interface.GetEntitiesWithMetricsArg {
	return &insights_interface.GetEntitiesWithMetricsArg{Query: &insights_interface.Query{
		EntityList: []*insights_interface.EntityGuid{{EntityTypeName: &entityType}},
		GroupBy:    &insights_interface.QueryGroupBy{RawColumns: MakeRawColumns(attrs)},
	}}
}

func MakeQueryMetricsEntityIDSimple(entityType string, entityID string) *insights_interface.GetEntitiesArg {
	metaData := true
	return &insights_interface.GetEntitiesArg{
		EntityGuidList: []*insights_interface.EntityGuid{GetEntityGUID(entityType, entityID)},
		MetaDataOnly:   &metaData,
	}
}

func MakeRawColumns(attrs []string) []*insights_interface.QueryRawColumn {
	rawColumns := make([]*insights_interface.QueryRawColumn, 0)
	for _, attr := range attrs {
		attrCopy := attr
		queryRawColumn := &insights_interface.QueryRawColumn{Column: &attrCopy}
		rawColumns = append(rawColumns, queryRawColumn)
	}
	return rawColumns
}

func MakeAttributeDataArgList(dataMap map[string]interface{}) []*insights_interface.AttributeDataArg {
	argsList := make([]*insights_interface.AttributeDataArg, 0)

	for attr, val := range dataMap {
		if val == nil || val == "" {
			continue
		}
		attrCopy := attr
		valueType := CheckValueTypeAndSet(val)
		attrDataArg := &insights_interface.AttributeDataArg{AttributeData: &insights_interface.AttributeData{
			Name:  &attrCopy,
			Value: valueType,
		}}
		argsList = append(argsList, attrDataArg)
	}
	return argsList
}

func MakeBatchUpdateQuery(args []*insights_interface.UpdateEntityArg) *insights_interface.BatchUpdateEntitiesArg {
	batchArg := &insights_interface.BatchUpdateEntitiesArg{}
	batchArg.EntityList = make([]*insights_interface.UpdateEntityArg, 0, len(args))

	batchArg.EntityList = append(batchArg.EntityList, args...)
	return batchArg
}

func MakeUpdateArg(id string, entityName string, attrs []*insights_interface.AttributeDataArg) *insights_interface.UpdateEntityArg {
	return &insights_interface.UpdateEntityArg{
		EntityGuid:           GetEntityGUID(entityName, id),
		AttributeDataArgList: attrs,
	}
}

func MakeUpdateArgWithCasValue(id string, entityName string, casValue *uint64, attrs []*insights_interface.AttributeDataArg) *insights_interface.UpdateEntityArg {
	return &insights_interface.UpdateEntityArg{
		EntityGuid:           GetEntityGUID(entityName, id),
		AttributeDataArgList: attrs,
		CasValue:             casValue,
	}
}

// MakeCategoryKeyUpdateArg creates an UpdateEntityArg for a category key to persist in the abac_category_key table
func MakeCategoryKeyUpdateArg(casValue *uint64, values map[string]interface{}) *insights_interface.UpdateEntityArg {
	attrs := make(map[string]interface{})
	immutable, _ := values[util.ImmutableAttribute].(bool)
	internal, _ := values[util.InternalAttribute].(bool)
	keyProto := categoriesproto.AbacCategoryKey{
		Uuid:        values[util.UUIDAttribute].(string),
		Name:        values[util.NameAttribute].(string),
		Description: values[util.DescriptionAttribute].(string),
		Immutable:   immutable,
		Internal:    internal,
		OwnerUuid:   values[util.OwnerUUIDAttribute].(string),
	}
	if n, ok := values[util.CARDINALITY]; ok {
		keyProto.Cardinality = n.(uint64)
	}
	pb, _ := proto.Marshal(&keyProto)
	attrs[util.ZPROTO] = pb
	return MakeUpdateArgWithCasValue(values[util.UUIDAttribute].(string), util.AbacCategoryKeyType, casValue, MakeAttributeDataArgList(attrs))
}

// MakeCategoryValueUpdateArg creates an UpdateEntityArg for a category value to persist in the abac_category table
func MakeCategoryValueUpdateArg(casValue *uint64, values map[string]interface{}) *insights_interface.UpdateEntityArg {
	attrs := make(map[string]interface{})
	immutable, _ := values[util.ImmutableAttribute].(bool)
	internal, _ := values[util.InternalAttribute].(bool)
	attrs[util.UUIDAttribute] = values[util.UUIDAttribute].(string)
	attrs[util.DescriptionAttribute] = values[util.DescriptionAttribute].(string)
	attrs[util.OwnerUUIDAttribute] = values[util.OwnerUUIDAttribute].(string)
	attrs[util.ImmutableAttribute] = values[util.ImmutableAttribute].(bool)
	attrs[util.InternalAttribute] = values[util.InternalAttribute].(bool)
	attrs[util.ValueAttribute] = values[util.ValueAttribute].(string)
	attrs[util.AbacCategoryKeyType] = values[util.AbacCategoryKeyType].(string)
	valueProto := categoriesproto.AbacCategory{
		Uuid:            values[util.UUIDAttribute].(string),
		AbacCategoryKey: values[util.AbacCategoryKeyType].(string),
		Value:           values[util.ValueAttribute].(string),
		Immutable:       immutable,
		Description:     values[util.DescriptionAttribute].(string),
		Internal:        internal,
		OwnerUuid:       values[util.OwnerUUIDAttribute].(string),
	}
	pb, _ := proto.Marshal(&valueProto)
	attrs[util.ZPROTO] = pb
	return MakeUpdateArgWithCasValue(values[util.UUIDAttribute].(string), util.AbacCategoryType, casValue, MakeAttributeDataArgList(attrs))
}

// GetCasValue retrieves the CAS value for an entity from the database
// If entityID is not present, then it's cas value is 0. If present, then some non-zero value will be returned
// increment cas value on the caller's side, when returned bool value is false. False value represents that such an entry exits in db. Hence, increment it's cas value.
func GetCasValue(entityID string, tableName string, db idf.IIdfExecutor) (*uint64, bool) {
	arg := MakeQueryMetricsEntityIDSimple(tableName, entityID)
	result, err := db.GetEntities(arg)
	if err != nil {
		glog.Errorf("error while fetching entity: %v with entityID: %s from table: %s", err, entityID, tableName)
		return nil, false
	}
	if len(result.Entity) == 0 {
		zeroValue := uint64(0)
		return &zeroValue, true
	}
	casValue := result.Entity[0].GetCasValue()
	return &casValue, false
}

func FixComma(db idf.IIdfExecutor) error {
	tables := []string{util.AbacCategoryKeyType, util.AbacCategoryType, util.CategoryIdfEntity}
	columns := [][]string{
		{util.UUIDAttribute, util.DescriptionAttribute, util.NameAttribute, util.ImmutableAttribute, util.InternalAttribute, util.OwnerUUIDAttribute},
		{util.UUIDAttribute, util.AbacCategoryKeyAttribute, util.DescriptionAttribute, util.ImmutableAttribute, util.InternalAttribute, util.OwnerUUIDAttribute, util.UserSpecifiedNameAttribute, util.ValueAttribute},
		{util.ExtIDAttribute, util.ParentExtIDAttribute, util.DescriptionAttribute, util.KeyAttribute, util.NameAttribute, util.FqNameAttribute, util.UserSpecifiedNameAttribute, util.KeyAttribute, util.CategoryTypeAttribute, util.ImmutableAttribute, util.InternalAttribute, util.OwnerUUIDAttribute},
	}
	for idx, table := range tables {
		glog.Infof("Starting to process table: %s", table)
		glog.Infof("Fetching all entries from table: %s", table)
		// Fetch entries in batches using offset and length
		offset := int64(0)
		length := int64(500) // Batch size
		kAscendingAttr := insights_interface.QueryOrderBy_kAscending
		for {
			arg := &insights_interface.GetEntitiesWithMetricsArg{
				Query: &insights_interface.Query{
					EntityList: []*insights_interface.EntityGuid{
						{
							EntityTypeName: proto.String(table),
						},
					},
					GroupBy: &insights_interface.QueryGroupBy{
						RawColumns: MakeRawColumns(columns[idx]),
						RawLimit: &insights_interface.QueryLimit{
							Limit:  &length,
							Offset: &offset,
						},
						RawSortOrder: &insights_interface.QueryOrderBy{
							SortColumn: &columns[idx][0],
							SortOrder:  &kAscendingAttr,
						},
					},
				},
			}
			glog.Infof("Fetching entries with offset: %d and length: %d", offset, length)
			ret, err := db.GetEntitiesWithMetrics(arg)
			if err != nil {
				glog.Errorf("Error fetching entries from table %s: %v", table, err)
				return err
			}
			var fetchedCount int
			if ret != nil && len(ret.GetGroupResultsList()) > 0 && ret.GetGroupResultsList()[0].GetRawResults() != nil {
				fetchedCount = len(ret.GetGroupResultsList()[0].GetRawResults())
			} else {
				glog.Warningf("No results found or invalid response for table: %s", table)
				fetchedCount = 0
			}
			glog.Infof("Fetched %d entries from table: %s with offset: %d", fetchedCount, table, offset)
			if fetchedCount == 0 {
				glog.Infof("No more entries to process in table: %s", table)
				break
			}

			glog.Infof("Processing entries from table: %s", table)
			// Process each entry
			updates := make([]*insights_interface.UpdateEntityArg, 0)
			for _, entity := range ret.GetGroupResultsList()[0].GetRawResults() {
				updated := false
				attrs := make(map[string]*insights_interface.DataValue)
				attrs[util.DescriptionAttribute] = &insights_interface.DataValue{
					ValueType: &insights_interface.DataValue_StrValue{StrValue: ""},
				}
				attrs[util.OwnerUUIDAttribute] = &insights_interface.DataValue{
					ValueType: &insights_interface.DataValue_StrValue{StrValue: ""},
				}
				for _, attrData := range entity.GetMetricDataList() {
					if len(attrData.GetValueList()) == 0 {
						continue
					}
					attrs[attrData.GetName()] = attrData.GetValueList()[0].GetValue()
				}
				for key, value := range attrs {
					if value == nil {
						continue
					}
					switch value.GetValueType().(type) {
					case *insights_interface.DataValue_StrValue:
						if strings.Contains(value.GetStrValue(), ",") && key != util.DescriptionAttribute {
							glog.Infof("Found comma in string value for attribute: %s of entity %s in table: %s. Replacing with '-'", key, entity.GetEntityGuid().GetEntityId(), table)
							newValue := strings.ReplaceAll(value.GetStrValue(), ",", "-")
							attrs[key] = &insights_interface.DataValue{
								ValueType: &insights_interface.DataValue_StrValue{StrValue: newValue},
							}
							updated = true
						}
					case *insights_interface.DataValue_StrList_:
						glog.Infof("Converting string list to single string for key: %s in table: %s", key, table)
						var joinedValue string
						if key == util.DescriptionAttribute {
							joinedValue = strings.Join(value.GetStrList().GetValueList(), ",")
						} else {
							joinedValue = strings.Join(value.GetStrList().GetValueList(), "-")
							if strings.Contains(joinedValue, ",") {
								glog.Infof("Found comma in string list value for attribute: %s of entity %s in table: %s. Replacing with '-'", key, entity.GetEntityGuid().GetEntityId(), table)
								joinedValue = strings.ReplaceAll(joinedValue, ",", "-")
							}
						}
						attrs[key] = &insights_interface.DataValue{
							ValueType: &insights_interface.DataValue_StrValue{StrValue: joinedValue},
						}
						updated = true
					default:
						glog.V(1).Infof("No action required for attribute: %s of entity %s in table: %s", key, entity.GetEntityGuid().GetEntityId(), table)
						attrs[key] = value
					}
				}

				if updated {
					glog.Infof("Attributes updated for entity ID: %s in table: %s. Preparing update argument.", entity.GetEntityGuid().GetEntityId(), table)
					convertedAttrs := make(map[string]interface{})
					for key, value := range attrs {
						if value == nil {
							continue
						}
						switch value.GetValueType().(type) {
						case *insights_interface.DataValue_StrValue:
							convertedAttrs[key] = value.GetStrValue()
						case *insights_interface.DataValue_Int64Value:
							convertedAttrs[key] = value.GetInt64Value()
						case *insights_interface.DataValue_BoolValue:
							convertedAttrs[key] = value.GetBoolValue()
						}
					}
					updateArg := &insights_interface.UpdateEntityArg{}
					if table == util.AbacCategoryKeyType {
						casValue, _ := GetCasValue(entity.GetEntityGuid().GetEntityId(), table, db)
						(*casValue)++
						updateArg = MakeCategoryKeyUpdateArg(casValue, convertedAttrs)
					} else if table == util.AbacCategoryType {
						casValue, _ := GetCasValue(entity.GetEntityGuid().GetEntityId(), table, db)
						(*casValue)++
						updateArg = MakeCategoryValueUpdateArg(casValue, convertedAttrs)
					} else {
						updateArg = MakeUpdateArg(entity.GetEntityGuid().GetEntityId(), table, MakeAttributeDataArgList(convertedAttrs))
					}
					glog.V(1).Infof("updateArg: %v", updateArg)
					updates = append(updates, updateArg)
				}
			}

			// Batch update the modified entries
			if len(updates) > 0 {
				glog.Infof("Batch updating %d entries in table: %s", len(updates), table)
				_, err := db.BatchUpdateEntities(MakeBatchUpdateQuery(updates))
				if err != nil {
					glog.Errorf("Error updating entries in table %s: %v", table, err)
					return err
				}
				glog.Infof("Successfully updated %d entries in table: %s", len(updates), table)
			} else {
				glog.Infof("No updates required for the entries in table: %s", table)
			}

			// Increment offset for the next batch
			offset += length
		}

		glog.Infof("Finished processing table: %s", table)
	}
	return nil
}
