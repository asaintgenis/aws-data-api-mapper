package client

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	"log"
	"reflect"
)

type Client struct {
	service   *rdsdata.Client
	secretArn *string
	dbArn     *string
	dbName    *string
}

func NewClient(dbname, dbArn, secretArn, region string) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("failed to load configuration, %v", err)
	}

	client := rdsdata.NewFromConfig(cfg)

	return &Client{client, &secretArn, &dbArn, &dbname}, nil
}

func (c *Client) SelectFirst(ctx context.Context, dest interface{}, tableName string) error {
	query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", *c.dbName, tableName)

	log.Printf("Generated query: %s", query)
	// Perform the query
	resp, err := c.service.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
		Sql:                   &query,
		Database:              c.dbName,
		IncludeResultMetadata: true,
		SecretArn:             c.secretArn,
		ResourceArn:           c.dbArn,
	})
	if err != nil {
		return err
	}

	// Reflect on the destination interface to prepare mapping
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a pointer to a struct")
	}
	destElemType := destVal.Elem().Type()

	// Map column names to struct fields
	fieldMap := make(map[string]int)
	for i := 0; i < destElemType.NumField(); i++ {
		field := destElemType.Field(i)
		pgmapTag := field.Tag.Get("pgmap")
		if pgmapTag != "" {
			fieldMap[pgmapTag] = i
		}
	}

	log.Printf("fieldMap: %#v", fieldMap)
	// Map the result to the destination struct
	if len(resp.Records) > 0 {
		record := resp.Records[0]
		destElem := destVal.Elem()
		for colIdx, column := range record {
			colName := *resp.ColumnMetadata[colIdx].Name
			fieldIdx, found := fieldMap[colName]
			if found {
				field := destElem.Field(fieldIdx)
				setValue(field, column) // Set value using a helper function
			}
		}
	}

	return nil
}

// setValue sets a value on the given reflect.Value based on the SQL column data
func setValue(field reflect.Value, column types.Field) {
	fmt.Printf("field: %v, column: %v\n", field.String(), column)
	switch col := column.(type) {
	case *types.FieldMemberStringValue:
		log.Printf("Setting value to %s", col.Value)
		field.SetString(col.Value)
	case *types.FieldMemberLongValue:
		log.Printf("Setting value to %s", col.Value)
		field.SetInt(col.Value)
	case *types.FieldMemberBooleanValue:
		log.Printf("Setting value to %s", col.Value)
		field.SetBool(col.Value)
	case *types.FieldMemberDoubleValue:
		log.Printf("Setting value to %s", col.Value)
		field.SetFloat(col.Value)
	case *types.FieldMemberIsNull:
		// Handle NULL values if necessary
	default:
		// Handle other cases as necessary
	}
}
