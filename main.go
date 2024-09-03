package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TablePOC struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

type User struct {
	Email     string `json:"email" dynamodbav:"email"`
	Name      string `json:"name" dynamodbav:"name"`
	CreatedAt string `json:"created_at" dynamodbav:"created_at"`
	UpdatedAt string `json:"updated_at" dynamodbav:"updated_at"`
	IsDeleted bool   `json:"is_deleted" dynamodbav:"is_deleted"`
	Role      string `json:"role" dynamodbav:"role"`
	Timezone  string `json:"timezone" dynamodbav:"timezone"`
}

func main() {
	op := flag.String("op", "create", "Operation to perform")
	userNum := flag.Int("user-num", 1, "User number")
	flag.Parse()

	fmt.Println("flags", *op, *userNum)

	cfg, cfgErr := config.LoadDefaultConfig(context.TODO())
	if cfgErr != nil {
		panic(cfgErr)
	}
	cfg.Region = "us-west-2"

	tbl := TablePOC{
		DynamoDbClient: dynamodb.NewFromConfig(cfg),
		TableName:      "POC-SecurlyUsers",
	}

	// ###########

	// user := User{
	// 	Email:     "user2@securlyqa1.com",
	// 	Name:      "User2",
	// 	CreatedAt: "2024-10-01T00:00:00Z",
	// 	UpdatedAt: "2024-10-01T00:00:00Z",
	// 	IsDeleted: false,
	// 	Role:      "User",
	// 	Timezone:  "Asia/Kolkata",
	// }

	// userItem, userItemErr := attributevalue.MarshalMap(user)
	// if userItemErr != nil {
	// 	panic(userItemErr)
	// }
	// _, putErr := tbl.DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
	// 	TableName: &tbl.TableName,
	// 	Item:      userItem,
	// })
	// if putErr != nil {
	// 	panic(putErr)
	// }

	// ###########

	// user := User{}

	// emailItem, emailItemErr := attributevalue.Marshal("user1@securlyqa1.com")
	// if emailItemErr != nil {
	// 	panic(emailItemErr)
	// }
	// nameItem, nameItemErr := attributevalue.Marshal("User1")
	// if nameItemErr != nil {
	// 	panic(nameItemErr)
	// }
	// resp, respErr := tbl.DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
	// 	TableName: &tbl.TableName,
	// 	Key: map[string]types.AttributeValue{
	// 		"email": emailItem,
	// 		"name":  nameItem,
	// 	},
	// 	//ProjectionExpression: aws.String("email, created_at, updated_at, is_deleted, role, timezone"),
	// 	ConsistentRead: aws.Bool(true),
	// })
	// if respErr != nil {
	// 	panic(respErr)
	// }
	// unmarshalErr := attributevalue.UnmarshalMap(resp.Item, &user)
	// if unmarshalErr != nil {
	// 	panic(unmarshalErr)
	// }
	// fmt.Println(user)

	// ###########

	if *op == "create" {
		rand.Seed(time.Now().UnixNano())
		user := User{
			Email:     fmt.Sprintf("user%v@securlyqa1.com", *userNum),
			Name:      fmt.Sprintf("User%v", *userNum),
			CreatedAt: fmt.Sprintf("2024-%v-%vT00:00:00Z", rand.Intn(12)+1, rand.Intn(28)+1),
			UpdatedAt: fmt.Sprintf("2024-%v-%vT00:00:00Z", rand.Intn(12)+1, rand.Intn(28)+1),
			IsDeleted: false,
			Role:      "User",
			Timezone:  "America/Los_Angeles",
		}

		params, err := attributevalue.MarshalMap(user)
		if err != nil {
			panic(err)
		}
		paramsList := []types.AttributeValue{}
		itemsStrList := []string{}
		for k, v := range params {
			paramsList = append(paramsList, v)
			itemsStrList = append(itemsStrList, fmt.Sprintf("'%v': ?", k))
		}
		_, err = tbl.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
			Statement: aws.String(
				fmt.Sprintf("INSERT INTO \"%v\" VALUE {%v}", tbl.TableName, strings.Join(itemsStrList, ", ")),
			),
			Parameters: paramsList,
		})
		if err != nil {
			panic(err)
		}
	}

	if *op == "update" {
		rand.Seed(time.Now().UnixNano())
		userEmail := fmt.Sprintf("user%v@securlyqa1.com", *userNum)
		user := User{
			CreatedAt: fmt.Sprintf("2024-%v-%vT00:00:00Z", rand.Intn(12)+1, rand.Intn(28)+1),
			UpdatedAt: fmt.Sprintf("2024-%v-%vT00:00:00Z", rand.Intn(12)+1, rand.Intn(28)+1),
		}

		params, err := attributevalue.MarshalMap(user)
		if err != nil {
			panic(err)
		}
		paramsList := []types.AttributeValue{}
		itemsStrList := []string{}
		for k, v := range params {
			paramsList = append(paramsList, v)
			itemsStrList = append(itemsStrList, fmt.Sprintf("'%v'=?", k))
		}
		emailItem, err := attributevalue.Marshal(userEmail)
		if err != nil {
			panic(err)
		}
		paramsList = append(paramsList, emailItem)
		_, err = tbl.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
			Statement: aws.String(
				fmt.Sprintf("UPDATE \"%v\" SET %v WHERE 'email'=?", tbl.TableName, strings.Join(itemsStrList, ", ")),
			),
			Parameters: paramsList,
		})
		if err != nil {
			panic(err)
		}
	}

	// ###########

	if *op == "read" {
		user := User{}
		params, err := attributevalue.MarshalList([]interface{}{
			fmt.Sprintf("user%v@securlyqa1.com", *userNum),
		})
		if err != nil {
			panic(err)
		}
		response, err := tbl.DynamoDbClient.ExecuteStatement(context.TODO(), &dynamodb.ExecuteStatementInput{
			Statement: aws.String(
				fmt.Sprintf("SELECT * FROM \"%v\" WHERE email = ?", tbl.TableName),
			),
			Parameters: params,
		})
		if err != nil {
			panic(err)

		}
		err = attributevalue.UnmarshalMap(response.Items[0], &user)
		if err != nil {
			panic(err)
		}
		fmt.Println(user)
	}
}
