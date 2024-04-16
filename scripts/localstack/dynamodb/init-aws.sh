#!/bin/bash

# -- > Create DynamoDb Table
echo Creating  DynamoDb \'TestTable\' table ...
echo $(awslocal dynamodb create-table --cli-input-json '{"TableName":"TestTable", "KeySchema":[{"AttributeName":"UserId","KeyType":"HASH"}], "AttributeDefinitions":[{"AttributeName":"UserId","AttributeType":"S"}],"BillingMode":"PAY_PER_REQUEST"}')

# --> List DynamoDb Tables
echo Listing tables ...
echo $(awslocal dynamodb list-tables)