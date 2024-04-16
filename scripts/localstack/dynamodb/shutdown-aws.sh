#!/bin/bash

tables=$(awslocal dynamodb list-tables --output json | jq -r '.TableNames[]')

echo "Deleting DynamoDB tables..."

for table in $tables; do
    echo "Deleting table $table..."
    awslocal dynamodb delete-table --table-name $table
    echo "Table $table deleted."
done

echo "All tables deleted."
