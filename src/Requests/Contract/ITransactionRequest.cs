﻿using Amazon.DynamoDBv2.Model;

namespace DynamoDBv2.Transactions.Requests.Contract;

/// <summary>
///  Contract for the DynamoDBv2.Transactions.Requests, that defines one of <see cref="TransactOperationType"/> operations.
/// </summary>
public interface ITransactionRequest
{
    string TableName { get; }
    Dictionary<string, AttributeValue> Key { get; }
    string? ConditionExpression { get; }
    TransactOperationType Type { get; }
    public Dictionary<string, string> ExpressionAttributeNames { get; set; }
    public Dictionary<string, AttributeValue> ExpressionAttributeValues { get; set; }
}