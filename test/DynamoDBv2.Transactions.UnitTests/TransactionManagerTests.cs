﻿using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Contract;
using DynamoDBv2.Transactions.Requests.Properties;
using DynamoDBv2.Transactions.UnitTests.Setup;
using Moq;
using Xunit;

namespace DynamoDBv2.Transactions.UnitTests
{
    public class TransactionManagerTests
    {
        [Fact]
        public async Task ExecuteTransactionAsync_MapsOperationsCorrectly()
        {
            // Arrange
            var client = new Mock<AmazonDynamoDBClient>();
            var manager = new TransactionManager(client.Object);
            var requests = new List<ITransactionRequest>
            {
                new ConditionCheckTransactionRequest<SomeDynamoDbEntity>(),
                new DeleteTransactionRequest<SomeDynamoDbEntity>(new KeyValue { Key = "Id", Value = "123" }),
                new UpdateTransactionRequest<SomeDynamoDbEntity>(),
                new PutTransactionRequest<SomeDynamoDbEntity>(new SomeDynamoDbEntity()),
                new PatchTransactionRequest<SomeDynamoDbEntity>(new KeyValue { Key = "Id", Value = "123" }, new Property() { Name = "Name", Value = "Test"})
            };

            // Act
            var response = await manager.ExecuteTransactionAsync(requests);

            // Assert
            client.Verify(c => c.TransactWriteItemsAsync(It.IsAny<TransactWriteItemsRequest>(), CancellationToken.None), Times.Once());
        }

        [Fact]
        public async Task ExecuteTransactionAsync_AllItemsPassedToEnd()
        {
            // Arrange
            var client = new Mock<AmazonDynamoDBClient>();
            var manager = new TransactionManager(client.Object);
            var requests = new List<ITransactionRequest>
            {
                new ConditionCheckTransactionRequest<SomeDynamoDbEntity>(),
                new DeleteTransactionRequest<SomeDynamoDbEntity>(new KeyValue { Key = "Id", Value = "123" }),
                new UpdateTransactionRequest<SomeDynamoDbEntity>(),
                new PutTransactionRequest<SomeDynamoDbEntity>(new SomeDynamoDbEntity()),
                new PatchTransactionRequest<SomeDynamoDbEntity>(new KeyValue { Key = "Id", Value = "123" }, new Property() { Name = "Name", Value = "Test"})
            };

            // Act
            await manager.ExecuteTransactionAsync(requests);

            // Assert
            client.Verify(x => x.TransactWriteItemsAsync(It.Is<TransactWriteItemsRequest>(req =>
                    req.TransactItems.Count == 5 &&
                    req.TransactItems[0].ConditionCheck != null &&
                    req.TransactItems[1].Delete != null &&
                    req.TransactItems[2].Update != null &&
                    req.TransactItems[3].Put != null &&
                    req.TransactItems[4].Update != null // Check for Patch, which should map to Update
            ), It.IsAny<CancellationToken>()), Times.Once());
        }
    }
}
