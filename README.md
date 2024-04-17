# DynamoDBv2.Transactions

DynamoDBv2.Transactions is a .NET library that provides a robust wrapper around the Amazon DynamoDB low-level API, enabling easy and efficient management of transactions for batch operations. This library is designed to simplify complex transactional logic and ensure data consistency across your DynamoDB operations.

![Badge](https://camo.githubusercontent.com/7a1b0b1a230ee19f14cef6ac1970103f931c61e06e96866a0c7a45c68bfd8755/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f362f362d5041535345442d627269676874677265656e2e737667) ![Badge](https://camo.githubusercontent.com/e6f58b5667bf820dd34d07762b5f0232f3d27d6fde052988c9e07af61ab1448e/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f34312f34312d5041535345442d627269676874677265656e2e737667)

[![.github/workflows/dotnet.yml](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml/badge.svg)](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml)


## Features

- **Transactional Operations**: Supports `CreateOrUpdate`, `Delete`, and `Patch` operations within transactions.
- **Error Handling**: Gracefully handles transaction failures and rollbacks.
- **Versioning Support**: Automatic handling of version increments for transactional integrity.
- **Easy Integration**: Seamlessly integrates with existing DynamoDB setups.
- **Asynchronous API**: Fully asynchronous API for optimal performance.

## Installation

You can install the DynamoDBv2.Transactions library via NuGet Package Manager. Run the following command in your Package Manager Console:

```bash
Install-Package DynamoDBv2.Transactions
```

## Quick Start

To get started with DynamoDBv2.Transactions, you'll need to set up an instance of `TransactionalWriter` using an `ITransactionManager`, which is responsible for executing the transactions against your DynamoDB instance.

### Prerequisites

Ensure you have the AWS SDK for .NET configured in your project, with access to Amazon DynamoDB.

### Example Usage

Here's a quick example to show you how to use the `TransactionalWriter` to perform a transaction:

```csharp
using DynamoDBv2.Transactions;

// Initialize the DynamoDB client
var client = new AmazonDynamoDBClient();

// Setup transaction manager and writer
var transactionManager = new TransactionManager(client);
var writer = new TransactionalWriter(transactionManager);

var userId = Guid.NewGuid().ToString();
var testItem = new TestTable
{
    UserId = userId,
    SomeDecimal = 123.45m,
    SomeDate = DateTime.UtcNow,
    SomeInt = 123
};

// Perform transaction
await using (writer)
{
    writer.CreateOrUpdate(testItem);
}

// Load and verify
var dbContext = new DynamoDBContext(client);
var data = await dbContext.LoadAsync<TestTable>(userId);
Console.WriteLine($"Item saved with UserId: {data.UserId}");
```

## Running Tests

To run integration tests, ensure you have a test instance of DynamoDB available. Tests are written using xUnit and should be configured to interact directly with your database:

```csharp
// Example test
[Fact]
public async Task SaveDataAndRetrieve()
{
    var writer = new TransactionalWriter(new TransactionManager(_fixture.Db.Client));
    var userId = Guid.NewGuid().ToString();
    var testItem = new TestTable
    {
        UserId = userId,
        SomeInt = 123
    };

    await using (var writer = new TransactionalWriter(new TransactionManager(_fixture.Db.Client)))
    {
        writer.CreateOrUpdate(testItem);
    }

    var retrievedItem = await _fixture.Db.Context.LoadAsync<TestTable>(userId);
    Assert.NotNull(retrievedItem);
}
```

## Contributing

When creating PRs, please review the following guidelines:

- [ ] The action code does not contain sensitive information.
- [ ] At least one of the commit messages contains the appropriate `+semver:` keywords listed under [Incrementing the Version] for major and minor increments.
- [ ] The action has been recompiled.  See [Recompiling Manually] for details.
- [ ] The README.md has been updated with the latest version of the action.  See [Updating the README.md] for details.


## License

Copyright &copy; 2024, Vitali Bibikov. Code released under the [MIT license](LICENSE).

## Contact

Your Name - [bibikovvitaly@gmail.com]
