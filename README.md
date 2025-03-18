# DynamoDBv2.Transactions

DynamoDBv2.Transactions is a .NET library that provides a robust wrapper around the Amazon DynamoDB low-level API, enabling easy and efficient management of transactions for batch operations. This library is designed to simplify complex transactional logic and ensure data consistency across your DynamoDB operations.
It skips additional implicit for some cases DescribeTable call, thus making DynamoDB attributes mandatory - alternatively too using the attributes you can provide KeyName/KeyValue as a separate parameter to a method.

Unit Tests: ![Badge](https://camo.githubusercontent.com/63990e7e4752bde704f569b9db6cce24d94eeb19d12898d1f99579928858f55e/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f3132342f3132342d5041535345442d627269676874677265656e2e737667) 
Integration Tests via localstack: ![Badge](https://camo.githubusercontent.com/6a167e3368c36788b026d8703bcd29d2f0c46199a6cfd14e9b94c7765e036bd3/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f33332f33332d5041535345442d627269676874677265656e2e737667)

[![.github/workflows/dotnet.yml](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml/badge.svg)](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml)


[![codecov](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions/graph/badge.svg?token=CYF75Y00KH)](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions)

• Source Link: ✅ Valid with Symbol Server
• Deterministic (dll/exe): ✅ Valid
• Compiler Flags: ✅ Valid

## Features

- **Transactional Operations**: Supports `CreateOrUpdate`, `Delete`, and `Update`and `ConditionCheck` operations within transactions.
- **Error Handling**: Gracefully handles transaction failures and rollbacks.
- **Versioning Support**: Automatic handling of version increments for transactional integrity.
- **Easy Integration**: Seamlessly integrates with existing DynamoDB setups.
- **Asynchronous API**: Fully asynchronous API for optimal performance.

## Plans
- Add Source Generator in order to validate abscence of [DynamoDBHashKey("UserId")] attribute
- Write more tests on SortKey
- Write more benchmarks.

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
!!! It skips additional implicit for some cases DescribeTable call, thus making DynamoDB attribute [DynamoDBHashKey("YourId")] mandatory !!!
Which makes it faster in comparison with the traditional wrapper.
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

### Deleting an Item
```csharp
// Arrange
var userIdToDelete = "unique-user-id";

// Act
await using (var transactor = new DynamoDbTransactor(_fixture.Db.Client))
{
    transactor.DeleteAsync<TestTable>(userIdToDelete);
};

// This operation will asynchronously delete the specified item from DynamoDB.

```
### Patching an Item

```csharp
// Arrange
var userIdToPatch = "unique-user-id";
var updatedDate = DateTime.UtcNow.AddDays(1);

// Act
await using (var transactor = new DynamoDbTransactor(_fixture.Db.Client))
{
    transactor.PatchAsync<TestTable, DateTime?>(userIdToPatch, t => t.SomeNullableDate1, updatedDate);
};

// This code patches the 'SomeNullableDate1' property of the specified item to a new date.

```


### Adding a conditional check

```csharp
// Arrange
var userIdToCheck = "unique-user-id";

// Act
await using (var transactor = new DynamoDbTransactor(_fixture.Db.Client))
{
    transactor.ConditionGreaterThan<TestTable, int>(userIdToCheck, t => t.SomeInt, 100);
    transactor.CreateOrUpdate(new TestTable { UserId = userIdToCheck, SomeInt = 200 });
};

// This will add a conditional check to ensure 'SomeInt' is greater than 100 before updating or creating the item.

```

###  Complex Transaction with Multiple Operations

```csharp
// Arrange
var userId = Guid.NewGuid().ToString();
var testItem = new TestTable
{
    UserId = userId,
    SomeInt = 150,
    SomeDate = DateTime.UtcNow,
    SomeBool = true
};

// Act
await using (var transactor = new DynamoDbTransactor(_fixture.Db.Client))
{
    transactor.ConditionNotEquals<TestTable, bool>(userId, t => t.SomeBool, false);
    transactor.CreateOrUpdate(testItem);
    transactor.PatchAsync<TestTable, int>(userId, t => t.SomeInt, 200);
};

// This transaction will check if 'SomeBool' is not false, then create or update the item, and finally patch 'SomeInt' to 200.

```

### Version Check Before Update
```csharp
// Arrange
var userId = Guid.NewGuid().ToString();
var expectedVersion = 1;

// Act
await using (var transactor = new DynamoDbTransactor(_fixture.Db.Client))
{
    transactor.ConditionVersionEquals<TestTable>(userId, t => t.Version, expectedVersion);
    transactor.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 250 });
};

// This ensures the item's version matches the expected version before it is updated or created.

```


## BenchmarkDotNet results

// * Summary *

BenchmarkDotNet v0.13.12, Windows 11 (10.0.22631.3527/23H2/2023Update/SunValley3)
AMD Ryzen 9 6900HS with Radeon Graphics, 1 CPU, 16 logical and 8 physical cores
.NET SDK 8.0.200
  [Host]    : .NET 8.0.2 (8.0.224.6711), X64 RyuJIT AVX2
  OutOfProc : .NET 8.0.2 (8.0.224.6711), X64 RyuJIT AVX2

Job=OutOfProc  IterationCount=15  LaunchCount=3
WarmupCount=10

| Method                            | Mean     | Error    | StdDev   | Allocated |
|---------------------------------- |---------:|---------:|---------:|----------:|
| DynamoDbTransactionsWrapper       | 11.99 ms | 0.046 ms | 0.087 ms |  80.96 KB |
| OriginalWrapper                   | 15.83 ms | 0.236 ms | 0.442 ms |  83.77 KB |
| DynamoDbTransactionsWrapper3Items | 13.37 ms | 0.066 ms | 0.123 ms | 114.74 KB |
| OriginalWrapper3Items             | 46.44 ms | 0.444 ms | 0.834 ms | 251.01 KB |

// * Hints *
Outliers
  Benchmark.DynamoDbTransactionsWrapper3Items: OutOfProc -> 3 outliers were removed (13.64 ms..14.81 ms)
  Benchmark.OriginalWrapper3Items: OutOfProc             -> 3 outliers were removed (48.78 ms..49.19 ms)

// * Legends *
  Mean      : Arithmetic mean of all measurements
  Error     : Half of 99.9% confidence interval
  StdDev    : Standard deviation of all measurements
  Allocated : Allocated memory per single operation (managed only, inclusive, 1KB = 1024B)
  1 ms      : 1 Millisecond (0.001 sec)

// * Diagnostic Output - MemoryDiagnoser *


// ***** BenchmarkRunner: End *****
Run time: 00:04:09 (249.42 sec), executed benchmarks: 4

### To run benchmark:
1. Goto .\DynamoDBv2.Transactions
2. dotnet build .\test\DynamoDBv2.Transactions.Benchmarks\ -c Release
3. Execute in shell .\test\DynamoDBv2.Transactions.Benchmarks\bin\Release\net8.0\DynamoDBv2.Transactions.exe

## Running Tests

### To run integration tests
ensure you have a test instance of DynamoDB available.  (and configure it in env of the docker compose file)
(On my env tests are running both in real DynamoDB and localstack instance)
Tests are written using xUnit and should be configured to interact directly with your database:

1. docker-compose up --exit-code-from tests tests localstack
2. docker-compose up --exit-code-from unittests unittests

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
