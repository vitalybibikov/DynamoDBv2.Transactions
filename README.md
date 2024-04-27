# DynamoDBv2.Transactions

DynamoDBv2.Transactions is a .NET library that provides a robust wrapper around the Amazon DynamoDB low-level API, enabling easy and efficient management of transactions for batch operations. This library is designed to simplify complex transactional logic and ensure data consistency across your DynamoDB operations.

![Badge](https://camo.githubusercontent.com/f824639c7971a3cecd08cef859f85e5b7181d8f0701df0a1c6e7d475f0e8f20f/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f32302f32302d5041535345442d627269676874677265656e2e737667) ![Badge](https://camo.githubusercontent.com/e6f58b5667bf820dd34d07762b5f0232f3d27d6fde052988c9e07af61ab1448e/68747470733a2f2f696d672e736869656c64732e696f2f62616467652f34312f34312d5041535345442d627269676874677265656e2e737667)

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


## BenchmarkDotNet results

// * Summary *

BenchmarkDotNet v0.13.12, Windows 11 (10.0.22631.3527/23H2/2023Update/SunValley3)
AMD Ryzen 9 6900HS with Radeon Graphics, 1 CPU, 16 logical and 8 physical cores
.NET SDK 8.0.200
  [Host]    : .NET 8.0.2 (8.0.224.6711), X64 RyuJIT AVX2
  OutOfProc : .NET 8.0.2 (8.0.224.6711), X64 RyuJIT AVX2

Job=OutOfProc  IterationCount=15  LaunchCount=3
WarmupCount=10

| Method                      | Mean     | Error    | StdDev   | Allocated |
|---------------------------- |---------:|---------:|---------:|----------:|
| DynamoDbTransactionsWrapper | 12.15 ms | 0.075 ms | 0.139 ms |  80.96 KB |
| OriginalWrapper             | 15.73 ms | 0.158 ms | 0.300 ms |  83.75 KB |

// * Hints *
Outliers
  Benchmark.DynamoDbTransactionsWrapper: OutOfProc -> 1 outlier  was  removed (13.54 ms)

// * Legends *
  Mean      : Arithmetic mean of all measurements
  Error     : Half of 99.9% confidence interval
  StdDev    : Standard deviation of all measurements
  Allocated : Allocated memory per single operation (managed only, inclusive, 1KB = 1024B)
  1 ms      : 1 Millisecond (0.001 sec)

// * Diagnostic Output - MemoryDiagnoser *


// ***** BenchmarkRunner: End *****
Run time: 00:02:02 (122.6 sec), executed benchmarks: 2

Global total time: 00:02:24 (144.74 sec), executed benchmarks: 2

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
