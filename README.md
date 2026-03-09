# DynamoDBv2.Transactions

DynamoDBv2.Transactions is a .NET library that provides a robust wrapper around the Amazon DynamoDB low-level API, enabling easy and efficient management of transactions for batch operations. This library is designed to simplify complex transactional logic and ensure data consistency across your DynamoDB operations.
It skips additional implicit for some cases DescribeTable call, thus making DynamoDB attributes mandatory - alternatively too using the attributes you can provide KeyName/KeyValue as a separate parameter to a method.

Unit Tests: 259 Passed

Integration Tests via localstack: 33 Passed

[![.github/workflows/dotnet.yml](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml/badge.svg)](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml)


[![codecov](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions/graph/badge.svg?token=CYF75Y00KH)](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions)

- Source Link: Valid with Symbol Server
- Deterministic (dll/exe): Valid
- Compiler Flags: Valid

## Features

- **AWS SDK v4 Support**: Built for AWSSDK.DynamoDBv2 v4.x with full compatibility.
- **Source Generator**: Compile-time DynamoDB attribute mapping — zero reflection at runtime for `partial` entity classes.
- **Transactional Operations**: Supports `CreateOrUpdate`, `Delete`, `Update`, `Patch`, and `ConditionCheck` operations within transactions.
- **100-Item Limit Validation**: Enforces DynamoDB's 100 transact-item limit before sending the request.
- **TransactionOptions**: Configure `ClientRequestToken`, `ReturnConsumedCapacity`, and `ReturnItemCollectionMetrics`.
- **Versioning Support**: Automatic handling of version increments for transactional integrity.
- **Error Handling**: Gracefully handles transaction failures and rollbacks.
- **Easy Integration**: Seamlessly integrates with existing DynamoDB setups.
- **Asynchronous API**: Fully asynchronous API for optimal performance.
- **Multi-targeting**: Supports both .NET 8.0 and .NET 9.0.

## Source Generator (Zero-Reflection Mapping)

For maximum performance, make your DynamoDB entity classes `partial`. The included source generator will automatically generate compile-time attribute mappings, eliminating all reflection overhead at runtime.

```csharp
// Just add 'partial' — the source generator does the rest
[DynamoDBTable("MyTable")]
public partial class MyEntity : ITransactional
{
    [DynamoDBHashKey("PK")]
    public string Id { get; set; }

    [DynamoDBProperty("Name")]
    public string Name { get; set; }

    [DynamoDBVersion]
    public long? Version { get; set; }
}
```

The generator automatically discovers all `partial` classes with `[DynamoDBHashKey]` properties and registers them via `[ModuleInitializer]`. No additional configuration needed.

For explicit opt-in, you can also use `[DynamoDbGenerateMapping]`:

```csharp
[DynamoDbGenerateMapping]
public partial class MyEntity { ... }
```

Non-partial classes continue to work via cached reflection (the existing behavior).

## Installation

You can install the DynamoDBv2.Transactions library via NuGet Package Manager. Run the following command in your Package Manager Console:

```bash
Install-Package DynamoDBv2.Transactions
```

## Quick Start

To get started with DynamoDBv2.Transactions, you'll need to set up an instance of `DynamoDbTransactor` using an `IAmazonDynamoDB` client.

### Prerequisites

Ensure you have the AWS SDK for .NET configured in your project, with access to Amazon DynamoDB.

### Example Usage
!!! It skips additional implicit for some cases DescribeTable call, thus making DynamoDB attribute [DynamoDBHashKey("YourId")] mandatory !!!
Which makes it faster in comparison with the traditional wrapper.
Here's a quick example to show you how to use the `DynamoDbTransactor` to perform a transaction:

```csharp
using DynamoDBv2.Transactions;

// Initialize the DynamoDB client
var client = new AmazonDynamoDBClient();

var userId = Guid.NewGuid().ToString();
var testItem = new TestTable
{
    UserId = userId,
    SomeDecimal = 123.45m,
    SomeDate = DateTime.UtcNow,
    SomeInt = 123
};

// Perform transaction
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.CreateOrUpdate(testItem);
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

### Using TransactionOptions

```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.Options = new TransactionOptions
    {
        ClientRequestToken = "idempotency-token-123",
        ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL
    };

    transactor.CreateOrUpdate(item1);
    transactor.CreateOrUpdate(item2);
};
```

## Benchmark Results

### Mapper Performance: Source-Generated vs Reflection

Isolated mapping operations — no DynamoDB I/O. Entity with 15 properties including all common types.
Reflection results use warmed-up `ConcurrentDictionary` caches (best-case reflection).

```
BenchmarkDotNet v0.15.8, Linux Ubuntu 25.10
.NET SDK 9.0.311, .NET 9.0.13, X64 RyuJIT x86-64-v3

Runtime=.NET 9.0  IterationCount=20  LaunchCount=3  WarmupCount=5
```

| Method                                    | Mean         | Allocated | vs Reflection |
|------------------------------------------ |-------------:|----------:|--------------:|
| MapToAttribute **(source-generated)**     |  4,048.56 ns |    3232 B |  **4.1x faster** |
| MapToAttribute (reflection)               | 16,412.42 ns |    4000 B |     baseline  |
| GetPropertyAttributedName **(source-gen)**|     20.74 ns |       0 B |  **1.9x faster** |
| GetPropertyAttributedName (reflection)    |     39.38 ns |       0 B |     baseline  |
| GetHashKeyAttributeName **(source-gen)**  |     14.43 ns |       0 B |  **2.1x faster** |
| GetHashKeyAttributeName (reflection)      |     30.49 ns |       0 B |     baseline  |
| GetVersion **(source-generated)**         |    144.75 ns |      56 B |  **1.3x faster** |
| GetVersion (reflection)                   |    192.52 ns |      56 B |     baseline  |
| GetTableName **(source-generated)**       |     13.87 ns |       0 B | **82x faster**  |
| GetTableName (reflection)                 |  1,135.12 ns |     144 B |     baseline  |

Key lookups are **zero-allocation** via compile-time switch expressions. `MapToAttribute` is **4x faster** by eliminating `PropertyInfo.GetValue()` reflection calls. `GetTableName` is **82x faster** because reflection must call `GetCustomAttribute<DynamoDBTableAttribute>()` on every invocation.

### End-to-End Transaction Performance

Full transactional writes against DynamoDB (includes network I/O via localstack).

```
BenchmarkDotNet v0.13.12, Windows 11
AMD Ryzen 9 6900HS, .NET 8.0.2

Job=OutOfProc  IterationCount=15  LaunchCount=3  WarmupCount=10
```

| Method                            | Mean     | Error    | StdDev   | Allocated |
|---------------------------------- |---------:|---------:|---------:|----------:|
| DynamoDbTransactionsWrapper       | 11.99 ms | 0.046 ms | 0.087 ms |  80.96 KB |
| OriginalWrapper                   | 15.83 ms | 0.236 ms | 0.442 ms |  83.77 KB |
| DynamoDbTransactionsWrapper3Items | 13.37 ms | 0.066 ms | 0.123 ms | 114.74 KB |
| OriginalWrapper3Items             | 46.44 ms | 0.444 ms | 0.834 ms | 251.01 KB |

### To run benchmarks:
```bash
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release -- --filter '*MapperBenchmark*'
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release -- --filter '*Benchmark*'
```

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
    var userId = Guid.NewGuid().ToString();
    var testItem = new TestTable
    {
        UserId = userId,
        SomeInt = 123
    };

    await using (var transactor = new DynamoDbTransactor(_fixture.Db.Client))
    {
        transactor.CreateOrUpdate(testItem);
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

Copyright &copy; 2026, Vitali Bibikov. Code released under the [MIT license](LICENSE).

## Contact

Vitali Bibikov - [bibikovvitaly@gmail.com]
