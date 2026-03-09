# DynamoDBv2.Transactions

A high-performance .NET library for Amazon DynamoDB transactions with **compile-time source generation** — up to **82x faster** than reflection-based mapping.

<p align="center">

[![CI](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml/badge.svg)](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml)
[![Auto-Release](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/auto-release.yml/badge.svg)](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/auto-release.yml)
[![codecov](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions/graph/badge.svg?token=CYF75Y00KH)](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions)
[![NuGet](https://img.shields.io/nuget/v/DynamoDBv2.Transactions?logo=nuget&label=NuGet)](https://www.nuget.org/packages/DynamoDBv2.Transactions)
[![NuGet Downloads](https://img.shields.io/nuget/dt/DynamoDBv2.Transactions?logo=nuget&label=Downloads)](https://www.nuget.org/packages/DynamoDBv2.Transactions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

</p>

<p align="center">

![Unit Tests](https://img.shields.io/badge/Unit_Tests-259_passed-brightgreen?logo=dotnet&logoColor=white)
![Integration Tests](https://img.shields.io/badge/Integration_Tests-33_passed-brightgreen?logo=docker&logoColor=white)
![Source Generator Tests](https://img.shields.io/badge/Source_Generator-16_tests-brightgreen?logo=dotnet&logoColor=white)
![Benchmarks](https://img.shields.io/badge/Benchmarks-A%2FB_validated-blue?logo=speedtest&logoColor=white)

</p>

<p align="center">

![.NET 8](https://img.shields.io/badge/.NET-8.0-512BD4?logo=dotnet&logoColor=white)
![.NET 9](https://img.shields.io/badge/.NET-9.0-512BD4?logo=dotnet&logoColor=white)
![.NET 10](https://img.shields.io/badge/.NET-10.0-512BD4?logo=dotnet&logoColor=white)
![AWS SDK v4](https://img.shields.io/badge/AWS_SDK-v4-FF9900?logo=amazonaws&logoColor=white)
![Source Link](https://img.shields.io/badge/Source_Link-valid-green)
![Deterministic](https://img.shields.io/badge/Deterministic-valid-green)

</p>

---

## Why This Library?

DynamoDBv2.Transactions skips the implicit `DescribeTable` call that the standard AWS SDK wrapper makes, using DynamoDB attributes directly instead. Combined with the **source generator**, this delivers dramatically faster performance:

<table>
<tr>
<td width="50%">

### Source Generator vs Reflection

| Operation | Speedup |
|-----------|--------:|
| `GetTableName` | **82x faster** |
| `MapToAttribute` (15 props) | **4.1x faster** |
| `GetHashKeyAttributeName` | **2.1x faster** |
| `GetPropertyAttributedName` | **1.9x faster** |
| `GetVersion` | **1.3x faster** |

</td>
<td width="50%">

### End-to-End vs Standard SDK Wrapper

| Operation | This Library | Standard | Speedup |
|-----------|------------:|----------:|--------:|
| 1-item write | 12.0 ms | 15.8 ms | **1.3x** |
| 3-item write | 13.4 ms | 46.4 ms | **3.5x** |
| 3-item alloc | 115 KB | 251 KB | **2.2x less** |

</td>
</tr>
</table>

> All key lookups are **zero-allocation** via compile-time switch expressions. Just make your entity class `partial` — no other changes needed.

---

## Installation

```bash
dotnet add package DynamoDBv2.Transactions
```

## Quick Start

### 1. Define Your Entity

Make it `partial` and the source generator handles the rest — zero configuration:

```csharp
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

The generator auto-discovers all `partial` classes with `[DynamoDBHashKey]` and registers them at startup via `[ModuleInitializer]`. Non-partial classes fall back to cached reflection seamlessly.

### 2. Perform a Transaction

```csharp
using DynamoDBv2.Transactions;

var client = new AmazonDynamoDBClient();

await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.CreateOrUpdate(new MyEntity
    {
        Id = "user-123",
        Name = "Alice"
    });
}
```

## Features

- **AWS SDK v4**: Built for AWSSDK.DynamoDBv2 v4.x
- **Source Generator**: Compile-time DynamoDB attribute mapping — zero reflection for `partial` classes
- **Transactional Operations**: `CreateOrUpdate`, `Delete`, `Update`, `Patch`, `ConditionCheck`
- **100-Item Limit Validation**: Enforces DynamoDB's limit before sending the request
- **TransactionOptions**: `ClientRequestToken`, `ReturnConsumedCapacity`, `ReturnItemCollectionMetrics`
- **Versioning**: Automatic version increment handling for optimistic concurrency
- **Async API**: Fully asynchronous
- **Multi-targeting**: .NET 8.0, .NET 9.0, and .NET 10.0

## Usage Examples

### Deleting an Item
```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.DeleteAsync<TestTable>(userIdToDelete);
}
```

### Patching a Property
```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.PatchAsync<TestTable, DateTime?>(userId, t => t.SomeNullableDate1, updatedDate);
}
```

### Conditional Check + Update
```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.ConditionGreaterThan<TestTable, int>(userId, t => t.SomeInt, 100);
    transactor.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 200 });
}
```

### Complex Multi-Operation Transaction
```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.ConditionNotEquals<TestTable, bool>(userId, t => t.SomeBool, false);
    transactor.CreateOrUpdate(testItem);
    transactor.PatchAsync<TestTable, int>(userId, t => t.SomeInt, 200);
}
```

### Version Check Before Update
```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.ConditionVersionEquals<TestTable>(userId, t => t.Version, expectedVersion);
    transactor.CreateOrUpdate(new TestTable { UserId = userId, SomeInt = 250 });
}
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
}
```

## Detailed Benchmark Results

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

### Running Benchmarks
```bash
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release -- --filter '*MapperBenchmark*'
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release -- --filter '*Benchmark*'
```

## Running Tests

Integration tests run via Docker Compose with localstack:

```bash
docker-compose up --exit-code-from tests tests localstack
docker-compose up --exit-code-from unittests unittests
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
