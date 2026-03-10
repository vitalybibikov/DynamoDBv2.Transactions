# DynamoDBv2.Transactions

A high-performance .NET library for Amazon DynamoDB transactions with **compile-time source generation** — up to **60x faster** than reflection-based mapping with **32% fewer allocations**.

<p align="center">

[![CI](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml/badge.svg)](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml)
[![Auto-Release](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/auto-release.yml/badge.svg)](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/auto-release.yml)
[![codecov](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions/branch/master/graph/badge.svg?token=CYF75Y00KH)](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions)
[![NuGet](https://img.shields.io/nuget/v/DynamoDBv2.Transactions?logo=nuget&label=NuGet)](https://www.nuget.org/packages/DynamoDBv2.Transactions)
[![NuGet Downloads](https://img.shields.io/nuget/dt/DynamoDBv2.Transactions?logo=nuget&label=Downloads)](https://www.nuget.org/packages/DynamoDBv2.Transactions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

</p>

<p align="center">

![Unit Tests](https://img.shields.io/badge/Unit_Tests-526_passed-brightgreen?logo=dotnet&logoColor=white)
![Integration Tests](https://img.shields.io/badge/Integration_Tests-176_passed-brightgreen?logo=docker&logoColor=white)
![Source Generator Tests](https://img.shields.io/badge/Source_Generator-34_tests-brightgreen?logo=dotnet&logoColor=white)
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

## What It Covers

- Typed transactional writes through `DynamoDbTransactor`
- Typed transactional reads through `DynamoDbReadTransactor`
- Source-generated mapping for `partial` entities
- Cached reflection fallback for existing non-partial entities
- Optimistic concurrency via `[DynamoDBVersion]`
- Expression-based condition checks
- Composite-key support for reads and request-level write operations
- Table-name prefixing through `DynamoDbMapper.TableNamePrefix`

## Comparison with Standard AWS SDK

This library is focused on transactional DynamoDB workflows and tries to keep those workflows typed and compact.

| Area | `DynamoDBv2.Transactions` | Typical AWS SDK transaction path |
|---|---|---|
| Write transactions | Queue typed operations and execute one `TransactWriteItems` call | Build `TransactWriteItemsRequest` and `TransactWriteItem` objects manually |
| Transactional reads | Queue `Get<T>()` calls and materialize typed results | Build `TransactGetItemsRequest` and deserialize `AttributeValue` maps manually |
| Mapping | Source-generated for `partial` entities, cached reflection otherwise | Usually manual `Dictionary<string, AttributeValue>` work in transaction code |
| Versioning | `[DynamoDBVersion]` is incremented and checked automatically | Manual version increment and condition-expression bookkeeping |
| Condition checks | Expression-based helpers like `ConditionEquals<TModel, TValue>()` | Raw condition strings, placeholder names, and placeholder values |
| Table names | Resolved from DynamoDB model attributes plus optional prefix | Usually hard-coded in low-level transaction requests |

## Performance Comparison

### At a glance

Published benchmark snapshot from this repository:

- 1-item transactional write: `11.99 ms` vs `15.83 ms`
- 3-item transactional write: `13.37 ms` vs `46.44 ms`
- `MapToAttribute` on a simple entity: `2,452 ns` vs `13,056 ns`
- `MapFromAttributes` on a simple entity: `1,225 ns` vs `6,261 ns`

These values come from the benchmark suite in this repository. Treat them as directional measurements, not production guarantees.

### End-to-End Write Benchmark

Comparison target: the existing AWS SDK wrapper benchmark in this repository. These measurements include client, serializer, and local DynamoDB test-environment overhead.

| Scenario | `DynamoDBv2.Transactions` | AWS SDK wrapper benchmark | Relative result |
|---|---:|---:|---:|
| 1-item write | `11.99 ms / 80.96 KB` | `15.83 ms / 83.77 KB` | `1.3x` faster |
| 3-item write | `13.37 ms / 114.74 KB` | `46.44 ms / 251.01 KB` | `3.5x` faster |

The 3-item scenario is where the transactional wrapper shows the biggest practical gain in the published suite: one transactional write request versus per-item save work in the comparison benchmark.

### Mapper Comparison: Simple Entity

Entity shape: 15 properties, mostly primitives.

| Operation | Source-generated | Reflection fallback | Relative result |
|---|---:|---:|---:|
| `MapToAttribute` | `2,452 ns / 2,464 B` | `13,056 ns / 3,616 B` | `5.3x` faster, `32%` fewer allocations |
| `GetPropertyAttributedName` | `21 ns / 0 B` | `59 ns / 0 B` | `2.7x` faster |
| `GetHashKeyAttributeName` | `16 ns / 0 B` | `27 ns / 0 B` | `1.7x` faster |
| `GetVersion` | `64 ns / 56 B` | `106 ns / 56 B` | `1.7x` faster |
| `GetTableName` | `13 ns / 0 B` | `796 ns / 144 B` | about `60x` faster |

### Mapper Comparison: Complex Entity

Entity shape: 19 properties with nested objects, collections, and dictionaries.

| Operation | Source-generated | Reflection fallback | Relative result |
|---|---:|---:|---:|
| `MapToAttribute` | `13,209 ns / 8,144 B` | `25,020 ns / 9,361 B` | `1.9x` faster, `13%` fewer allocations |
| `GetPropertyAttributedName` | `18 ns / 0 B` | `57 ns / 0 B` | `3.3x` faster |
| `GetVersion` | `53 ns / 56 B` | `80 ns / 56 B` | `1.5x` faster |

### Deserialization Comparison

| Operation | Source-generated | Reflection fallback | Relative result |
|---|---:|---:|---:|
| `MapFromAttributes` | `1,225 ns / 168 B` | `6,261 ns / 952 B` | `5.1x` faster, `82%` fewer allocations |
| `MapFromAttributes` complex | `1,109 ns / 216 B` | `4,162 ns / 592 B` | `3.8x` faster, `63%` fewer allocations |
| Round-trip serialize + deserialize | `3,436 ns / 2,656 B` | `17,483 ns / 4,569 B` | `5.1x` faster, `42%` fewer allocations |

### What These Numbers Mean

- Primitive-heavy entities benefit the most because the generator emits direct `AttributeValue` construction.
- Complex entities still benefit, but the gap narrows because nested objects and collections use the runtime mapper.
- Metadata lookups like `GetTableName()` and `GetPropertyAttributedName()` are effectively free on the generated path.
- The end-to-end benchmark in this repo compares against the existing AWS SDK wrapper benchmark, not a hand-tuned low-level request implementation.

Published benchmark environments:

- Mapper and deserialization benchmarks: BenchmarkDotNet `0.15.8`, Linux Ubuntu 25.10, Intel Core i7-8700, .NET `10.0.3`, `launchCount=3`, `warmupCount=5`, `iterationCount=20`
- End-to-end benchmark: BenchmarkDotNet `0.13.12`, Windows 11, AMD Ryzen 9 6900HS, .NET `8.0.2`

## Installation

```bash
dotnet add package DynamoDBv2.Transactions
```

The NuGet package includes the source generator automatically.

Supported target frameworks:

- `net8.0`
- `net9.0`
- `net10.0`

## Define an Entity

The library uses standard DynamoDB attributes from `Amazon.DynamoDBv2.DataModel`.

```csharp
using Amazon.DynamoDBv2.DataModel;
using DynamoDBv2.Transactions;

[DynamoDBTable("Orders")]
public partial class Order : ITransactional
{
    [DynamoDBHashKey("PK")]
    public string OrderId { get; set; } = "";

    [DynamoDBProperty("CustomerName")]
    public string CustomerName { get; set; } = "";

    [DynamoDBProperty("Status")]
    public string Status { get; set; } = "";

    [DynamoDBProperty("Total")]
    public decimal Total { get; set; }

    [DynamoDBVersion]
    public long? Version { get; set; }
}
```

Notes:

- `partial` enables source-generated mapping.
- Non-partial classes still work through reflection.
- `ITransactional` is optional. Versioning is driven by `[DynamoDBVersion]`.

## Quick Start

### Write transaction

`DynamoDbTransactor` queues operations and sends a single `TransactWriteItems` request when it is disposed.

```csharp
var client = new AmazonDynamoDBClient();

await using (var tx = new DynamoDbTransactor(client))
{
    tx.CreateOrUpdate(new Order
    {
        OrderId = "ORD-001",
        CustomerName = "Alice",
        Status = "Pending",
        Total = 149.99m
    });
}
```

### Transactional read

`DynamoDbReadTransactor` queues `Get` operations and executes them when you call `ExecuteAsync()`.

```csharp
var reader = new DynamoDbReadTransactor(client);

reader.Get<Order>("ORD-001");

var result = await reader.ExecuteAsync();
var order = result.GetItem<Order>(0);
```

## Write API

### Create or update

```csharp
await using (var tx = new DynamoDbTransactor(client))
{
    tx.CreateOrUpdate(order);
}
```

If the model has a `[DynamoDBVersion]` property, the library increments it automatically and adds the corresponding condition expression.

### Delete

Delete by inferred hash key:

```csharp
await using (var tx = new DynamoDbTransactor(client))
{
    tx.DeleteAsync<Order>("ORD-001");
}
```

Delete by explicit property:

```csharp
await using (var tx = new DynamoDbTransactor(client))
{
    tx.DeleteAsync<Order, string>(x => x.OrderId, "ORD-001");
}
```

Delete by explicit key name:

```csharp
await using (var tx = new DynamoDbTransactor(client))
{
    tx.DeleteAsync<Order>("PK", "ORD-001");
}
```

### Patch a single property

Patch by hash key and expression:

```csharp
await using (var tx = new DynamoDbTransactor(client))
{
    tx.PatchAsync<Order, string>("ORD-001", x => x.Status, "Shipped");
}
```

Patch from an existing model instance:

```csharp
order.Status = "Shipped";

await using (var tx = new DynamoDbTransactor(client))
{
    tx.PatchAsync(order, nameof(order.Status));
}
```

### Condition checks

Standalone condition check:

```csharp
await using (var tx = new DynamoDbTransactor(client))
{
    tx.ConditionEquals<Order, string>("ORD-001", x => x.Status, "Pending");
}
```

Composite-key condition check:

```csharp
await using (var tx = new DynamoDbTransactor(client))
{
    tx.ConditionVersionEquals<OrderLine>(
        "ORD-001",
        "LINE-001",
        x => x.Version,
        3);
}
```

Available helper methods:

- `ConditionEquals<TModel, TValue>`
- `ConditionNotEquals<TModel, TValue>`
- `ConditionGreaterThan<TModel, TValue>`
- `ConditionLessThan<TModel, TValue>`
- `ConditionVersionEquals<TModel>`

Important DynamoDB rule:

- A transaction cannot contain multiple operations on the same item.
- For example, a `ConditionCheck` and a `Patch` against the same key in the same transaction are invalid.

### Transaction options

```csharp
await using (var tx = new DynamoDbTransactor(client))
{
    tx.Options = new TransactionOptions
    {
        ClientRequestToken = "order-001-confirm-v1",
        ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,
        ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.SIZE
    };

    tx.CreateOrUpdate(order);
}
```

## Read API

### Full item

```csharp
var reader = new DynamoDbReadTransactor(client);

reader.Get<Order>("ORD-001");

var result = await reader.ExecuteAsync();
var order = result.GetItem<Order>(0);
```

### Projection

```csharp
var reader = new DynamoDbReadTransactor(client);

reader.Get<Order>("ORD-001", x => new { x.Status, x.Total });

var result = await reader.ExecuteAsync();
var order = result.GetItem<Order>(0);
```

### Composite keys

```csharp
var reader = new DynamoDbReadTransactor(client);

reader.Get<OrderLine>("ORD-001", "LINE-001");
reader.Get<OrderLine>("ORD-001", "LINE-002");

var result = await reader.ExecuteAsync();
```

### Read options and raw access

```csharp
var reader = new DynamoDbReadTransactor(client)
{
    Options = new ReadTransactionOptions
    {
        ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL
    }
};

reader.Get<Order>("ORD-001");

var result = await reader.ExecuteAsync();
var order = result.GetItem<Order>(0);
var raw = result.GetRawItem(0);
var capacity = result.ConsumedCapacity;
```

`TransactionGetResult` gives you:

- `GetItem<T>(index)` for a typed item
- `GetRawItem(index)` for raw DynamoDB attributes
- `GetItems<T>()` for all result items requested as `T`
- `ConsumedCapacity` when requested

## Composite Keys

Read helpers support composite keys directly.

For write operations, request-level constructors provide the most complete composite-key coverage:

```csharp
using DynamoDBv2.Transactions.Requests;
using DynamoDBv2.Transactions.Requests.Properties;

await using (var tx = new DynamoDbTransactor(client))
{
    tx.AddRawRequest(new DeleteTransactionRequest<OrderLine>(
        "ORD-001",
        "LINE-001"));

    tx.AddRawRequest(new PatchTransactionRequest<OrderLine>(
        "ORD-001",
        "LINE-002",
        new Property
        {
            Name = nameof(OrderLine.Status),
            Value = "Packed"
        }));
}
```

If you need full control over request composition, `AddRawRequest()` is the escape hatch.

## Mapping Modes

### Source-generated mapping

Recommended for new entities.

Use a `partial` class with DynamoDB attributes:

```csharp
[DynamoDBTable("Orders")]
public partial class Order
{
    [DynamoDBHashKey("PK")]
    public string OrderId { get; set; } = "";
}
```

You can also opt in explicitly:

```csharp
[DynamoDbGenerateMapping]
[DynamoDBTable("Orders")]
public partial class Order
{
    [DynamoDBHashKey("PK")]
    public string OrderId { get; set; } = "";
}
```

### Reflection fallback

Existing entities do not need to be changed:

```csharp
[DynamoDBTable("LegacyOrders")]
public class LegacyOrder
{
    [DynamoDBHashKey("PK")]
    public string OrderId { get; set; } = "";
}
```

Both modes use the same public API.

## Supported Mapping Features

- `[DynamoDBHashKey]`
- `[DynamoDBRangeKey]`
- `[DynamoDBProperty]`
- `[DynamoDBVersion]`
- `[DynamoDBIgnore]`
- `enum` values
- `DateTimeOffset`
- nested classes and records
- dictionaries and collections through the runtime mapper

Global table prefixing is also supported:

```csharp
DynamoDbMapper.TableNamePrefix = "dev-";
```

## Advanced Requests

You can build request objects directly when the convenience API is not enough.

Example: return the old item when a condition check fails.

```csharp
using Amazon.DynamoDBv2.Model;
using DynamoDBv2.Transactions.Requests;

var request = new ConditionCheckTransactionRequest<Order>("ORD-001");
request.Equals<Order, string>(x => x.Status, "Pending");
request.ReturnValuesOnConditionCheckFailure =
    ReturnValuesOnConditionCheckFailure.ALL_OLD;

await using (var tx = new DynamoDbTransactor(client))
{
    tx.AddRawRequest(request);
}
```

## Current Limitations

This README reflects the current codebase, including a few important constraints:

- Convenience key-based APIs are string-oriented. Tables with Number or Binary keys are only partially supported today.
- `Get` helpers currently assume string hash and range key values.
- For composite-key patch and delete workflows, prefer explicit request constructors through `AddRawRequest()`.
- Query, Scan, and non-transactional CRUD are out of scope.
- Write transactions are executed on `DisposeAsync()`. If the transactor is never disposed, nothing is sent.

## Development

### Unit tests

```bash
dotnet test test/DynamoDBv2.Transactions.UnitTests/DynamoDBv2.Transactions.UnitTests.csproj -c Release
```

### Integration tests

Start LocalStack:

```bash
docker compose up -d localstack
```

Then run:

```bash
dotnet test test/DynamoDBv2.Transactions.IntegrationTests/DynamoDBv2.Transactions.IntegrationTests.csproj -c Release
```

### Benchmarks

```bash
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release
```

Useful filters:

```bash
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release -- --filter '*MapperBenchmark*'
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release -- --filter '*DeserializationBenchmark*'
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release -- --filter '*Benchmark*'
```

## License

MIT. See [LICENSE](LICENSE).
