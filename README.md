# DynamoDBv2.Transactions

A high-performance .NET library for Amazon DynamoDB transactions with **compile-time source generation** — up to **82x faster** than reflection-based mapping.

<p align="center">

[![CI](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml/badge.svg)](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/dotnet.yml)
[![Auto-Release](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/auto-release.yml/badge.svg)](https://github.com/vitalybibikov/DynamoDBv2.Transactions/actions/workflows/auto-release.yml)
[![codecov](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions/branch/master/graph/badge.svg?token=CYF75Y00KH)](https://codecov.io/gh/vitalybibikov/DynamoDBv2.Transactions)
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

The standard AWS SDK `DynamoDBContext` makes an implicit `DescribeTable` network call to resolve key schemas at runtime. **DynamoDBv2.Transactions eliminates that entirely** by reading `[DynamoDBHashKey]` attributes directly — and with the **included source generator**, all mapping is resolved at compile time with zero reflection overhead.

### Performance at a Glance

<table>
<tr>
<td width="50%">

#### Mapper: Source-Generated vs Reflection

| Operation | Source-Gen | Reflection | Speedup |
|-----------|----------:|-----------:|--------:|
| `GetTableName` | 13.9 ns / 0 B | 1,135 ns / 144 B | **82x** |
| `MapToAttribute` (15 props) | 4,049 ns / 3,232 B | 16,412 ns / 4,000 B | **4.1x** |
| `GetHashKeyAttributeName` | 14.4 ns / 0 B | 30.5 ns / 0 B | **2.1x** |
| `GetPropertyAttributedName` | 20.7 ns / 0 B | 39.4 ns / 0 B | **1.9x** |
| `GetVersion` | 144.8 ns / 56 B | 192.5 ns / 56 B | **1.3x** |

</td>
<td width="50%">

#### End-to-End: This Library vs Standard SDK Wrapper

| Scenario | This Library | Standard SDK | Speedup |
|----------|------------:|-----------:|--------:|
| 1-item transaction | 12.0 ms / 81 KB | 15.8 ms / 84 KB | **1.3x** |
| 3-item transaction | 13.4 ms / 115 KB | 46.4 ms / 251 KB | **3.5x** |

The standard SDK wrapper issues **separate `DescribeTable` + write** calls per item. This library batches everything into a single `TransactWriteItems` request.

</td>
</tr>
</table>

> All key lookups are **zero-allocation** via compile-time switch expressions. Just add `partial` to your entity class — no other changes needed.

---

## Installation

```bash
dotnet add package DynamoDBv2.Transactions
```

The package includes the source generator automatically — no additional packages required.

---

## Quick Start

### 1. Define Your Entity

Add `partial` and standard DynamoDB attributes. The source generator handles everything at compile time:

```csharp
[DynamoDBTable("Orders")]
public partial class Order : ITransactional
{
    [DynamoDBHashKey("PK")]
    public string OrderId { get; set; }

    [DynamoDBProperty("CustomerName")]
    public string CustomerName { get; set; }

    [DynamoDBProperty("Total")]
    public decimal Total { get; set; }

    [DynamoDBProperty("Status")]
    public string Status { get; set; }

    [DynamoDBVersion]
    public long? Version { get; set; }
}
```

### 2. Write a Transaction

Transactions execute atomically on `DisposeAsync` — if any operation fails, they all roll back:

```csharp
var client = new AmazonDynamoDBClient();

await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.CreateOrUpdate(new Order
    {
        OrderId = "ORD-001",
        CustomerName = "Alice",
        Total = 99.99m,
        Status = "Confirmed"
    });
}
```

That's it. The source generator wires up all mapping at compile time, the transaction executes on dispose.

---

## How It Works

```
Your Code                    Compile Time                     Runtime
   |                              |                              |
   v                              v                              v
[DynamoDBTable("Orders")]    Source Generator            DynamoDbTransactor
public partial class Order   generates switch-expr       batches operations
   |                         mappings (zero alloc)            |
   v                              |                           v
[DynamoDBHashKey("PK")]      [ModuleInitializer]        TransactWriteItems
public string OrderId        auto-registers at              (single call)
                             app startup
```

**Two mapping strategies, one API:**

| Strategy | When | Performance | Setup |
|----------|------|------------|-------|
| **Source Generator** | Entity class is `partial` | Up to 82x faster, zero allocation | Just add `partial` |
| **Cached Reflection** | Non-partial classes | Good (warmed caches) | No changes needed |

Both strategies are transparent — the same `DynamoDbTransactor` API works with either. The source generator is automatically discovered at startup via `[ModuleInitializer]`.

---

## Features

| Feature | Description |
|---------|-------------|
| **Source Generator** | Compile-time DynamoDB attribute mapping with zero reflection and zero-allocation key lookups |
| **AWS SDK v4** | Built for AWSSDK.DynamoDBv2 v4.x, auto-tracked via Dependabot with automated releases |
| **Transactional Operations** | `CreateOrUpdate`, `Delete`, `Patch`, `Update`, `ConditionCheck` — all in a single atomic request |
| **Optimistic Concurrency** | Automatic version increment via `ITransactional` — no manual version tracking needed |
| **Condition Expressions** | Type-safe `Equals`, `NotEquals`, `GreaterThan`, `LessThan`, `VersionEquals` checks |
| **100-Item Validation** | Enforces DynamoDB's transaction limit before sending the request |
| **TransactionOptions** | `ClientRequestToken` (idempotency), `ReturnConsumedCapacity`, `ReturnItemCollectionMetrics` |
| **Multi-targeting** | .NET 8.0, .NET 9.0, and .NET 10.0 |
| **Source Link + Deterministic** | Step-into debugging from NuGet, reproducible builds |

---

## Usage Examples

### Create or Update

```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.CreateOrUpdate(new Order
    {
        OrderId = "ORD-001",
        CustomerName = "Alice",
        Total = 149.99m,
        Status = "Confirmed"
    });
}
// Transaction executes atomically on DisposeAsync
```

### Delete

```csharp
// Delete by hash key (attribute name inferred from [DynamoDBHashKey])
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.DeleteAsync<Order>("ORD-001");
}

// Delete with explicit key name
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.DeleteAsync<Order>("PK", "ORD-001");
}

// Delete with expression (type-safe)
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.DeleteAsync<Order, string>(o => o.OrderId, "ORD-001");
}
```

### Patch a Single Property

Update one field without touching the rest of the item:

```csharp
// Expression-based (type-safe)
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.PatchAsync<Order, string>("ORD-001", o => o.Status, "Shipped");
}

// Instance-based (patch from an existing object)
await using (var transactor = new DynamoDbTransactor(client))
{
    order.Status = "Shipped";
    transactor.PatchAsync(order, nameof(order.Status));
}
```

### Conditional Checks

Assert conditions before writing — the entire transaction rolls back if any check fails:

```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    // Only update if the order total is above the threshold
    transactor.ConditionGreaterThan<Order, decimal>("ORD-001", o => o.Total, 50.0m);
    transactor.PatchAsync<Order, string>("ORD-001", o => o.Status, "Priority");
}
```

Available condition methods:
- `ConditionEquals<TModel, TValue>` — field must equal value
- `ConditionNotEquals<TModel, TValue>` — field must not equal value
- `ConditionGreaterThan<TModel, TValue>` — field must be greater than value
- `ConditionLessThan<TModel, TValue>` — field must be less than value
- `ConditionVersionEquals<TModel>` — optimistic concurrency version check

### Optimistic Concurrency (Versioning)

Implement `ITransactional` to get automatic version management:

```csharp
[DynamoDBTable("Orders")]
public partial class Order : ITransactional
{
    [DynamoDBHashKey("PK")]
    public string OrderId { get; set; }

    [DynamoDBVersion]
    public long? Version { get; set; }  // Auto-incremented on every write
}

// First write: Version is set to 0
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.CreateOrUpdate(new Order { OrderId = "ORD-001" });
}

// Second write: Version auto-increments to 1, with a condition check
// that the current version in DynamoDB matches the expected version.
// If another writer modified the item, this transaction fails.
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.CreateOrUpdate(existingOrder);  // Version check is automatic
}

// Explicit version check
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.ConditionVersionEquals<Order>("ORD-001", o => o.Version, 1);
    transactor.PatchAsync<Order, string>("ORD-001", o => o.Status, "Completed");
}
```

### Complex Multi-Operation Transaction

Combine multiple operations in a single atomic transaction (up to 100 items):

```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    // Check: inventory must be sufficient
    transactor.ConditionGreaterThan<Inventory, int>(
        "SKU-100", i => i.Quantity, 0);

    // Create the order
    transactor.CreateOrUpdate(new Order
    {
        OrderId = "ORD-042",
        CustomerName = "Bob",
        Total = 299.99m,
        Status = "Confirmed"
    });

    // Update inventory count
    transactor.PatchAsync<Inventory, int>(
        "SKU-100", i => i.Quantity, currentQuantity - 1);

    // Log the transaction
    transactor.CreateOrUpdate(new AuditLog
    {
        LogId = Guid.NewGuid().ToString(),
        Action = "OrderCreated",
        Timestamp = DateTime.UtcNow
    });
}
// All 4 operations succeed or fail together
```

### Transaction Options

Configure idempotency and capacity tracking:

```csharp
await using (var transactor = new DynamoDbTransactor(client))
{
    transactor.Options = new TransactionOptions
    {
        // Idempotency token — retrying with the same token within 10 minutes
        // returns the original result instead of executing again
        ClientRequestToken = "order-042-confirm-v1",

        // Track consumed read/write capacity units
        ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL,

        // Track item collection metrics (useful for tables with LSIs)
        ReturnItemCollectionMetrics = ReturnItemCollectionMetrics.SIZE
    };

    transactor.CreateOrUpdate(order);
    transactor.PatchAsync<Inventory, int>("SKU-100", i => i.Quantity, newQty);
}
```

### Dependency Injection

Use `ITransactionManager` for testable, DI-friendly code:

```csharp
// Registration
services.AddSingleton<IAmazonDynamoDB>(new AmazonDynamoDBClient());
services.AddSingleton<ITransactionManager, TransactionManager>();

// Usage
public class OrderService(ITransactionManager transactionManager)
{
    public async Task ConfirmOrder(Order order)
    {
        await using var transactor = new DynamoDbTransactor(transactionManager);
        transactor.CreateOrUpdate(order);
    }
}
```

---

## Source Generator Deep Dive

### How Discovery Works

The source generator runs at compile time and discovers entities via **two pipelines**:

1. **Auto-discovery** — any `partial` class with a `[DynamoDBHashKey]` property
2. **Explicit opt-in** — classes decorated with `[DynamoDbGenerateMapping]`

```csharp
// Auto-discovered (recommended)
[DynamoDBTable("Orders")]
public partial class Order
{
    [DynamoDBHashKey("PK")]
    public string OrderId { get; set; }
}

// Explicit opt-in
[DynamoDbGenerateMapping]
[DynamoDBTable("Orders")]
public partial class Order
{
    [DynamoDBHashKey("PK")]
    public string OrderId { get; set; }
}
```

### What Gets Generated

For each discovered entity, the generator emits:

- **`__DynamoDbMetadata` nested class** with:
  - `GetPropertyAttributeName(string)` — zero-allocation switch expression
  - `MapToAttributes(T)` — compile-time serialization (no `PropertyInfo.GetValue()`)
  - `GetVersion(T)` — direct property access
- **`DynamoDbMappingRegistration.g.cs`** — `[ModuleInitializer]` that registers all types at app startup

### Fallback Behavior

Non-partial classes work seamlessly via cached reflection. You can mix both in the same project:

```csharp
// Source-generated (82x faster lookups)
public partial class FastEntity { ... }

// Reflection fallback (still fast with warmed caches)
public class LegacyEntity { ... }
```

---

## Detailed Benchmark Results

### Mapper: Source-Generated vs Reflection

Isolated mapping operations — no DynamoDB I/O. Entity with 15 properties including all common types.
Reflection results use warmed-up `ConcurrentDictionary` caches (best-case reflection).

```
BenchmarkDotNet v0.15.8, Linux Ubuntu 25.10
.NET SDK 9.0.311, .NET 9.0.13, X64 RyuJIT x86-64-v3

Runtime=.NET 9.0  IterationCount=20  LaunchCount=3  WarmupCount=5
```

| Method | Mean | Allocated | vs Reflection |
|--------|-----:|----------:|--------------:|
| MapToAttribute **(source-generated)** | 4,048.56 ns | 3,232 B | **4.1x faster** |
| MapToAttribute (reflection) | 16,412.42 ns | 4,000 B | baseline |
| GetPropertyAttributedName **(source-gen)** | 20.74 ns | 0 B | **1.9x faster** |
| GetPropertyAttributedName (reflection) | 39.38 ns | 0 B | baseline |
| GetHashKeyAttributeName **(source-gen)** | 14.43 ns | 0 B | **2.1x faster** |
| GetHashKeyAttributeName (reflection) | 30.49 ns | 0 B | baseline |
| GetVersion **(source-generated)** | 144.75 ns | 56 B | **1.3x faster** |
| GetVersion (reflection) | 192.52 ns | 56 B | baseline |
| GetTableName **(source-generated)** | 13.87 ns | 0 B | **82x faster** |
| GetTableName (reflection) | 1,135.12 ns | 144 B | baseline |

### End-to-End: Full Transaction (includes network I/O)

```
BenchmarkDotNet v0.13.12, Windows 11
AMD Ryzen 9 6900HS, .NET 8.0.2

Job=OutOfProc  IterationCount=15  LaunchCount=3  WarmupCount=10
```

| Method | Mean | Error | StdDev | Allocated |
|--------|-----:|------:|-------:|----------:|
| **DynamoDBv2.Transactions (1 item)** | **11.99 ms** | 0.046 ms | 0.087 ms | **80.96 KB** |
| Standard SDK Wrapper (1 item) | 15.83 ms | 0.236 ms | 0.442 ms | 83.77 KB |
| **DynamoDBv2.Transactions (3 items)** | **13.37 ms** | 0.066 ms | 0.123 ms | **114.74 KB** |
| Standard SDK Wrapper (3 items) | 46.44 ms | 0.444 ms | 0.834 ms | 251.01 KB |

> The 3-item gap is dramatic because the standard SDK issues separate `DescribeTable` + write calls per item, while this library sends a single `TransactWriteItems` request.

### Run Benchmarks Yourself

```bash
# Mapper benchmarks (source-gen vs reflection)
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release -- --filter '*MapperBenchmark*'

# End-to-end benchmarks (requires localstack)
dotnet run --project test/DynamoDBv2.Transactions.Benchmarks -c Release -- --filter '*Benchmark*'
```

---

## Running Tests

```bash
# Integration tests (requires Docker + localstack)
docker-compose up --exit-code-from tests tests localstack

# Unit tests
docker-compose up --exit-code-from unittests unittests
```

**Test coverage:** 259 unit tests, 33 integration tests, 16 source generator tests — all validated in CI on every push.

---

## Versioning

This library's version tracks the underlying AWSSDK.DynamoDBv2 version:

```
Format: {aws_major}.{aws_minor}.{aws_patch}.{aws_rev * 100 + lib_rev}
```

| AWS SDK Version | Library Release | Meaning |
|----------------|----------------|---------|
| 4.0.14.1 | 4.0.14.100 | Initial release for AWS SDK 4.0.14.1 |
| 4.0.14.1 | 4.0.14.101 | Library-only fix (same AWS SDK) |
| 4.0.15.0 | 4.0.15.0 | New AWS SDK version, lib rev 0 |

The first three segments always tell you which AWS SDK version is inside. New AWS SDK versions are auto-released via Dependabot + CI pipeline.

---

## Contributing

When creating PRs, please ensure:

- [ ] No sensitive information (tokens, keys, credentials) is included
- [ ] All existing tests pass (`docker-compose up --exit-code-from tests tests localstack`)
- [ ] New features include appropriate test coverage

---

## License

Copyright &copy; 2026, Vitali Bibikov. Code released under the [MIT license](LICENSE).

## Contact

Vitali Bibikov — bibikovvitaly@gmail.com
