using Xunit;

// Several tests mutate the process-wide static DynamoDbMapper.TableNamePrefix. xUnit runs test
// collections in parallel by default, so a class holding a non-null prefix could race with classes
// that assert on unprefixed table names. Disable cross-collection parallelization to keep the suite
// deterministic.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
