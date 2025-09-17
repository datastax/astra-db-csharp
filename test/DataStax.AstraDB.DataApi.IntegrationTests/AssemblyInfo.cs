using Xunit;

[assembly: CollectionBehavior(MaxParallelThreads = 4)]
[assembly: AssemblyFixture(typeof(DataStax.AstraDB.DataApi.IntegrationTests.Fixtures.AssemblyFixture))]
