using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
[assembly: AssemblyFixture(typeof(DataStax.AstraDB.DataApi.IntegrationTests.Fixtures.AssemblyFixture))]
