using DataStax.AstraDB.DataApi.Core;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("Database")]
public class DatabaseCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<DatabaseFixture>
{

}

public class DatabaseFixture : BaseFixture
{
    public DatabaseFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "database")
    {
    }
}