using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Tables;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("UserDefinedTypes")]
public class UserDefinedTypesCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<UserDefinedTypesFixture>
{

}

public class UserDefinedTypesFixture : BaseFixture
{
    public UserDefinedTypesFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "userDefinedTypes")
    {
        
    }

}