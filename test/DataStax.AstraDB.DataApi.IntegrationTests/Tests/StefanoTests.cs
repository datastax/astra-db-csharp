using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.SerDes;
using DataStax.AstraDB.DataApi.Tables;
using System.ComponentModel.DataAnnotations;
using DataStax.AstraDB.DataApi.Utils;

using Xunit;

using MongoDB.Bson;
using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

public class SteoCollectionObjectA
{
    [DocumentId]
    [ColumnPrimaryKey]
    public int? Id { get; set; }
    public string Name { get; set; }
    [DocumentMapping(DocumentMappingField.Vector)]
    [ColumnVector(1536)]
    public float[] VectorEmbeddings { get; set; }
}

public class SteoCollectionObjectVze
{
    [DocumentId]
    public int? Id { get; set; }
    public string Name { get; set; }
    [DocumentMapping(DocumentMappingField.Vectorize)]
    public string StringToVectorize => Name;
    [DocumentMapping(DocumentMappingField.Vector)]
    // [ColumnVector(1024)]
    public float[]? TheVector { get; set; }
}

[TableName("tableNameWithObject")]
public class SteoTableObjectA
{
    [ColumnPrimaryKey]
    public int Id { get; set; }
    public string Name { get; set; }
    [ColumnVector(1536)]
    public float[] VectorEmbeddings { get; set; }
}

public class SteoTableObjectB
{
    [ColumnPrimaryKey]
    public int Id { get; set; }
    public string Name { get; set; }
    [ColumnVector(1536)]
    public float[] VectorEmbeddings { get; set; }
}

public class SteoTableObjectVzeFull
{
  [ColumnPrimaryKey]
  public string a { get; set; }

  [ColumnVectorize(
    1024,
    serviceProvider: "nvidia",
    serviceModelName: "nvidia/nv-embedqa-e5-v5"
  )]
  public object? vze { get; set; }
}

// public class SteoTableObjectVzeNoDimension
// {
//   [ColumnPrimaryKey]
//   public string a { get; set; }

//   [ColumnVectorize(
//     null, <== errors!
//     serviceProvider: "nvidia",
//     serviceModelName: "nvidia/nv-embedqa-e5-v5"
//   )]
//   public object? vze { get; set; }
// }

[TableName("inet_test_table")]
public class InetTableObject
{
    [ColumnPrimaryKey]
    public string id { get; set; }
    public System.Net.IPAddress the_inet { get; set; }
}

[TableName("blob_test_table")]
public class BlobTableObject
{
    [ColumnPrimaryKey]
    public string id { get; set; }
    public byte[] blb { get; set; }
}

[UserDefinedType()]
public class SteoMiniUDT
{
    public int? a { get; set; }
    public string? b { get; set; }
}

public class SteoMiniUDTObject
{
    [ColumnPrimaryKey()]
    public string id { get; set; }
    public SteoMiniUDT value { get; set; }
}


[Collection("Database")]
public class SteoTests
{
    DatabaseFixture fixture;

    public SteoTests(AssemblyFixture assemblyFixture, DatabaseFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoPassingDummyTest()
    {
        try
        {
            int theNumber = 123;
            Assert.Equal(theNumber, 120 + 3);
        }
        finally
        {
            var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Test test designed to fail, skip please!")]
    public async Task SteoFailingDummyTest()
    {
        try
        {
            int theNumber = 123;
            Assert.Equal(theNumber, 120 - 3);
        }
        finally
        {
            var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoCollectionNamePrecedenceTest()
    {
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<SteoCollectionObjectA>("explicitCollName"); // WORKS
            // var collection = await fixture.Database.CreateCollectionAsync<SteoCollectionObjectA>(); <== THERE'S NO OVERLOAD FOR THIS (there's no annotation in class for collection name)
        }
        finally
        {
            var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoCollectionVectorizeExperimentsTest()
    {
        try
        {
            var options = new CollectionDefinition
            {
                Vector = new VectorOptions
                {
                    Metric = SimilarityMetric.Cosine,
                    Service = new VectorServiceOptions
                    {
                        Provider = "nvidia",
                        ModelName = "nvidia/nv-embedqa-e5-v5"
                    }
                }
            };
            var collection = await fixture.Database.CreateCollectionAsync<SteoCollectionObjectVze>("c_vze", options);
            SteoCollectionObjectVze doc = new SteoCollectionObjectVze
            {
                Id = 0,
                Name = "The sentence"
            };
            await collection.InsertOneAsync(doc);

            var findOptions = new DocumentFindOptions<SteoCollectionObjectVze>(){
                Projection = Builders<Document>
                    .Projection.Include("$vector"),
            };
            var fdoc = await collection.FindOneAsync<SteoCollectionObjectVze>(findOptions);
            Assert.NotNull(fdoc);
            Assert.Equal("The sentence", fdoc.StringToVectorize);
            Assert.NotNull(fdoc.TheVector);
            Assert.Equal(1024, fdoc.TheVector.Length);

        }
        finally
        {
            var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableNamePrecedenceTest()
    {
        try
        {
            //var table_e = await fixture.Database.CreateTableAsync<SteoTableObjectA>("explicitTbllName");
            //var table_i = await fixture.Database.CreateTableAsync<SteoTableObjectA>();
            //var table_no_e = await fixture.Database.CreateTableAsync<SteoTableObjectB>("explicitTbllNameB");
            var table_no_i = await fixture.Database.CreateTableAsync<SteoTableObjectB>();
        }
        finally
        {
            var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableVectorizeExperimentsTest()
    {
        try
        {
            // var mytable = await fixture.Database.CreateTableAsync<SteoTableObjectVzeFull>("vze_c_full_obj"); // works

            // var mytable = await fixture.Database.CreateTableAsync<SteoTableObjectVzeNoDimension>("vze_c_nodim_obj"); <== not possible, it seems

            // untyped: works with and without dimension
            var createDefinition = new TableDefinition()
                .AddColumn("a", DataApiType.Text())
                .AddColumn("vze", DataApiType.Vectorize(new VectorServiceOptions
                {
                    Provider = "nvidia",
                    ModelName = "nvidia/nv-embedqa-e5-v5"
                }))
                .AddSinglePrimaryKey("a");

            var table = await fixture.Database.CreateTableAsync("vze_c_nodim_def", createDefinition);

        }
        finally
        {
            var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableInetDataTypeTest()
    {
        try
        {
            var table_i = await fixture.Database.CreateTableAsync<InetTableObject>(new CreateTableCommandOptions()
            {
                SkipIfExists = true
            });

            var items = new List<(string id, string ipString)>() {
                ("i_01", "0.0.0.0"),
                ("i_02", "127.0.0.1"),
                ("i_03", "192.168.0.1"),
                ("i_04", "255.255.255.255"),
                ("i_05", "8.8.8.8"),
                ("i_06", "1.2.3.4"),
                ("i_07", "10.0.0.0"),
                ("i_08", "172.16.0.1"),
                ("i_09", "169.254.1.1"),
                ("i_10", "224.0.0.1"),
                ("i_11", "001.002.003.004"),
                ("i_12", "010.000.000.001"),
                ("i_13", "::"),
                ("i_14", "::1"),
                ("i_15", "2001:0db8:0000:0000:0000:0000:0000:0001"),
                ("i_16", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
                ("i_17", "0000:0000:0000:0000:0000:0000:0000:0000"),
                ("i_18", "2001:db8::1"),
                ("i_19", "2001:db8:0:0:0:0:2:1"),
                ("i_20", "2001:db8::2:1"),
                ("i_21", "2001:0db8:0000:0000:0000:0000:1428:57ab"),
                ("i_22", "fe80::"),
                ("i_23", "ff02::1"),
                ("i_24", "ff02::2"),
                ("i_25", "::ffff:192.0.2.128"),
                ("i_26", "2001:db8::192.0.2.33"),
                ("i_27", "::ffff:0:192.0.2.128"),
                ("i_28", "2001:DB8::BEEF"),
                ("i_29", "fe80::1%eth0"),
                ("i_30", "fe80::1%25eth0"),
            };

            for (var i = 0; i < items.Count; i++)
            {
                try
                {
                    var ipAddress = System.Net.IPAddress.Parse(items[i].ipString);
                    var item = new InetTableObject { id = items[i].id, the_inet = ipAddress };
                    await table_i.InsertOneAsync(item);
                }
                catch (Exception ex)
                {
                    // Log or handle the error, but continue with the test
                    Console.WriteLine($"Insert failed for {items[i].id}: {ex.Message}");
                }
            }
        }
        finally
        {
            var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableBlobDataTypeTest()
    {
        try
        {
            var table_blb = await fixture.Database.CreateTableAsync<BlobTableObject>(new CreateTableCommandOptions()
            {
                SkipIfExists = true
            });

            var the_blb = System.Text.Encoding.ASCII.GetBytes("Test Blob");
            var item = new BlobTableObject { id = "the_id", blb = the_blb };
            await table_blb.InsertOneAsync(item);
            var doc = await table_blb.FindOneAsync();
            Assert.NotNull(doc);
            Assert.Equal(the_blb, doc.blb);
        }
        finally
        {
            var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableUntypedUDTReadingTest()
    {
        var udtTestTable = fixture.Database.GetTable("s_udt_table");

        var filter_full = Builders<Row>.Filter.Eq("id", "full");
        var row_full = await udtTestTable.FindOneAsync(filter_full);
        Assert.Equal("full", ((System.Text.Json.JsonElement)row_full["id"]).GetString());
        Assert.Equal("one", ((System.Text.Json.JsonElement)row_full["value"]).GetProperty("b").GetString());
        Assert.Equal(1, ((System.Text.Json.JsonElement)row_full["value"]).GetProperty("a").GetInt32());

        var filter_part = Builders<Row>.Filter.Eq("id", "partial");
        var row_part = await udtTestTable.FindOneAsync(filter_part);
        Assert.Equal("partial", ((System.Text.Json.JsonElement)row_part["id"]).GetString());
        Assert.Equal("two", ((System.Text.Json.JsonElement)row_part["value"]).GetProperty("b").GetString());
        // Assert.Null(((System.Text.Json.JsonElement)row_part["value"]).GetProperty("a").GetInt32()); <== FAILS, client does not restore missing nulls in udt (from schema)

        var filter_missing = Builders<Row>.Filter.Eq("id", "missing");
        var row_missing = await udtTestTable.FindOneAsync(filter_missing);
        Assert.Equal("missing", ((System.Text.Json.JsonElement)row_missing["id"]).GetString());
        // Assert.Null(((System.Text.Json.JsonElement)row_missing["value"]).GetProperty("b").GetString()); <== FAILS as above, no filling
        // Assert.Null(((System.Text.Json.JsonElement)row_missing["value"]).GetProperty("a").GetInt32()); <== FAILS as above, no filling
        Assert.Null(row_missing["value"]); // <== there's a top-level null for the udt here
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableTypedUDTReadingTest()
    {
        var udtTestTable = fixture.Database.GetTable<SteoMiniUDTObject>("s_udt_table");

        var filter_full = Builders<SteoMiniUDTObject>.Filter.Eq(r => r.id, "full");
        var row_full = await udtTestTable.FindOneAsync(filter_full);
        Assert.Equal("full", row_full.id);
        Assert.Equal(1,     row_full.value.a);
        Assert.Equal("one", row_full.value.b);

        var filter_part = Builders<SteoMiniUDTObject>.Filter.Eq(r => r.id, "partial");
        var row_part = await udtTestTable.FindOneAsync(filter_part);
        Assert.Equal("partial", row_part.id);
        // Assert.Equal(0, row_part.value.a);      // <== ZERO if the udt class has a non-nullable
        Assert.Null(row_part.value.a);          // <== NULL if the udt class has nullable
        Assert.Equal("two", row_part.value.b);

        var filter_missing = Builders<SteoMiniUDTObject>.Filter.Eq(r => r.id, "missing");
        var row_missing = await udtTestTable.FindOneAsync(filter_missing);
        Assert.Equal("missing", row_missing.id);
        // NO: a big top-level null instead:
        // Assert.Null(row_missing.value.a);
        // Assert.Null(row_missing.value.b);
        Assert.Null(row_missing.value);
    }

}
