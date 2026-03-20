using DataStax.AstraDB.DataApi.Admin;
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
    "nvidia",
    "nvidia/nv-embedqa-e5-v5",
    dimension: 1024
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

public class CollectionDatetimeObject
{
    [DocumentId]
    public string _id {get; set;}
    public DateTime dt_naive { get; set; }
    public DateTime dt_aware { get; set; }
    public DateTime dt_unspecified { get; set; }
}

public class ExtremeTimestampsObject
{
    [DocumentId]
    public string _id {get; set;}
    public DateTime dt_far_past { get; set; }
    public DateTime dt_far_future { get; set; }
    public string far_past_desc { get; set; }
    public string far_future_desc { get; set; }
}

public class SteoTableFillersObject
{
    [ColumnPrimaryKey]
    public string id { get; set; }
    public int? f_int { get; set; }
    public string? f_text { get; set; }
    public DateTime? f_date { get; set; }
    public HashSet<int>? f_s_int { get; set; }
    public List<string>? f_l_text { get; set; }
    public Dictionary<string, DateTime>? f_m_text_date { get; set; }
}

public class SteoTableNansObject{
    [ColumnPrimaryKey]
    public string id { get; set; }
    //
    public float p_float_nan { get; set; }
    public float p_float_pinf { get; set; }
    public float p_float_minf { get; set; }
    public double p_double_nan { get; set; }
    public double p_double_pinf { get; set; }
    public double p_double_minf { get; set; }
    public double[] p_list_double { get; set; }
    public HashSet<double> p_set_double { get; set; }
    public float[] p_list_float { get; set; }
    public HashSet<float> p_set_float { get; set; }
}

public class SteoDurationObject{
    [ColumnPrimaryKey]
    public string id {get; set;}
    public Duration du {get; set;}
}

public class SteoDecimalDocumentObject{
    [DocumentId]
    public string _id {get; set;}
    public decimal dec { get; set; }
    public double dou { get; set; }
}

public class SteoDecimalRowObject{
    [ColumnPrimaryKey]
    public string id {get; set;}
    public decimal dec { get; set; }
    public double dou { get; set; }
}

public class SteoProjTestObject{
    [ColumnPrimaryKey]
    [ColumnName("a_db")]
    public string TheA {get; set;}
    [ColumnName("b_db")]
    public int TheB { get; set; }
}

public class BinaryVectorObject{
    [DocumentId]
    public string _id { get; set; }
    [ColumnVector(3)]
    public float[] TheVector { get; set; }
}

public class ZBook
{
    [ColumnPrimaryKey(1)]
    [ColumnName("title")]
    public string Title { get; set; } = null!;
    [ColumnName("metadata")]
    public Dictionary<string, string>? Metadata { get; set; }
}

public class ZBook2
{
    [ColumnPrimaryKey(1)]
    [ColumnName("title")]
    public string Title { get; set; } = null!;
    [ColumnName("metadata")]
    public Dictionary<int, string>? Metadata { get; set; }
}

public class SBook
{
  [ColumnPrimaryKey(1)]
  [ColumnName("title")]
  public string? Title { get; set; }

  [ColumnPrimaryKey(2)]
  [ColumnName("author")]
  public string? Author { get; set; }

  [ColumnName("map_column_int_str")]
  public Dictionary<int, string>? MapColumnIntStr { get; set; }

  [ColumnName("map_column_str_str")]
  public Dictionary<string, string>? MapColumnStrStr { get; set; }
}

public class SteoNestedCollectionSubobject
{
    public string sfield { get; set; }
    public int ifield { get; set; }
}

public class SteoNestedCollectionObject
{
    [DocumentId]
    public string _id { get; set; }
    public SteoNestedCollectionSubobject subobject { get; set; }
    public Dictionary<string, SteoNestedCollectionSubobject> subobject_map { get; set; }
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
                IfNotExists = true
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
                IfNotExists = true
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

    /*
    For the next two tests, CQL prerequisite:

    create type the_udt (a int, b text);
    create table s_udt_table (id text primary key, value the_udt);
    insert into s_udt_table (id,value) values ('full', {a:1,b:'one'});
    insert into s_udt_table (id,value) values ('partial', {b:'two'});
    insert into s_udt_table (id) values ('missing');
    */

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
        Assert.Null(row_missing["value"]); // <== there's a top-level null for the udt here, as opposed to a UDT with null fields
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

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoCollectionDatetimeTest()
    {
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<CollectionDatetimeObject>("dt_coll");
            var insertee = new CollectionDatetimeObject {
                _id = "from_cs",
                dt_naive = new DateTime(2024, 6, 15, 10, 30, 0, 500, DateTimeKind.Local),
                dt_aware = new DateTime(2024, 6, 15, 10, 30, 0, 500, DateTimeKind.Utc),
                dt_unspecified = new DateTime(2024, 6, 15, 10, 30, 0, 500, DateTimeKind.Unspecified)
            };
            await collection.InsertOneAsync(insertee);

            var filter = Builders<CollectionDatetimeObject>.Filter.Eq(d => d._id, "from_cs");
            var reread = await collection.FindOneAsync(filter);

            // The following fails with:
            //     Expected: 2024-06-15T10:30:00.5000000+02:00
            //     Actual:   2024-06-15T08:30:00.5000000Z
            Assert.Equal(insertee.dt_naive, reread.dt_naive);

            // This is OK
            Assert.Equal(insertee.dt_aware, reread.dt_aware);

            // The following fails with:
            //    Expected: 2024-06-15T10:30:00.5000000
            //    Actual:   2024-06-15T08:30:00.5000000Z
            Assert.Equal(insertee.dt_unspecified, reread.dt_unspecified);

            // Look at what is on the collection:
            // {
            //     "_id": "from_cs",
            //     "dt_aware":       {"$date": 1718447400500}, ==> in UTC this represents 2024, 6, 15, 10, 30, 0, 500000
            //     "dt_naive":       {"$date": 1718440200500}, (same as below)
            //     "dt_unspecified": {"$date": 1718440200500}  ==> in UTC this represents 2024, 6, 15, 8, 30, 0, 500000 !!! (note hour=8)
            // }
        }
        finally
        {
            // var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoCollectionExtremeTimestampsTest()
    {
        /* Setup requires astrapy (or curl if you want):

            from astrapy.data_types import DataAPITimestamp

            tsa=DataAPITimestamp.from_string('+50000-01-01T01:01:01.001Z')
            tsb=DataAPITimestamp.from_string('-50000-01-01T01:01:01.001Z')

            the_doc = {
                "_id": "from_astrapy",
                "dt_far_past": tsb,
                "dt_far_future": tsa,
                "far_past_desc": f"From '{tsb.to_string()}', timestamp_ms={tsb.timestamp_ms}",
                "far_future_desc": f"From '{tsa.to_string()}', timestamp_ms={tsa.timestamp_ms}",
            }

            coll=database.create_collection("extreme_timestamps_coll")
            coll.insert_one(the_doc)

        **Equivalently** insert this with curl to Data API once collection created:
            curl -XPOST \
                $ASTRA_DB_URL/api/json/v1/default_keyspace/extreme_timestamps_coll  \
                -H "token: $ASTRA_DB_TOKEN"  \
                -H "Content-Type: application/json"  \
                -d '{
                "insertOne": {
                    "document": {
                        "_id": "from_astrapy",
                        "dt_far_past": {
                            "$date": -1640014815538999
                        },
                        "dt_far_future": {
                            "$date": 1515680384461001
                        },
                        "far_past_desc": "From '-50000-01-01T01:01:01.001Z', timestamp_ms=-1640014815538999",
                        "far_future_desc": "From '+50000-01-01T01:01:01.001Z', timestamp_ms=1515680384461001"
                    }
                }
            }'
        */
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<ExtremeTimestampsObject>("extreme_timestamps_coll");

            var filter = Builders<ExtremeTimestampsObject>.Filter.Eq(d => d._id, "from_astrapy");
            var et_read = await collection.FindOneAsync(filter); // <== FAILS due to timestamp on collection out of bounds for the DateTime class
        }
        finally
        {
            // var names = await fixture.Database.ListCollectionNamesAsync();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableFillersTest()
    {
        try
        {
            var table = await fixture.Database.CreateTableAsync<SteoTableFillersObject>("cs_fillers", new CreateTableCommandOptions()
            {
                IfNotExists = true
            });
            
            /*
            Now with CQL...
                INSERT INTO cs_fillers (id) VALUES ('null_party');
            */

            var the_row = await table.FindOneAsync();
            Assert.NotNull(the_row);
            // fillers
            Assert.Null(the_row.f_text);
            Assert.Null(the_row.f_int);
            Assert.Null(the_row.f_date);
            // fillers for collection columns
            Assert.NotNull(new HashSet<int>());
            Assert.NotNull(new List<string>());
            Assert.NotNull(new Dictionary<string,DateTime>());
            // Assert.NotNull(the_row.f_s_int); <== FAILS, it's null
            // Assert.NotNull(the_row.f_l_text); <== FAILS, it's null
            // Assert.NotNull(the_row.f_m_text_date); <== FAILS, it's null
            /* therefore these also don't pass:
                Assert.Equal(the_row.f_s_int.Count, 0);
                Assert.Equal(the_row.f_l_text.Count, 0);
                Assert.Equal(the_row.f_m_text_date.Count, 0);
            */
        }
        finally
        {
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableNansTest()
    {
        try
        {
            var table = await fixture.Database.CreateTableAsync<SteoTableNansObject>("cs_nans", new CreateTableCommandOptions()
            {
                IfNotExists = true
            });
            
            /*
            Let the test run once, create the table and whatever.

            Now, do this with CQL:
                INSERT INTO cs_nans (
                    id,
                    p_float_nan,
                    p_float_pinf,
                    p_float_minf,
                    p_double_nan,
                    p_double_pinf,
                    p_double_minf,
                    p_list_double,
                    p_set_double,
                    p_list_float,
                    p_set_float
                ) VALUES (
                    'from_cql',
                    NaN,
                    Infinity,
                    -Infinity,
                    NaN,
                    Infinity,
                    -Infinity,
                    [-43.21, Nan, Infinity, -Infinity, 12.34],
                    {-43.21, Nan, Infinity, -Infinity, 12.34},
                    [-43.21, Nan, Infinity, -Infinity, 12.34],
                    {-43.21, Nan, Infinity, -Infinity, 12.34}
                );
            
            Re-run the test and check the find-one below:
            */

            // Errors: (cannot decode the nans/infinities as doubles/floats at the moment)
            var the_row = await table.FindOneAsync();
            Assert.NotNull(the_row);

            /*
            The error looks like:
                System.Text.Json.JsonException : The JSON value could not be converted to System.Double. Path: $ | LineNumber: 0 | BytePositionInLine: 10.
                ---- System.InvalidOperationException : Cannot get the value of a token type 'String' as a number.

            The actual response from the find is literally the following:
                {
                "data": {
                    "document": {
                    "p_double_pinf": "Infinity",
                    "p_list_float": [
                        -43.21,
                        "NaN",
                        "Infinity",
                        "-Infinity",
                        12.34
                    ],
                    "p_float_minf": "-Infinity",
                    "p_float_nan": "NaN",
                    "p_set_float": [
                        "-Infinity",
                        -43.21,
                        12.34,
                        "Infinity",
                        "NaN"
                    ],
                    "p_list_double": [
                        -43.21,
                        "NaN",
                        "Infinity",
                        "-Infinity",
                        12.34
                    ],
                    "p_set_double": [
                        "-Infinity",
                        -43.21,
                        12.34,
                        "Infinity",
                        "NaN"
                    ],
                    "p_double_nan": "NaN",
                    "id": "from_cql",
                    "p_float_pinf": "Infinity",
                    "p_double_minf": "-Infinity"
                    }
                },
                "status": {
                    "projectionSchema": {
                    "id": {
                        "type": "text"
                    },
                    "p_double_minf": {
                        "type": "double"
                    },
                    "p_double_nan": {
                        "type": "double"
                    },
                    "p_double_pinf": {
                        "type": "double"
                    },
                    "p_float_minf": {
                        "type": "float"
                    },
                    "p_float_nan": {
                        "type": "float"
                    },
                    "p_float_pinf": {
                        "type": "float"
                    },
                    "p_list_double": {
                        "type": "list",
                        "valueType": "double"
                    },
                    "p_list_float": {
                        "type": "list",
                        "valueType": "float"
                    },
                    "p_set_double": {
                        "type": "set",
                        "valueType": "double"
                    },
                    "p_set_float": {
                        "type": "set",
                        "valueType": "float"
                    }
                    }
                }
                }
            */
        }
        finally
        {
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoDurationUnitTest()
    {
        try
        {
            var zerod = Duration.Parse("0");

            Duration.Parse("P1Y");
            Duration.Parse("P3W");

            Assert.Equal(zerod, Duration.Parse("P"));
            Assert.Equal(zerod, Duration.Parse("PT"));
            Assert.Equal(zerod, Duration.Parse("-P"));
            Assert.Equal(zerod, Duration.Parse("-PT"));

            Duration.Parse("1mo");

            // Failures (expected)
            // Assert.Throws<FormatException>(() => Duration.Parse("?P1Y")); // does not throw
            Assert.Throws<FormatException>(() => Duration.Parse("P1YR"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1YG1M"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1Y1B"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1Y2Y1M"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1Y1M4Y"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1Y+1M"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1Y-1M"));
            Assert.Throws<FormatException>(() => Duration.Parse("-P1Y-1M"));
            Assert.Throws<FormatException>(() => Duration.Parse("P3W1D"));
            Assert.Throws<FormatException>(() => Duration.Parse("P3WT12H"));
            Assert.Throws<FormatException>(() => Duration.Parse("P3WT"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1Y1MT1H1M4M1.123S"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1Y1MT1H1M1.123S3H"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1Y1MT1H1M+1.123S"));
            Assert.Throws<FormatException>(() => Duration.Parse("P1Y1MT1H-1M1.123S"));
            Assert.Throws<FormatException>(() => Duration.Parse("-P1Y1MT1H-1M1.123S"));
            // Assert.Throws<FormatException>(() => Duration.Parse("X1mo")); // does not throw
            // Assert.Throws<FormatException>(() => Duration.Parse("1moY")); // does not throw
            // Assert.Throws<FormatException>(() => Duration.Parse("1moX1s")); // does not throw
            // Assert.Throws<FormatException>(() => Duration.Parse("1mo6b1s")); // does not throw
            Assert.Throws<FormatException>(() => Duration.Parse("1mo1d1d1s"));
            Assert.Throws<FormatException>(() => Duration.Parse("1d1mo1s"));
            // Assert.Throws<FormatException>(() => Duration.Parse("1mo+3s")); // does not throw
            // Assert.Throws<FormatException>(() => Duration.Parse("-1d-1s")); // does not throw
            // Assert.Throws<FormatException>(() => Duration.Parse("1d-1s")); // does not throw

            Assert.Equal(
                Duration.Parse("P1Y1M1DT1H1M1.001001001S"),
                new Duration(13, 1, 3661001001001)
            );
            Assert.Equal(
                Duration.Parse("P1M1DT1M"),
                new Duration(1, 1, 60000000000)
            );
            Assert.Equal(
                Duration.Parse("-P1YT1H1.000000123S"),
                new Duration(-12, 0, -3601000000123)
            );

            Assert.Equal(
                Duration.Parse("PT1M1S"),
                Duration.Parse("PT61S")
            );
            Assert.Equal(
                Duration.Parse("P12MT60M"),
                Duration.Parse("P1YT1H")
            );
            Assert.Equal(
                Duration.Parse("P0Y"),
                zerod
            );
            Assert.Equal(
                Duration.Parse("P0M0D"),
                zerod
            );
            Assert.Equal(
                Duration.Parse("-P0DT0S"),
                zerod
            );
            Assert.Equal(
                Duration.Parse("P1YT"),
                Duration.Parse("P1Y")
            );
            Assert.Equal(
                Duration.Parse("-P1YT"),
                Duration.Parse("-P1Y")
            );


            Assert.Equal(
                Duration.Parse("1y1mo1w1d1h1m1s1ms1us1ns"),
                new Duration(13, 8, 3661001001001)
            );
            Assert.Equal(
                Duration.Parse("-123US"),
                Duration.Parse("-123µs")
            );
            Assert.Equal(
                Duration.Parse("1mo1d1m1ms1ns"),
                new Duration(1, 1, 60001000001)
            );
            Assert.Equal(
                Duration.Parse("-1y1w1h1s1us"),
                new Duration(-12, -7, -3601000001000)
            );
            Assert.Equal(
                Duration.Parse("1m1s"),
                Duration.Parse("61s")
            );

            Assert.Equal(
                Duration.Parse("13mo2w1h1s1us"),
                Duration.Parse("1y1mo14d60m1000ms1000ns")
            );
            Assert.Equal(zerod, Duration.Parse("0y"));
            Assert.Equal(zerod, Duration.Parse("0mo0d"));
            Assert.Equal(zerod, Duration.Parse("0w0ns"));
            //
            // // This one: c# errors (astrapy doesn't). I think it's OK to error,
            // // because the Data API will not return an empty string for the null duration
            // // (even though CQLSH displays it as "" (but requires e.g. `0w` when inserting))
            // Assert.Equal(zerod, Duration.Parse(""));
            //
            Assert.Equal(
                zerod,
                Duration.Parse("PT0S")
            );
            Assert.Equal(
                Duration.Parse("-1h1s333ms"),
                Duration.Parse("-PT1H1.333S")
            );
            Assert.Equal(
                Duration.Parse("-191h1s"),
                Duration.Parse("-PT191H1S")
            );
            Assert.Equal(
                Duration.Parse("1h44m3s777000ns"),
                Duration.Parse("PT1H44M3.000777S")
            );
            Assert.Equal(
                Duration.Parse("-1y1s"),
                Duration.Parse("-P1YT1S")
            );

            const long NANOS_PER_MICRO = 1000;
            const long NANOS_PER_MILLI = 1000 * NANOS_PER_MICRO;
            const long NANOS_PER_SECOND = 1000 * NANOS_PER_MILLI;
            const long NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;
            const long NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE;
            //
            Assert.Equal(new Duration(12, 2, 0), Duration.Parse("P1Y2D"));
            Assert.Equal(new Duration(14, 0, 0), Duration.Parse("P1Y2M"));
            Assert.Equal(new Duration(0, 14, 0), Duration.Parse("P2W"));
            Assert.Equal(new Duration(12, 0, 2 * NANOS_PER_HOUR), Duration.Parse("P1YT2H"));
            Assert.Equal(new Duration(-14, 0, 0), Duration.Parse("-P1Y2M"));
            Assert.Equal(new Duration(0, 2, 0), Duration.Parse("P2D"));
            Assert.Equal(new Duration(0, 0, 30 * NANOS_PER_HOUR), Duration.Parse("PT30H"));
            Assert.Equal(new Duration(0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE), Duration.Parse("PT30H20M"));
            Assert.Equal(new Duration(0, 0, 20 * NANOS_PER_MINUTE), Duration.Parse("PT20M"));
            Assert.Equal(new Duration(0, 0, 56 * NANOS_PER_SECOND), Duration.Parse("PT56S"));
            Assert.Equal(new Duration(15, 0, 130 * NANOS_PER_MINUTE), Duration.Parse("P1Y3MT2H10M"));
            //
            Assert.Equal(new Duration (14, 0, 0), Duration.Parse("1y2mo"));
            Assert.Equal(new Duration (-14, 0, 0), Duration.Parse("-1y2mo"));
            Assert.Equal(new Duration (14, 0, 0), Duration.Parse("1Y2MO"));
            Assert.Equal(new Duration (0, 14, 0), Duration.Parse("2w"));
            Assert.Equal(new Duration (0, 2, 10 * NANOS_PER_HOUR), Duration.Parse("2d10h"));
            Assert.Equal(new Duration (0, 2, 0), Duration.Parse("2d"));
            Assert.Equal(new Duration (0, 0, 30 * NANOS_PER_HOUR), Duration.Parse("30h"));
            Assert.Equal(new Duration (0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE), Duration.Parse("30h20m"));
            Assert.Equal(new Duration (0, 0, 20 * NANOS_PER_MINUTE), Duration.Parse("20m"));
            Assert.Equal(new Duration (0, 0, 56 * NANOS_PER_SECOND), Duration.Parse("56s"));
            Assert.Equal(new Duration (0, 0, 567 * NANOS_PER_MILLI), Duration.Parse("567ms"));
            Assert.Equal(new Duration (0, 0, 1950 * NANOS_PER_MICRO), Duration.Parse("1950us"));
            Assert.Equal(new Duration (0, 0, 1950 * NANOS_PER_MICRO), Duration.Parse("1950µs"));
            Assert.Equal(new Duration (0, 0, 1950000), Duration.Parse("1950000ns"));
            Assert.Equal(new Duration (0, 0, 1950000), Duration.Parse("1950000NS"));
            Assert.Equal(new Duration (0, 0, -1950000), Duration.Parse("-1950000ns"));
            Assert.Equal(new Duration (15, 0, 130 * NANOS_PER_MINUTE), Duration.Parse("1y3mo2h10m"));
        }
        finally
        {
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableDurationFormatTest()
    {
        try
        {
            var table_dur = await fixture.Database.CreateTableAsync<SteoDurationObject>("cd_dur", new CreateTableCommandOptions()
            {
                IfNotExists = true
            });

            var the_row = new SteoDurationObject{id="nanoseconds", du=new Duration(0,0,123456789)};
            
            await table_dur.InsertOneAsync(the_row);

            var read_row = await table_dur.FindOneAsync();
            Assert.NotNull(read_row);
            Assert.Equal(the_row.du, read_row.du);
        }
        finally
        {
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoDecimalsInCollectionTest()
    {
        try
        {
            var coll_dec = await fixture.Database.CreateCollectionAsync<SteoDecimalDocumentObject>("c_dec");

            SteoDecimalDocumentObject the_doc = new SteoDecimalDocumentObject{
                _id="x",
                dec=decimal.Parse("12.345678901234567890213"),
                dou=12.345678901234567
            };
            await coll_dec.InsertOneAsync(the_doc);

            // read typed
            SteoDecimalDocumentObject re_read = await coll_dec.FindOneAsync();
            Assert.NotNull(re_read);
            Assert.Equal(the_doc.dec, re_read.dec);
            Assert.Equal(the_doc.dou, re_read.dou);

            // read untyped: all as doubles (not a problem)
            var coll_dec_unt = fixture.Database.GetCollection("c_dec");
            var re_read_unt = await coll_dec_unt.FindOneAsync();
            Assert.NotNull(re_read_unt);
            Assert.Equal(the_doc.dou, re_read_unt["dou"]);
            // Assert.Equal(the_doc.dec, re_read_unt["dec"]); // This (expectedly) fails
        }
        finally
        {
        }
    }
    
    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoDecimalsInTableTest()
    {
        try
        {
            var tb_dec = await fixture.Database.CreateTableAsync<SteoDecimalRowObject>("t_dec", new CreateTableCommandOptions()
            {
                IfNotExists = true
            });

            SteoDecimalRowObject the_row = new SteoDecimalRowObject{
                id="x",
                dec=decimal.Parse("12.345678901234567890213"),
                dou=12.345678901234567
            };
            await tb_dec.InsertOneAsync(the_row);

            // read typed
            SteoDecimalRowObject re_read = await tb_dec.FindOneAsync();
            Assert.NotNull(re_read);
            Assert.Equal(the_row.dec, re_read.dec);
            Assert.Equal(the_row.dou, re_read.dou);

            // read untyped: all as doubles (not a problem)
            var tb_dec_unt = fixture.Database.GetTable("t_dec");
            var re_read_unt = await tb_dec_unt.FindOneAsync();
            Assert.NotNull(re_read_unt);

            // WORKING
            Assert.Equal(the_row.dou, ((System.Text.Json.JsonElement)re_read_unt["dou"]).GetDouble());
            Assert.Equal(the_row.dec, ((System.Text.Json.JsonElement)re_read_unt["dec"]).GetDecimal());

        }
        finally
        {
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableMapTests()
    {
        var tableName = "maps_table";
        try
        {
            var table = await fixture.Database.CreateTableAsync<DictionaryTypeTest>(tableName, new CreateTableCommandOptions()
            {
                IfNotExists = true
            });

            DictionaryTypeTest row = new DictionaryTypeTest()
            {
                Id = 101,
                StringDictionary = new Dictionary<string, string>{},
                IntDictionary = new Dictionary<string, int>{},
                DateTimeKey = new Dictionary<DateTime, string>{},
                IntKey = new Dictionary<int, string>{},
                DecimalKey = new Dictionary<decimal, string>{}
            };

            // await table.InsertOneAsync(row); // FAILS because empty maps cannot be encoded as [] and must remain {}

            // So after the first run the table is created but empty. Try a simple CQL insertion:
            //  insert into maps_table ("Id") values (101);
            // And run the following part:

            var findOptions = new TableFindOptions<DictionaryTypeTest>()
            {
                Filter = Builders<DictionaryTypeTest>.TableFilter.Eq((t) => t.Id, row.Id),
            };
            var read_row = await table.FindOneAsync();
            Assert.NotNull(read_row);
            Assert.Equal(read_row.Id, row.Id);

            // Assert.Equal(read_row.StringDictionary, row.StringDictionary); // FAILS, null found
            // Assert.Equal(read_row.IntDictionary, row.IntDictionary); // FAILS, null found
            // Assert.Equal(read_row.DateTimeKey, row.DateTimeKey); // FAILS, null found
            // Assert.Equal(read_row.IntKey, row.IntKey); // FAILS, null found
            // Assert.Equal(read_row.DecimalKey, row.DecimalKey); // FAILS, null found
        }
        finally
        {
            //await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableTimeuuidTest()
    {
        /* PRELIMINARY
            create table tuid(a int primary key, tuid timeuuid);
            insert into tuid(a,tuid) values(123,a448ba80-1723-11f1-aedc-e7a263c8acfc);
            select * from tuid;
        */
        var table = fixture.Database.GetTable("tuid");

        var row = await table.FindOneAsync();
        Assert.NotNull(row);
        Assert.Equal(((System.Text.Json.JsonElement)row["tuid"]).GetString(), "a448ba80-1723-11f1-aedc-e7a263c8acfc"); // It should not be the case
        // The 'tuid' field should be converted into a Guid based on the schema (which lists the column type as 'timeuuid')
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableCounterTest()
    {
        /* PRELIMINARY
            create table table_counter(a int primary key, c counter);
            update table_counter set c=c+1 where a=123;
            select * from table_counter;
        */
        var table = fixture.Database.GetTable("table_counter");

        var row = await table.FindOneAsync();
        Assert.NotNull(row);
        Assert.Equal(((System.Text.Json.JsonElement)row["c"]).GetInt32(), 1);
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTablePartialSupportTest()
    {
        /* PRELIMINARY
            create table table_madcolumn (a int, b int, mad map<timestamp, blob> static, primary key(a,b));
            insert into table_madcolumn(a,b,mad) values(1,1,{'2023-01-01T12:34:56' : 0xff});
            select * from table_madcolumn;
        */
        var table = fixture.Database.GetTable("table_madcolumn");

        var row = await table.FindOneAsync();
        Assert.NotNull(row);
        Assert.NotNull(((System.Text.Json.JsonElement)row["mad"]));

        // The following whole stuff checks that mad is like this: [['2023-01-01T12:34:56Z', {'$binary': '/w=='}]]

        // Cast to JsonElement
        var madElement = (System.Text.Json.JsonElement)row["mad"];
        // Assert it's an array with one element
        Assert.Equal(System.Text.Json.JsonValueKind.Array, madElement.ValueKind);
        Assert.Equal(1, madElement.GetArrayLength());
        // Get the first (and only) element - should be an array with 2 elements
        var innerArray = madElement[0];
        Assert.Equal(System.Text.Json.JsonValueKind.Array, innerArray.ValueKind);
        Assert.Equal(2, innerArray.GetArrayLength());
        // First element should be a string: "2023-01-01T12:34:56Z"
        var timestampElement = innerArray[0];
        Assert.Equal(System.Text.Json.JsonValueKind.String, timestampElement.ValueKind);
        Assert.Equal("2023-01-01T12:34:56Z", timestampElement.GetString());
        // Second element should be an object (dictionary) with one property "$binary"
        var binaryObject = innerArray[1];
        Assert.Equal(System.Text.Json.JsonValueKind.Object, binaryObject.ValueKind);
        Assert.True(binaryObject.TryGetProperty("$binary", out var binaryValue));
        Assert.Equal(System.Text.Json.JsonValueKind.String, binaryValue.ValueKind);
        Assert.Equal("/w==", binaryValue.GetString());
        // (courtesy of bob)
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoCollectionEmbeddingApiKeyTest()
    {
        try
        {
            var coll = fixture.Database.GetCollection<Book>("a_coll", new DatabaseCollectionCommandOptions()
            {
                EmbeddingApiKey = "test-api-key-here"
            });
            var items = new List<Book>() {
                new Book()
                {
                    Title = "Test Book 1",
                    Author = "Test Author 1",
                    NumberOfPages = 100,
                },
                new Book()
                {
                    Title = "Test Book 2",
                    Author = "Test Author 2",
                    NumberOfPages = 200,
                },
            };
            var insertResult = await coll.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            //Manually verify the API key is included in the request by checking the logs
        }
        finally
        {
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoCustomDomainsDryRunTest()
    {
        string cd_api_endpoint = "https://the-database.at.company.org/api/blibblo/x/";

        var clientOptions = new CommandOptions {Destination = DataApiDestination.ASTRA};

        var client = new DataAPIClient(null, clientOptions);
        var database = client.GetDatabase(cd_api_endpoint);

        var coll = database.GetCollection("whatever_coll");
        // How to better test this?
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoNamesToUseInProjectionTest()
    {
        /* PRELIMINARY
            create table t_for_proj(a_db text primary key, b_db int);
            insert into t_for_proj(a_db,b_db) values('a', 1);
            select * from t_for_proj;
        */
        var table_proj = fixture.Database.GetTable<SteoProjTestObject>("t_for_proj");
        SteoProjTestObject row = await table_proj.FindOneAsync();
        Assert.NotNull(row);
        Assert.Equal(row.TheB, 1);
        Assert.Equal(row.TheA, "a");

        // project with names from obj
        var projection1 = Builders<SteoProjTestObject>.Projection.Include(r => r.TheB);
        SteoProjTestObject pj_obj = await table_proj.FindOneAsync(
            null, new TableFindOptions<SteoProjTestObject>() { Projection = projection1 }
        );
        Assert.NotNull(pj_obj);
        Assert.Equal(pj_obj.TheB, 1);
        Assert.Null(pj_obj.TheA);

        // project with db names
        var projection2 = Builders<SteoProjTestObject>.Projection.Include("b_db");
        SteoProjTestObject pj_db = await table_proj.FindOneAsync(
            null, new TableFindOptions<SteoProjTestObject>() { Projection = projection2 }
        );
        Assert.NotNull(pj_db);
        Assert.Equal(pj_db.TheB, 1);
        Assert.Null(pj_db.TheA);
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoBinaryEncodedVectorTypedCollectionsTest()
    {
        try
        {
            var coll = fixture.Database.GetCollection<BinaryVectorObject>("c");

            var findOptions = new DocumentFindOptions<BinaryVectorObject>(){
                Projection = Builders<Document>
                    .Projection.Include("$vector"),
            };
            var doc = await coll.FindOneAsync(null, findOptions);
            Assert.NotNull(doc);
            Assert.NotNull(doc.TheVector); // FAILS, this should contain the three floats [0.10000000149011612, -0.20000000298023224, 0.30000001192092896]
            Assert.Equal(doc.TheVector.Count(), 3); // likewise FAILS
            /*
            Yet the response from the find is this (as seen in the logs):
                {"data":{"document":{"_id":"1f7478e1-fc54-4d76-b478-e1fc54dd767d","$vector":{"$binary":"PczMzb5MzM0+mZma"}}}}
            */
        }
        finally
        {
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoBinaryEncodedVectorUntypedCollectionsTest()
    {
        try
        {
            var coll = fixture.Database.GetCollection("c");

            var findOptions = new DocumentFindOptions<Document>(){
                Projection = Builders<Document>
                    .Projection.Include("$vector"),
            };
            var doc = await coll.FindOneAsync(null, findOptions);
            Assert.NotNull(doc);

            // Is the vector retrieved/deserialized correctly?
            // p.s. I know this code is horrible (sorry).
            // Still it shows that the vector is not deserialized correctly into a list of floats.
            var vectorNode = doc["$vector"];
            var vectorDict = vectorNode as Dictionary<string, object>;
            Assert.NotNull(vectorDict);
            Assert.True(vectorDict.ContainsKey("$binary"));
            Assert.Equal("PczMzb5MzM0+mZma", ((System.Text.Json.JsonElement)vectorDict["$binary"]).GetString());
            // ==> vectorNode should be a list of floats...
        }
        finally
        {
        }
    }


    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoUserDefinedTypes_CreateFromClasses()
    {
        var tableName = "userDefinedTypesFromClasses";
        try
        {
            List<UdtTest> items = new List<UdtTest>() {
                new()
                {
                    Id = 0,
                    Udt = new TypesTester
                    {
                        String = "Test 1",
                        //Inet = IPAddress.Parse("192.168.0.1"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us1ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 1",
                            Number = 1001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 2",
                            Number = 1002
                        }
                    },
                },
                new()
                {
                    Id = 1,
                    Udt = new TypesTester
                    {
                        String = "Test 2",
                        //Inet = IPAddress.Parse("192.168.0.2"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us2ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 2 dot 1",
                            Number = 2001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 2 dot 2",
                            Number = 2002
                        }
                    },
                },
                new()
                {
                    Id = 2,
                    Udt = new TypesTester
                    {
                        String = "Test 3",
                        //Inet = IPAddress.Parse("192.168.0.3"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us2ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now), // <== leads to a TimeOnly decoding problem
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 3 dot 1",
                            Number = 3001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 3 dot 2",
                            Number = 3002
                        }
                    },
                },
            };

            var table = await fixture.Database.CreateTableAsync<UdtTest>(tableName, new CreateTableCommandOptions()
            {
                IfNotExists = true
            });
            var insertResult = await table.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var filter = Builders<UdtTest>.Filter.Eq(b => b.Udt.String, "Test 3");

            //TODO: Can you filter on UDT fields?
            // var result = await table.FindOneAsync(filter);
            // Assert.NotNull(result);
            // Assert.Equal(2, result.Id);

            // STEO added read-back-and-check
            var myFilter = Builders<UdtTest>.Filter.Eq(b => b.Id, 2);
            var result = await table.FindOneAsync(myFilter); // FAILS because of the TimeOnly field: cannot deserialize a value such as `"Time":"11:00:59.974229900"` coming from JSON
            Assert.NotNull(result);
            Assert.Equal(2, result.Id);

            // detailed
            Assert.Equal(result.Udt.String, items[2].Udt.String);
            Assert.Equal(result.Udt.Int, items[2].Udt.Int);
            Assert.Equal(result.Udt.TinyInt, items[2].Udt.TinyInt);
            Assert.Equal(result.Udt.SmallInt, items[2].Udt.SmallInt);
            Assert.Equal(result.Udt.BigInt, items[2].Udt.BigInt);
            Assert.Equal(result.Udt.Decimal, items[2].Udt.Decimal);
            Assert.Equal(result.Udt.Double, items[2].Udt.Double);
            Assert.Equal(result.Udt.Float, items[2].Udt.Float);
            Assert.Equal(result.Udt.Boolean, items[2].Udt.Boolean);
            Assert.Equal(result.Udt.UUID, items[2].Udt.UUID);
            Assert.Equal(result.Udt.Duration, items[2].Udt.Duration);
            // Assert.Equal(result.Udt.Timestamp, items[2].Udt.Timestamp); // FAILS for another issue (timezones and timestamps, see)
            Assert.Equal(result.Udt.Date, items[2].Udt.Date);
            Assert.Equal(result.Udt.Time, items[2].Udt.Time);
            Assert.Equal(result.Udt.MaybeTimestamp, items[2].Udt.MaybeTimestamp);
            Assert.Equal(result.Udt.MaybeDate, items[2].Udt.MaybeDate);
            Assert.Equal(result.Udt.MaybeTime, items[2].Udt.MaybeTime);
            // Assert.Equal(result.Udt.TimestampWithKind, items[2].Udt.TimestampWithKind); // FAILS for another issue (loss of precision with timestamps, see)

            Assert.Equal(2, result.UdtList.Count);
            Assert.Equal(
                result.UdtList[1].Name,
                items[2].UdtList[1].Name
            );
            Assert.Equal(
                result.UdtList[1].Number,
                items[2].UdtList[1].Number
            );
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }
        finally
        {
            // await fixture.Database.DropTableAsync(tableName);
            // await fixture.Database.DropTypeAsync<TypesTester>();
            // await fixture.Database.DropTypeAsync<SimpleUdt>();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTimezoneTroubleUDTTest()
    {
        var tableName = "userDefinedTypesFromClasses";
        try
        {
            List<UdtTest> items = new List<UdtTest>() {
                new()
                {
                    Id = 0,
                    Udt = new TypesTester
                    {
                        String = "Test 1",
                        //Inet = IPAddress.Parse("192.168.0.1"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us1ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 1",
                            Number = 1001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 2",
                            Number = 1002
                        }
                    },
                },
                new()
                {
                    Id = 1,
                    Udt = new TypesTester
                    {
                        String = "Test 2",
                        //Inet = IPAddress.Parse("192.168.0.2"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us2ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 2 dot 1",
                            Number = 2001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 2 dot 2",
                            Number = 2002
                        }
                    },
                },
                new()
                {
                    Id = 2,
                    Udt = new TypesTester
                    {
                        String = "Test 3",
                        //Inet = IPAddress.Parse("192.168.0.3"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us2ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        // Time = TimeOnly.FromDateTime(DateTime.Now), // <== commented, brings failure due to another issue
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 3 dot 1",
                            Number = 3001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 3 dot 2",
                            Number = 3002
                        }
                    },
                },
            };

            var table = await fixture.Database.CreateTableAsync<UdtTest>(tableName, new CreateTableCommandOptions()
            {
                IfNotExists = true
            });
            var insertResult = await table.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var filter = Builders<UdtTest>.Filter.Eq(b => b.Udt.String, "Test 3");

            // STEO added read-back-and-check
            var myFilter = Builders<UdtTest>.Filter.Eq(b => b.Id, 2);
            var result = await table.FindOneAsync(myFilter);
            Assert.NotNull(result);
            Assert.Equal(2, result.Id);

            // detailed
            Assert.Equal(result.Udt.String, items[2].Udt.String);
            Assert.Equal(result.Udt.Int, items[2].Udt.Int);
            Assert.Equal(result.Udt.TinyInt, items[2].Udt.TinyInt);
            Assert.Equal(result.Udt.SmallInt, items[2].Udt.SmallInt);
            Assert.Equal(result.Udt.BigInt, items[2].Udt.BigInt);
            Assert.Equal(result.Udt.Decimal, items[2].Udt.Decimal);
            Assert.Equal(result.Udt.Double, items[2].Udt.Double);
            Assert.Equal(result.Udt.Float, items[2].Udt.Float);
            Assert.Equal(result.Udt.Boolean, items[2].Udt.Boolean);
            Assert.Equal(result.Udt.UUID, items[2].Udt.UUID);
            Assert.Equal(result.Udt.Duration, items[2].Udt.Duration);
            Assert.Equal(result.Udt.Timestamp, items[2].Udt.Timestamp); // <== FAILS because of timezone roundtrip inconsistency
            /* Failure looks like:
                Assert.Equal() Failure: Values differ
                Expected: 2026-03-05T13:44:22.4390000Z
                Actual:   2026-03-05T14:44:22.4395700+01:00
            */
            Assert.Equal(result.Udt.Date, items[2].Udt.Date);
            Assert.Equal(result.Udt.Time, items[2].Udt.Time);
            Assert.Equal(result.Udt.MaybeTimestamp, items[2].Udt.MaybeTimestamp);
            Assert.Equal(result.Udt.MaybeDate, items[2].Udt.MaybeDate);
            Assert.Equal(result.Udt.MaybeTime, items[2].Udt.MaybeTime);
            // Assert.Equal(result.Udt.TimestampWithKind, items[2].Udt.TimestampWithKind); // commented, fails for another issue (sub-millisecond precision loss)

            Assert.Equal(2, result.UdtList.Count);
            Assert.Equal(
                result.UdtList[1].Name,
                items[2].UdtList[1].Name
            );
            Assert.Equal(
                result.UdtList[1].Number,
                items[2].UdtList[1].Number
            );

        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }
        finally
        {
            // await fixture.Database.DropTableAsync(tableName);
            // await fixture.Database.DropTypeAsync<TypesTester>();
            // await fixture.Database.DropTypeAsync<SimpleUdt>();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoTableUDTDatetimeLimitationsTest()
    {
        var tableName = "datetime_limitations_table";
        try
        {
            List<UdtTest> items = new List<UdtTest>() {
                new()
                {
                    Id = 0,
                    Udt = new TypesTester
                    {
                        String = "Test 1",
                        //Inet = IPAddress.Parse("192.168.0.1"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us1ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 1",
                            Number = 1001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 2",
                            Number = 1002
                        }
                    },
                },
                new()
                {
                    Id = 1,
                    Udt = new TypesTester
                    {
                        String = "Test 2",
                        //Inet = IPAddress.Parse("192.168.0.2"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us2ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        Time = TimeOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 2 dot 1",
                            Number = 2001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 2 dot 2",
                            Number = 2002
                        }
                    },
                },
                new()
                {
                    Id = 2,
                    Udt = new TypesTester
                    {
                        String = "Test 3",
                        //Inet = IPAddress.Parse("192.168.0.3"),
                        Int = int.MaxValue,
                        TinyInt = byte.MaxValue,
                        SmallInt = short.MaxValue,
                        BigInt = long.MaxValue,
                        Decimal = decimal.MaxValue,
                        Double = double.MaxValue,
                        Float = float.MaxValue,
                        Boolean = false,
                        UUID = Guid.NewGuid(),
                        Duration = Duration.Parse("12y3mo1d12h30m5s12ms7us2ns"),
                        Timestamp = DateTime.Now,
                        Date = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTimestamp = null,
                        MaybeDate = DateOnly.FromDateTime(DateTime.Now),
                        MaybeTime = null,
                        TimestampWithKind = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Utc),
                    },
                    UdtList = new List<SimpleUdt>
                    {
                        new SimpleUdt
                        {
                            Name = "List Test 3 dot 1",
                            Number = 3001
                        },
                        new SimpleUdt
                        {
                            Name = "List Test 3 dot 2",
                            Number = 3002
                        }
                    },
                },
            };

            var table = await fixture.Database.CreateTableAsync<UdtTest>(tableName, new CreateTableCommandOptions()
            {
                IfNotExists = true
            });
            var insertResult = await table.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIds.Count);
            var filter = Builders<UdtTest>.Filter.Eq(b => b.Udt.String, "Test 3");

            // STEO added read-back-and-check
            var myFilter = Builders<UdtTest>.Filter.Eq(b => b.Id, 2);
            var result = await table.FindOneAsync(myFilter);
            Assert.NotNull(result);
            Assert.Equal(2, result.Id);

            // detailed
            Assert.Equal(result.Udt.String, items[2].Udt.String);
            Assert.Equal(result.Udt.Int, items[2].Udt.Int);
            Assert.Equal(result.Udt.TinyInt, items[2].Udt.TinyInt);
            Assert.Equal(result.Udt.SmallInt, items[2].Udt.SmallInt);
            Assert.Equal(result.Udt.BigInt, items[2].Udt.BigInt);
            Assert.Equal(result.Udt.Decimal, items[2].Udt.Decimal);
            Assert.Equal(result.Udt.Double, items[2].Udt.Double);
            Assert.Equal(result.Udt.Float, items[2].Udt.Float);
            Assert.Equal(result.Udt.Boolean, items[2].Udt.Boolean);
            Assert.Equal(result.Udt.UUID, items[2].Udt.UUID);
            Assert.Equal(result.Udt.Duration, items[2].Udt.Duration);
            Assert.Equal(result.Udt.Date, items[2].Udt.Date);
            Assert.Equal(result.Udt.Time, items[2].Udt.Time);
            Assert.Equal(result.Udt.MaybeTimestamp, items[2].Udt.MaybeTimestamp);
            Assert.Equal(result.Udt.MaybeDate, items[2].Udt.MaybeDate);
            Assert.Equal(result.Udt.MaybeTime, items[2].Udt.MaybeTime);
            Assert.Equal(result.Udt.TimestampWithKind, items[2].Udt.TimestampWithKind); // FAILS because of loss of precision with timestamps
            /* The failure looks like this:
                Assert.Equal() Failure: Values differ
                Expected: 2026-03-05T15:22:27.9300000Z
                Actual:   2026-03-05T15:22:27.9301995Z
            */

            Assert.Equal(2, result.UdtList.Count);
            Assert.Equal(
                result.UdtList[1].Name,
                items[2].UdtList[1].Name
            );
            Assert.Equal(
                result.UdtList[1].Number,
                items[2].UdtList[1].Number
            );
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }
        finally
        {
            // await fixture.Database.DropTableAsync(tableName);
            // await fixture.Database.DropTypeAsync<TypesTester>();
            // await fixture.Database.DropTypeAsync<SimpleUdt>();
        }
    }

    [Fact(Skip = "Unskip manually when needed.")]
    public async Task SteoNonstringMapTableInsertionTests()
    {

        var tableName = "steo_nonstrmap_insertiontest";

        try
        {
            var table = await fixture.Database.CreateTableAsync<SBook>(tableName,
                new CreateTableCommandOptions() { IfNotExists = true });
            var untypedTable = fixture.Database.GetTable(tableName);

            // typed insertion
            var row = new SBook()
            {
                MapColumnIntStr = new Dictionary<int, string>
                {
                    { 1, "value1" },
                    { 2, "value2" },
                },
                MapColumnStrStr = new Dictionary<string, string>
                {
                    { "key1", "value1" },
                    { "key2", "value2" },
                },
                Title = "Once in a Living Memory",
                Author = "Kayla McMaster",
            };
            await table.InsertOneAsync(row);
            var rowE = new SBook()
            {
                MapColumnIntStr = new Dictionary<int, string>{},
                MapColumnStrStr = new Dictionary<string, string>{},
                Title = "emptyT",
                Author = "emptyA",
            };
            await table.InsertOneAsync(rowE);

            // untyped insertion
            var untypedRow = new Row()
            {
                {
                    "map_column_int_str",
                    new Dictionary<int, string> { { 1, "value1" }, { 2, "value2" } }
                },
                {
                    "map_column_str_str",
                    new Dictionary<string, string>
                    {
                        { "key1", "value1" },
                        { "key2", "value2" },
                    }
                },
                { "title", "UNTY in a Living Memory" },
                { "author", "UNTYP McMaster" },
            };
            await untypedTable.InsertOneAsync(untypedRow);

            var untypedRowE = new Row()
            {
                {
                    "map_column_int_str",
                    new Dictionary<int, string> {}
                },
                {
                    "map_column_str_str",
                    new Dictionary<string, string> {}
                },
                { "title", "UNTYemptyT" },
                { "author", "UNTYemptyA" },
            };

            await untypedTable.InsertOneAsync(untypedRowE);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    // [Fact(Skip = "Unskip manually when needed.")]
    [Fact()]
    public async Task SteoCollectionDictSerializationSanityTest()
    {
        var collName = "test_nestedobj_coll";
        try
        {
            var collection = await fixture.Database.CreateCollectionAsync<SteoNestedCollectionObject>(collName);
            var doc = new SteoNestedCollectionObject
            {
                _id = "one_typed",
                subobject = new SteoNestedCollectionSubobject { sfield = "sf0", ifield = 999 },
                subobject_map = new Dictionary<string, SteoNestedCollectionSubobject> {
                    ["key1"] = new SteoNestedCollectionSubobject { sfield = "sf11", ifield = 11 },
                    ["key2"] = new SteoNestedCollectionSubobject { sfield = "sf12", ifield = 12 }
                }
            };
            await collection.InsertOneAsync(doc);

            // untyped form
            var untypedCollection = fixture.Database.GetCollection(collName);
            var untypedDoc = new Document()
            {
                {"_id", "two_untyped"},
                {
                    "subobject",
                    new Dictionary<string, object>
                    {
                        {"sfield", "sf0"},
                        {"ifield", 999}
                    }
                },
                {
                    "subobject_map",
                    new Dictionary<string, object>
                    {
                        {
                            "key1",
                            new Dictionary<string, object>
                            {
                                {"sfield", "sf11"},
                                {"ifield", 11}
                            }
                        },
                        {
                            "key2",
                            new Dictionary<string, object>
                            {
                                {"sfield", "sf12"},
                                {"ifield", 12}
                            }
                        }
                    }
                }
            };
            await untypedCollection.InsertOneAsync(untypedDoc);
        }
        finally
        {
            await fixture.Database.DropCollectionAsync(collName);
        }
    }

}
