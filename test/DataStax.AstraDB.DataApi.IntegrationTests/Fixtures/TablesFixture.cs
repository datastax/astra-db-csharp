using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Tables;
using System.ComponentModel.DataAnnotations;
using System.Numerics;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("Tables")]
public class TablesCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<TablesFixture>
{
}

public class TablesFixture : BaseFixture, IAsyncLifetime
{
    public TablesFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "tables")
    {
    }

    public Table<RowBook> SearchTable { get; private set; }
    public Table<RowBook> DeleteTable { get; private set; }
    public Table<Row> UntypedTableSinglePrimaryKey { get; private set; }
    public Table<Row> UntypedTableCompoundPrimaryKey { get; private set; }
    public Table<Row> UntypedTableCompositePrimaryKey { get; private set; }

    public async ValueTask InitializeAsync()
    {
        await CreateSearchTable();
        await CreateDeleteTable();
        await CreateTestTablesNotTyped();
    }

    public async ValueTask DisposeAsync()
    {
        await Database.DropTableAsync(_queryTableName);
        await Database.DropTableAsync(_deleteTableName);
        await Database.DropTableAsync(_untypedSinglePkTableName);
        await Database.DropTableAsync(_untypedCompositePkTableName);
        await Database.DropTableAsync(_untypedCompoundPkTableName);
    }

    private const string _queryTableName = "tableQueryTests";
    private const string _untypedSinglePkTableName = "tableNotTyped_SinglePrimaryKey";
    private const string _untypedCompoundPkTableName = "tableNotTyped_CompoundPrimaryKey";
    private const string _untypedCompositePkTableName = "tableNotTyped_CompositePrimaryKey";
    private const string _deleteTableName = "tableDeleteTests";

    private async Task CreateSearchTable()
    {
        try
        {
            var rows = new List<RowBook>() {
            new RowBook()
            {
                Title = "Computed Wilderness",
                Author = "Ryan Eau",
                NumberOfPages = 22,
                DueDate = DateTime.Now - TimeSpan.FromDays(1),
                Genres = new HashSet<string>() { "History", "Biography" },
                Rating = 1.5f
            },
            new RowBook()
            {
                Title = "Desert Peace",
                Author = "Walter Dray",
                NumberOfPages = 33,
                DueDate = DateTime.Now - TimeSpan.FromDays(2),
                Genres = new HashSet<string>() { "Fiction" },
                Rating = 2.50123f
            }
        };
            for (var i = 0; i < 100; i++)
            {
                var row = new RowBook()
                {
                    Title = "Title " + i,
                    Author = "Author Number" + i,
                    NumberOfPages = 400 + i,
                    DueDate = DateTime.Now - TimeSpan.FromDays(1),
                    Genres = (i % 2 == 0)
                        ? new HashSet<string> { "History", "Biography" }
                        : new HashSet<string> { "Fiction", "History" },
                    Rating = (float)new Random().NextDouble()
                };
                rows.Add(row);
            }
            var table = await Database.CreateTableAsync<RowBook>(_queryTableName);
            // await table.CreateIndexAsync(new TableIndex()
            // {
            //     IndexName = "number_of_pages_index",
            //     Definition = new TableIndexDefinition<RowBook, int>()
            //     {
            //         Column = (b) => b.NumberOfPages
            //     }
            // });
            await table.CreateIndexAsync((b) => b.NumberOfPages);
            // await table.CreateVectorIndexAsync(new TableVectorIndex()
            // {
            //     IndexName = "author_index",
            //     Definition = new TableVectorIndexDefinition<RowBook, object>()
            //     {
            //         Column = (b) => b.Author,
            //         Metric = SimilarityMetric.Cosine,
            //     }
            // });
            await table.CreateIndexAsync((b) => b.Author, Builders.TableIndex.Vector(SimilarityMetric.Cosine));
            // await table.CreateIndexAsync(new TableIndex()
            // {
            //     IndexName = "due_date_index",
            //     Definition = new TableIndexDefinition<RowBook, DateTime?>()
            //     {
            //         Column = (b) => b.DueDate
            //     }
            // });
            await table.CreateIndexAsync((b) => b.DueDate);
            await table.InsertManyAsync(rows);
            SearchTable = table;
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    }

    private async Task CreateTestTablesNotTyped()
    {
        // Create a table with a single primary key
        var createDefinition = new TableDefinition()
                .AddColumn("Id", DataApiType.Int())
                .AddColumn("IdTwo", DataApiType.Text())
                .AddColumn("Name", DataApiType.Text())
                .AddColumn("SortOneAscending", DataApiType.Text())
                .AddColumn("SortTwoDescending", DataApiType.Text())
                .AddColumn("Vectorize", DataApiType.Vectorize(1024, new VectorServiceOptions
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                }))
                .AddColumn("Vector", DataApiType.Vector(384))
                .AddSinglePrimaryKey("Id");

        /*
        CHANGED FROM THIS:
        .AddVectorizeColumn("Vectorize", 1024, new VectorServiceOptions
                        {
                            Provider = "nvidia",
                            ModelName = "NV-Embed-QA"
                        })
        TO THIS:
        .AddColumn("Vectorize", DataApiType.Vectorize(1024, new VectorServiceOptions
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                }))

        CHANGED FROM THIS:
        .AddVectorColumn("Vector", 384)

        TO THIS:
        .AddColumn("Vector", DataApiType.Vector(384))

        */

        UntypedTableSinglePrimaryKey = await Database.CreateTableAsync(_untypedSinglePkTableName, createDefinition);
        await UntypedTableSinglePrimaryKey.CreateIndexAsync("vectorize_index", "Vectorize", Builders.TableIndex.Vector());
        await UntypedTableSinglePrimaryKey.CreateIndexAsync("vector_index", "Vector", Builders.TableIndex.Vector());

        // Create a table with a composite primary key
        createDefinition = new TableDefinition()
                .AddColumn("Id", DataApiType.Int())
                .AddColumn("IdTwo", DataApiType.Text())
                .AddColumn("Name", DataApiType.Text())
                .AddColumn("SortOneAscending", DataApiType.Text())
                .AddColumn("SortTwoDescending", DataApiType.Text())
                .AddColumn("Vectorize", DataApiType.Vectorize(1024, new VectorServiceOptions
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                }))
                .AddColumn("Vector", DataApiType.Vector(384))
                .AddCompositePrimaryKey(new[] { "Id", "IdTwo" });
        UntypedTableCompositePrimaryKey = await Database.CreateTableAsync(_untypedCompositePkTableName, createDefinition);
        await UntypedTableCompositePrimaryKey.CreateIndexAsync("composite_vectorize_index", "Vectorize", Builders.TableIndex.Vector());
        await UntypedTableCompositePrimaryKey.CreateIndexAsync("composite_vector_index", "Vector", Builders.TableIndex.Vector());

        // Create a table with a compound primary key
        createDefinition = new TableDefinition()
                .AddColumn("Id", DataApiType.Int())
                .AddColumn("IdTwo", DataApiType.Text())
                .AddColumn("Name", DataApiType.Text())
                .AddColumn("SortOneAscending", DataApiType.Text())
                .AddColumn("SortTwoDescending", DataApiType.Text())
                .AddColumn("Vectorize", DataApiType.Vectorize(1024, new VectorServiceOptions
                {
                    Provider = "nvidia",
                    ModelName = "NV-Embed-QA"
                }))
                .AddColumn("Vector", DataApiType.Vector(384))
                .AddCompoundPrimaryKey(new[] { "Id", "IdTwo" }, new[]
                {
                    new PrimaryKeySort("SortOneAscending", SortDirection.Ascending),
                    new PrimaryKeySort("SortTwoDescending", SortDirection.Descending)
                });
        UntypedTableCompoundPrimaryKey = await Database.CreateTableAsync(_untypedCompoundPkTableName, createDefinition);
        await UntypedTableCompoundPrimaryKey.CreateIndexAsync("compound_vectorize_index", "Vectorize", Builders.TableIndex.Vector());
        await UntypedTableCompoundPrimaryKey.CreateIndexAsync("compound_vector_index", "Vector", Builders.TableIndex.Vector());

        // Populate untyped tables with sample data
        List<Row> rows = new List<Row>();
        for (var i = 0; i < 50; i++)
        {
            var rowSingle = new Row
            {
                {"Id", i},
                {"IdTwo", "IdTwo_" + i},
                {"Name", "Name_" + i},
                {"SortOneAscending", "SortOne_" + i},
                {"SortTwoDescending", "SortTwo_" + (50 - i)},
                {"Vectorize", "String To Vectorize " + i},
                {"Vector", new double[] { -0.053202216, 0.01422119, 0.007062546, 0.0685742, -0.07858203, 0.010138983, 0.10238025, -0.012096751, 0.09522599, -0.030270875, 0.002181861, -0.064782545, -0.0026875706, 0.0060957014, -0.003964779, -0.030604681, -0.047901124, -0.019261848, -0.059947517, -0.10413115, -0.08611966, 0.03632282, -0.025586247, 0.0017129881, -0.07146128, 0.061734077, 0.017160414, -0.05659205, 0.0248427, -0.07782747, -0.032485314, -0.008684083, -0.011535832, 0.038153064, -0.057013486, -0.053252906, 0.004985692, 0.032392446, 0.0725966, 0.032940567, 0.024707653, -0.083363794, -0.015673108, -0.04811024, -0.003449794, 0.004415103, -0.035913676, -0.051946636, 0.015592655, 0.0035385543, -0.010283442, 0.047748506, -0.040175628, -0.009133693, -0.03460812, -0.03693011, -0.04091714, 0.0176677, -0.00934914, -0.053623937, 0.011154383, 0.016148455, 0.013840816, 0.028249927, 0.04024405, 0.02096661, -0.014487404, -0.0016292258, -0.004891051, 0.012042645, 0.04556029, 0.0130860545, 0.070578784, -0.03086842, 0.030368855, -0.10848343, 0.05554082, -0.017487692, 0.16430159, 0.051410932, -0.027641848, -0.029989198, -0.057063058, 0.056793693, 0.050923523, 0.015136637, -0.0012497514, 0.02384801, -0.06327192, 0.028891006, -0.055418354, -0.03496716, 0.03029518, 0.026919777, -0.08353811, 0.018368296, -0.03516996, -0.08284338, -0.07195326, 0.19801475, 0.016410688, 0.0445346, -0.003741409, -0.038506165, 0.053398475, -0.0034389244, -0.04352991, 0.06336845, -0.013076868, -0.019743098, -0.045236666, 0.020782078, -0.056481004, 0.057446502, 0.055468243, 0.021229729, -0.100917056, -0.03422642, 0.02944804, -0.03325292, 0.028943142, 0.030092051, -0.051856354, 0.008190983, -0.016726157, -0.08435183, 0.011159818, -5.9255234e-33, 0.030620761, -0.085034214, 0.0028181712, -0.041073505, -0.042798948, 0.041067425, 0.029467635, 0.036486518, -0.12122617, 0.013526328, -0.01391842, 0.0312512, -0.021689802, 0.01621624, 0.11224023, -0.006686669, -0.0018879274, 0.05318519, 0.03250415, -0.03782473, -0.046973582, 0.061971873, 0.063630275, 0.050121382, -0.007621213, -0.021432782, -0.03779708, -0.08284233, -0.026234223, 0.036130365, 0.041241154, 0.014499247, 0.073483825, 0.00073006714, -0.081418164, -0.055791657, -0.04209736, -0.096603446, -0.040196676, 0.028519753, 0.12910499, 0.010470544, 0.025057316, 0.01734334, -0.02719573, -0.0049704155, 0.015811851, 0.03439927, -0.044550493, 0.020814221, 0.027571082, -0.014297911, 0.028702551, -0.021064728, 0.008865078, 0.009936881, 0.0029201612, -0.023835903, 0.012977942, 0.06633931, 0.068944834, 0.082585804, 0.008766892, -0.013999867, 0.09115506, -0.122037254, -0.045294352, -0.018009886, -0.022158505, 0.02152304, -0.03885241, -0.019468945, 0.07964807, -0.015691828, 0.06885623, -0.015452343, 0.022757484, 0.025256434, -0.03119467, -0.033447854, -0.021564618, -0.010073421, 0.0055514527, 0.048961196, -0.021559088, 0.06377866, -0.019740583, -0.030324804, 0.0062891715, 0.045206502, -0.045785706, -0.049080465, 0.087099895, 0.027371299, 0.09064848, 3.433169e-33, 0.06266184, 0.028918529, 0.000108557906, 0.09145542, -0.030282516, 0.0048763165, -0.02540525, 0.066567004, -0.034166507, 0.047780972, -0.03424499, 0.007805756, 0.10785121, 0.008996277, 0.0076608267, 0.08868162, 0.0036972803, -0.030516094, 0.02168669, -0.004358315, -0.14477515, 0.011545589, 0.018421879, -0.025913069, -0.05191015, 0.03943329, 0.037553225, -0.0147632975, -0.022263186, -0.048638437, -0.0065658195, -0.039633695, -0.041322067, -0.02844163, 0.010661134, 0.15864708, 0.04770698, -0.04730114, -0.06286664, 0.008440104, 0.059898064, 0.019403962, -0.03227739, 0.11167067, 0.016108502, 0.052688885, -0.017888643, -0.0058668335, 0.052891612, 0.018419184, -0.04730259, -0.014312523, 0.030081172, -0.07333967, -0.012648647, 0.004494484, -0.09500656, 0.018896673, -0.029087285, -0.0051991083, -0.0029317876, 0.069698535, 0.012463835, 0.1219864, -0.10485225, -0.05362739, -0.0128166545, -0.027964052, 0.05004069, -0.07638481, 0.024308309, 0.04531832, -0.029027926, 0.010168302, -0.010628256, 0.030930692, -0.046634875, 0.0045742486, 0.007714686, -0.0063424213, -0.07790265, -0.06532262, -0.047622908, 0.010272605, -0.056622025, -0.011285954, 0.0020759962, 0.06382898, -0.013343911, -0.03008575, -0.009862737, 0.054995734, -0.021704284, -0.05336612, -0.02860762, -1.3317537e-8, -0.028604865, -0.029213138, -0.04298399, -0.019619852, 0.09963344, 0.0694588, -0.030038442, -0.0401437, -0.006644881, 0.026138376, 0.044374008, -0.01637589, -0.06998592, 0.013482148, 0.04653866, -0.0153024765, -0.053351574, 0.039734483, 0.06283631, 0.07712063, -0.050968867, 0.03027798, 0.055424906, 0.0023063482, -0.051206734, -0.035924364, 0.04564326, 0.106056266, -0.08215607, 0.038128633, -0.022592563, 0.14054875, -0.07613521, -0.03006324, -0.0040755956, -0.06966433, 0.07610892, -0.07929878, 0.024970463, 0.03414342, 0.050462823, 0.15209967, -0.020093411, -0.079005316, -0.0006247459, 0.062248245, 0.026453331, -0.12163222, -0.028260367, -0.056446116, -0.09818232, -0.0074948515, 0.027907023, 0.06908376, 0.014955464, 0.005030419, -0.0131421015, -0.047915705, -0.01678274, 0.03665314, 0.1114189, 0.029845735, 0.02391984, 0.110152245 }}
            };
            rows.Add(rowSingle);
        }
        await UntypedTableSinglePrimaryKey.InsertManyAsync(rows);
        await UntypedTableCompositePrimaryKey.InsertManyAsync(rows);
        await UntypedTableCompoundPrimaryKey.InsertManyAsync(rows);
    }

    private async Task CreateDeleteTable()
    {
        var rows = new List<RowBook>();
        for (var i = 0; i < 10; i++)
        {
            var row = new RowBook()
            {
                Title = "Title " + i,
                Author = "Author Number" + i,
                NumberOfPages = 400 + i,
                DueDate = DateTime.Now - TimeSpan.FromDays(1),
                Genres = (i % 2 == 0)
                    ? new HashSet<string> { "History", "Biography" }
                    : new HashSet<string> { "Fiction", "History" },
                Rating = (float)new Random().NextDouble()
            };
            rows.Add(row);
        }
        for (var i = 10; i < 20; i++)
        {
            var row = new RowBook()
            {
                Title = "Title " + i,
                Author = "AuthorDeleteMe",
                NumberOfPages = 22,
                DueDate = DateTime.Now - TimeSpan.FromDays(1),
                Genres = (i % 2 == 0)
                    ? new HashSet<string> { "History", "Biography" }
                    : new HashSet<string> { "Fiction", "History" },
                Rating = (float)new Random().NextDouble()
            };
            rows.Add(row);
        }
        var table = await Database.CreateTableAsync<RowBook>(_deleteTableName);
        await table.CreateIndexAsync("delete_table_number_of_pages_index", "NumberOfPages");
        await table.CreateIndexAsync("delete_table_author_vector_index", (b) => b.Author, Builders.TableIndex.Vector());
        await table.CreateIndexAsync("delete_table_due_date_index", (b) => b.DueDate);

        await table.InsertManyAsync(rows);
        DeleteTable = table;
    }

}
