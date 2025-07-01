using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Tables;
using Microsoft.VisualBasic;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("Tables")]
public class TableTests
{
    TablesFixture fixture;

    public TableTests(TablesFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task InsertRows()
    {
        var tableName = "insertRowsTest";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowBook>(tableName);
            var row1 = new RowBook()
            {
                Title = "Computed Wilderness",
                Author = "Ryan Eau",
                NumberOfPages = 432,
                DueDate = DateTime.Now - TimeSpan.FromDays(1),
                Genres = new HashSet<string> { "History", "Biography" }
            };
            var row2 = new RowBook()
            {
                Title = "Desert Peace",
                Author = "Walter Dray",
                NumberOfPages = 355,
                DueDate = DateTime.Now - TimeSpan.FromDays(2),
                Genres = new HashSet<string> { "Fiction" }
            };
            var rows = new List<RowBook> { row1, row2 };
            var result = await table.InsertManyAsync(rows);
            Assert.Equal(rows.Count, result.InsertedCount);
            // Assert.Equal(rows[0].Title, result.PrimaryKeys[0].Title);
            // Assert.Equal(rows[1].Title, result.PrimaryKeys[1].Title);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task InsertManyRows()
    {
        var tableName = "insertManyRowsTest";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowBook>(tableName);
            var rows = new List<RowBook>();
            for (var i = 0; i < 100; i++)
            {
                var row = new RowBook()
                {
                    Title = "Title" + i,
                    Author = "Author" + i,
                    NumberOfPages = 400 + i,
                    DueDate = DateTime.Now - TimeSpan.FromDays(1),
                    Genres = new HashSet<string> { "History", "Biography" }
                };
                rows.Add(row);
            }
            var options = new InsertManyOptions
            {
                ReturnDocumentResponses = true
            };
            var result = await table.InsertManyAsync(rows, options);
            Assert.Equal(rows.Count, result.InsertedCount);
            Assert.Equal(rows.Count, result.DocumentResponses.Count);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task InsertManyRows_TrackResponses()
    {
        string tableName = "insertManyRowsTestTrackResponses";
        try
        {
            var table = await fixture.Database.CreateTableAsync<RowBook>(tableName);
            var rows = new List<RowBook>();
            for (var i = 0; i < 10; i++)
            {
                var row = new RowBook()
                {
                    Title = "Title" + i,
                    Author = "Author" + i,
                    NumberOfPages = 400 + i,
                    DueDate = DateTime.Now - TimeSpan.FromDays(1),
                    Genres = new HashSet<string> { "History", "Biography" }
                };
                rows.Add(row);
            }
            rows.Add(new RowBook()
            {
                Title = "Title1",
                Author = "This Should Update",
                NumberOfPages = 1000,
                DueDate = DateTime.Now,
                Genres = new HashSet<string> { "Fiction" }
            });
            var options = new InsertManyOptions
            {
                ReturnDocumentResponses = true,
                InsertInOrder = true,
                ChunkSize = 2
            };
            var result = await table.InsertManyAsync(rows, options);
            Assert.Equal(rows.Count, result.InsertedCount);
            Assert.Equal(rows.Count, result.DocumentResponses.Count);
            var updatedRow = result.DocumentResponses.FirstOrDefault(r => r.Ids.Contains("Title1"));
            Assert.Equal("OK", updatedRow.Status);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public void LogicalAnd_MongoStyle()
    {
        var table = fixture.SearchTable;
        var builder = Builders<RowBook>.Filter;
        //TODO: AND not working yet via API
        var filter = builder.Gt(so => so.NumberOfPages, 430); // & builder.Gt(so => so.DueDate, DateTime.Now - TimeSpan.FromDays(20));
        var results = table.Find(filter).ToList();
        Assert.Equal(69, results.Count);
    }

    [Fact]
    public void LogicalAnd_AstraStyle()
    {
        var table = fixture.SearchTable;
        var builder = Builders<RowBook>.Filter;
        //TODO: AND not working yet via API
        //var filter = builder.And(builder.Gt(so => so.NumberOfPages, 430), builder.Eq(so => so.DueDate, DateTime.Now - TimeSpan.FromDays(20)));
        var filter = builder.Gt(so => so.NumberOfPages, 430);
        var results = table.Find(filter).ToList();
        Assert.Equal(69, results.Count);
    }

    [Fact]
    public void FindMany_RetrieveAll()
    {
        var table = fixture.SearchTable;
        var results = table.Find().ToList();
        Assert.Equal(102, results.Count);
    }

    [Fact]
    public void FindMany_Vectorize()
    {
        var table = fixture.SearchTable;
        var sorter = Builders<RowBook>.TableSort;
        var sort = sorter.Vectorize(b => b.Author, "Walter Dray");
        var results = table.Find().Sort(sort).ToList();
        Assert.Equal("Desert Peace", results.First().Title);
    }

    [Fact]
    public async Task FindOne_Vectorize()
    {
        var table = fixture.SearchTable;
        var sort = Builders<RowBook>.TableSort;
        var filter = sort.Vectorize(b => b.Author, "Walter Dray");
        var result = await table.FindOneAsync<RowBookWithSimilarity>(null,
            new TableFindOptions<RowBook>() { Sort = filter, IncludeSimilarity = true });
        Assert.Equal("Desert Peace", result.Title);
        //TODO: similarity not being returned currently via API
        //Assert.NotEqual(0, result.Similarity);
    }

    [Fact]
    public async Task FindOne_Sort()
    {
        var table = fixture.SearchTable;
        var sorter = Builders<RowBook>.TableSort;
        var sort = sorter.Descending(b => b.Title);
        var projection = Builders<RowBook>.Projection.Include(b => b.Title);
        var result = await table.FindOneAsync(null, new TableFindOptions<RowBook>() { Sort = sort, Projection = projection });
        Assert.Equal("Title 99", result.Title);
        Assert.Null(result.Author);
    }

    [Fact]
    public void FindOne_Sort_Skip_Exclude()
    {
        var table = fixture.SearchTable;
        var sorter = Builders<RowBook>.TableSort;
        var sort = sorter.Ascending(b => b.Title);
        var projection = Builders<RowBook>.Projection.Exclude(b => b.DueDate);
        var results = table.Find().Sort(sort).Project(projection).Skip(2).Limit(5);
        Assert.Equal(5, results.Count());
        //TODO: not working on API side yet?
        //Assert.Equal("Title 2", results.First().Title);
        Assert.Null(results.First().DueDate);
        Assert.NotEqual(default(int), results.First().NumberOfPages);
    }

    [Fact]
    public async Task Update_Test()
    {
        var table = fixture.SearchTable;
        var filter = Builders<RowBook>.Filter.CompositeKey(new PrimaryKeyFilter<RowBook, string>(x => x.Title, "Title 30"), new PrimaryKeyFilter<RowBook, int>(x => x.NumberOfPages, 430));
        var update = Builders<RowBook>.Update.Set(x => x.Rating, 3.07)
            .Set(x => x.Genres, new HashSet<string> { "SetItem1", "SetItem2" })
            .Unset(x => x.DueDate);
        var result = await table.UpdateOneAsync(filter, update);
        Assert.Equal(1, result.ModifiedCount);
        var updatedDocument = await table.FindOneAsync(filter);
        Assert.Equal(3.07f, updatedDocument.Rating);
        Assert.Equal(new HashSet<string> { "SetItem1", "SetItem2" }, updatedDocument.Genres);
        Assert.Equal(default, updatedDocument.DueDate);
    }

    [Fact]
    public async Task Delete_One()
    {
        var tableName = "testDeleteOne";
        try
        {
            var rows = new List<RowBookSinglePrimaryKey>();
            for (var i = 0; i < 10; i++)
            {
                var row = new RowBookSinglePrimaryKey()
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
                var row = new RowBookSinglePrimaryKey()
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
            var table = await fixture.Database.CreateTableAsync<RowBookSinglePrimaryKey>(tableName);
            await table.CreateIndexAsync(new TableIndex()
            {
                IndexName = "testDeleteOne_number_of_pages_index",
                Definition = new TableIndexDefinition<RowBookSinglePrimaryKey, int>()
                {
                    Column = (b) => b.NumberOfPages
                }
            });
            await table.InsertManyAsync(rows);
            var filter = Builders<RowBookSinglePrimaryKey>.Filter
                .Eq(so => so.Title, "Title 1");
            var findResult = await table.FindOneAsync(filter);
            Assert.Equal("Title 1", findResult.Title);
            var result = await table.DeleteOneAsync(filter);
            var deletedResult = await table.FindOneAsync(filter);
            Assert.Null(deletedResult);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task Delete_Many()
    {
        var tableName = "testDeleteMany";
        try
        {
            var rows = new List<RowBookSinglePrimaryKey>();
            for (var i = 0; i < 10; i++)
            {
                var row = new RowBookSinglePrimaryKey()
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
                var row = new RowBookSinglePrimaryKey()
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
            var table = await fixture.Database.CreateTableAsync<RowBookSinglePrimaryKey>(tableName);
            await table.CreateIndexAsync(new TableIndex()
            {
                IndexName = "testDeleteOne_number_of_pages_index",
                Definition = new TableIndexDefinition<RowBookSinglePrimaryKey, int>()
                {
                    Column = (b) => b.NumberOfPages
                }
            });
            await table.InsertManyAsync(rows);
            var filter = Builders<RowBookSinglePrimaryKey>.Filter
            .Eq(so => so.Title, "Title 1");
            var findResult = table.Find(filter).ToList();
            Assert.Single(findResult);
            var result = await table.DeleteManyAsync(filter);
            var deletedResult = table.Find(filter).ToList();
            Assert.Empty(deletedResult);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task Delete_CompositePrimaryKey()
    {
        var tableName = "deleteCompositePrimaryKey";
        try
        {
            var rows = new List<CompositePrimaryKey>();
            for (var i = 0; i < 10; i++)
            {
                var row = new CompositePrimaryKey()
                {
                    KeyOne = "KeyOne" + i,
                    KeyTwo = "KeyTwo" + i
                };
                rows.Add(row);
            }
            var table = await fixture.Database.CreateTableAsync<CompositePrimaryKey>(tableName);
            await table.InsertManyAsync(rows);
            var filter = Builders<CompositePrimaryKey>.Filter.CompositeKey(
                new PrimaryKeyFilter[] {
                    new PrimaryKeyFilter<CompositePrimaryKey, string>(x => x.KeyOne, "KeyOne3"),
                    new PrimaryKeyFilter<CompositePrimaryKey, string>(x => x.KeyTwo, "KeyTwo3")
                });
            var findResult = table.Find(filter).ToList();
            Assert.Single(findResult);
            var result = await table.DeleteManyAsync(filter);
            var deletedResult = table.Find(filter).ToList();
            Assert.Empty(deletedResult);
        }
        catch (Exception ex)
        {
            var msg = ex.Message;
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task Delete_CompoundPrimaryKey()
    {
        var tableName = "deleteCompoundPrimaryKey";
        try
        {
            var rows = new List<CompoundPrimaryKey>();
            for (var i = 0; i < 10; i++)
            {
                var row = new CompoundPrimaryKey()
                {
                    KeyOne = "KeyOne" + i,
                    KeyTwo = "KeyTwo" + i,
                    SortOneAscending = "SortOneAscending" + i,
                    SortTwoDescending = "SortTwoDescending" + i
                };
                rows.Add(row);
            }
            var table = await fixture.Database.CreateTableAsync<CompoundPrimaryKey>(tableName);
            await table.InsertManyAsync(rows);
            var filterBuilder = Builders<CompoundPrimaryKey>.Filter;
            var filter = filterBuilder.CompoundKey(
                new[] {
                    new PrimaryKeyFilter<CompoundPrimaryKey, string>(x => x.KeyOne, "KeyOne3"),
                    new PrimaryKeyFilter<CompoundPrimaryKey, string>(x => x.KeyTwo, "KeyTwo3")
                },
                new[] {
                    filterBuilder.Eq(x => x.SortOneAscending,"SortOneAscending3"),
                    filterBuilder.Eq(x => x.SortTwoDescending, "SortTwoDescending3")
                });
            var findResult = table.Find(filter).ToList();
            Assert.Single(findResult);
            var result = await table.DeleteManyAsync(filter);
            var deletedResult = table.Find(filter).ToList();
            Assert.Empty(deletedResult);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task Delete_All()
    {
        var tableName = "deleteAllTest";
        try
        {
            var rows = new List<CompoundPrimaryKey>();
            for (var i = 0; i < 10; i++)
            {
                var row = new CompoundPrimaryKey()
                {
                    KeyOne = "KeyOne" + i,
                    KeyTwo = "KeyTwo" + i,
                    SortOneAscending = "SortOneAscending" + i,
                    SortTwoDescending = "SortTwoDescending" + i
                };
                rows.Add(row);
            }
            var table = await fixture.Database.CreateTableAsync<CompoundPrimaryKey>(tableName);
            await table.InsertManyAsync(rows);
            var findResult = table.Find().ToList();
            Assert.Equal(rows.Count, findResult.Count);
            var result = await table.DeleteAllAsync();
            var deletedResult = table.Find().ToList();
            Assert.Empty(deletedResult);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    // same tests on untyped tables
    [Fact]
    public void LogicalAnd_MongoStyle_Untyped()
    {
        var builder = Builders<Row>.Filter;
        var filter = builder.Gte("Id", 10);
        //TODO: AND not working yet via API
        //var filter = builder.Gte("Id", 10) & builder.Gt("IdTwo", "IdTwo_20");

        var results = fixture.UntypedTableSinglePrimaryKey.Find(filter).ToList();
        Assert.Equal(40, results.Count);
        results = fixture.UntypedTableCompositePrimaryKey.Find(filter).ToList();
        Assert.Equal(40, results.Count);
        results = fixture.UntypedTableCompoundPrimaryKey.Find(filter).ToList();
        Assert.Equal(40, results.Count);

    }

    [Fact]
    public void LogicalAnd_AstraStyle_Untyped()
    {
        var builder = Builders<Row>.Filter;
        //TODO: AND not working yet via API
        //var filter = builder.And(builder.Gt("Id", 10), builder.Eq("IdTwo", "IdTwo_20"));
        var filter = builder.Gt("Id", 20);

        var results = fixture.UntypedTableSinglePrimaryKey.Find(filter).ToList();
        Assert.Equal(29, results.Count);
        results = fixture.UntypedTableCompositePrimaryKey.Find(filter).ToList();
        Assert.Equal(29, results.Count);
        results = fixture.UntypedTableCompoundPrimaryKey.Find(filter).ToList();
        Assert.Equal(29, results.Count);
    }

    [Fact]
    public void FindMany_RetrieveAll_Untyped()
    {
        var results = fixture.UntypedTableSinglePrimaryKey.Find().ToList();
        Assert.Equal(50, results.Count);
        results = fixture.UntypedTableCompositePrimaryKey.Find().ToList();
        Assert.Equal(50, results.Count);
        results = fixture.UntypedTableCompoundPrimaryKey.Find().ToList();
        Assert.Equal(50, results.Count);
    }

    [Fact]
    public void FindMany_Vectorize_Untyped()
    {
        var sorter = Builders<Row>.TableSort;
        var sort = sorter.Vectorize("Vectorize", "String To Vectorize 12");
        var results = fixture.UntypedTableSinglePrimaryKey.Find().Sort(sort).ToList();
        Assert.Equal(50, results.Count);
        Assert.Equal("Name_12", results.First()["Name"].ToString());
        results = fixture.UntypedTableCompositePrimaryKey.Find().Sort(sort).ToList();
        Assert.Equal("Name_12", results.First()["Name"].ToString());
        Assert.Equal(50, results.Count);
        results = fixture.UntypedTableCompoundPrimaryKey.Find().Sort(sort).ToList();
        Assert.Equal("Name_12", results.First()["Name"].ToString());
        Assert.Equal(50, results.Count);
    }

    [Fact]
    public async Task FindOne_Vectorize_Untyped()
    {
        var sorter = Builders<Row>.TableSort;
        var sort = sorter.Vectorize("Vectorize", "String To Vectorize 22");
        var results = await fixture.UntypedTableSinglePrimaryKey.FindOneAsync(null,
            new TableFindOptions<Row>() { Sort = sort, IncludeSimilarity = true });
        Assert.Equal("Name_22", results["Name"].ToString());
        results = await fixture.UntypedTableCompositePrimaryKey.FindOneAsync(null,
            new TableFindOptions<Row>() { Sort = sort, IncludeSimilarity = true });
        Assert.Equal("Name_22", results["Name"].ToString());
        results = await fixture.UntypedTableCompoundPrimaryKey.FindOneAsync(null,
            new TableFindOptions<Row>() { Sort = sort, IncludeSimilarity = true });
        Assert.Equal("Name_22", results["Name"].ToString());
    }

    [Fact]
    public async Task FindOne_Sort_Untyped()
    {
        var sorter = Builders<Row>.TableSort;
        var sort = sorter.Descending("Name");
        var projection = Builders<Row>.Projection.Include("Name");
        var result = await fixture.UntypedTableSinglePrimaryKey.FindOneAsync(null, new TableFindOptions<Row>() { Sort = sort, Projection = projection });
        Assert.Equal("Name_9", result["Name"].ToString());
        Assert.False(result.ContainsKey("SortOneAscending"));
        result = await fixture.UntypedTableCompositePrimaryKey.FindOneAsync(null, new TableFindOptions<Row>() { Sort = sort, Projection = projection });
        Assert.Equal("Name_9", result["Name"].ToString());
        Assert.False(result.ContainsKey("SortOneAscending"));
        result = await fixture.UntypedTableCompoundPrimaryKey.FindOneAsync(null, new TableFindOptions<Row>() { Sort = sort, Projection = projection });
        Assert.Equal("Name_9", result["Name"].ToString());
        Assert.False(result.ContainsKey("SortOneAscending"));
    }

    [Fact]
    public void FindOne_Sort_Skip_Exclude_Untyped()
    {
        var sorter = Builders<Row>.TableSort;
        var sort = sorter.Descending("Name");
        var projection = Builders<RowBook>.Projection.Exclude("SortOneAscending");
        var results = fixture.UntypedTableSinglePrimaryKey.Find().Sort(sort).Project(projection).Skip(2).Limit(5).ToList();
        Assert.Equal(5, results.Count());
        Assert.Equal("Name_7", results.First()["Name"].ToString());
        Assert.False(results.First().ContainsKey("SortOneAscending"));
        results = fixture.UntypedTableCompositePrimaryKey.Find().Sort(sort).Project(projection).Skip(2).Limit(5).ToList();
        Assert.Equal(5, results.Count());
        Assert.Equal("Name_7", results.First()["Name"].ToString());
        Assert.False(results.First().ContainsKey("SortOneAscending"));
        results = fixture.UntypedTableCompoundPrimaryKey.Find().Sort(sort).Project(projection).Skip(2).Limit(5).ToList();
        Assert.Equal(5, results.Count());
        Assert.Equal("Name_7", results.First()["Name"].ToString());
        Assert.False(results.First().ContainsKey("SortOneAscending"));
    }

    [Fact]
    public async Task Update_Test_Untyped()
    {
        var filter = Builders<Row>.Filter.Eq("Id", 3);
        var update = Builders<Row>.Update.Set("Name", "Name_3_Updated");
        var result = await fixture.UntypedTableSinglePrimaryKey.UpdateOneAsync(filter, update);
        Assert.Equal(1, result.ModifiedCount);
        var updatedDocument = await fixture.UntypedTableSinglePrimaryKey.FindOneAsync(filter);
        Assert.Equal("Name_3_Updated", updatedDocument["Name"].ToString());

        filter = Builders<Row>.Filter.CompositeKey(
            new PrimaryKeyFilter("Id", 3),
            new PrimaryKeyFilter("IdTwo", "IdTwo_3"));
        result = await fixture.UntypedTableCompositePrimaryKey.UpdateOneAsync(filter, update);
        Assert.Equal(1, result.ModifiedCount);
        updatedDocument = await fixture.UntypedTableCompositePrimaryKey.FindOneAsync(filter);
        Assert.Equal("Name_3_Updated", updatedDocument["Name"].ToString());

        filter = Builders<Row>.Filter.CompoundKey(
                new[] {
                    new PrimaryKeyFilter("Id", 3),
                    new PrimaryKeyFilter("IdTwo", "IdTwo_3"),
                },
                new[] {
                    Builders<Row>.Filter.Eq("SortOneAscending", "SortOneAscending3"),
                    Builders<Row>.Filter.Eq("SortTwoDescending", "SortTwoDescending47")
                });
        result = await fixture.UntypedTableCompoundPrimaryKey.UpdateOneAsync(filter, update);
        Assert.Equal(1, result.ModifiedCount);
        updatedDocument = await fixture.UntypedTableCompoundPrimaryKey.FindOneAsync(filter);
        Assert.Equal("Name_3_Updated", updatedDocument["Name"].ToString());
    }

}

