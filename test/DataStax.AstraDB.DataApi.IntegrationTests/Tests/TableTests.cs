/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.Tables;
using DataStax.AstraDB.DataApi.Utils;
using Microsoft.VisualBasic;
using System.Net;
using System.Text;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("Tables")]
public class TableTests
{
    TablesFixture fixture;

    public TableTests(AssemblyFixture assemblyFixture, TablesFixture fixture)
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
                DueDate = DateTime.UtcNow - TimeSpan.FromDays(1),
                Genres = new HashSet<string> { "History", "Biography" }
            };
            var row2 = new RowBook()
            {
                Title = "Desert Peace",
                Author = "Walter Dray",
                NumberOfPages = 355,
                DueDate = DateTime.UtcNow - TimeSpan.FromDays(2),
                Genres = new HashSet<string> { "Fiction" }
            };
            var rows = new List<RowBook> { row1, row2 };
            var result = await table.InsertManyAsync(rows);
            Assert.Equal(rows.Count, result.InsertedCount);
            Assert.Equal(rows[0].Title, result.InsertedIdTuples[0][0]);
            Assert.Equal(rows[0].NumberOfPages, result.InsertedIdTuples[0][1]);
            Assert.Equal(rows[1].Title, result.InsertedIdTuples[1][0]);
            Assert.Equal(rows[1].NumberOfPages, result.InsertedIdTuples[1][1]);
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task InsertAllTypesInPKRows()
    {
        try
        {
            var tableDefinition = new TableDefinition()
                // ALL PK COLUMNS HERE
                .AddColumn("TheAscii", DataAPIType.Ascii())
                .AddColumn("TheBigInt", DataAPIType.BigInt())
                .AddColumn("TheDateOnly", DataAPIType.Date())
                .AddColumn("TheBoolean", DataAPIType.Boolean())
                .AddColumn("TheBlob", DataAPIType.Blob())
                .AddColumn("TheDecimal", DataAPIType.Decimal())
                .AddColumn("TheDouble", DataAPIType.Double())
                .AddColumn("TheFloat", DataAPIType.Float())
                .AddColumn("TheInet", DataAPIType.Inet())
                .AddColumn("TheInt", DataAPIType.Int())
                .AddColumn("TheSmallint", DataAPIType.SmallInt())
                .AddColumn("TheText", DataAPIType.Text())
                .AddColumn("TheTime", DataAPIType.Time())
                .AddColumn("TheTimestamp", DataAPIType.Timestamp())
                .AddColumn("TheTinyint", DataAPIType.TinyInt())
                .AddColumn("TheUuid", DataAPIType.Uuid())
                .AddColumn("TheVarint", DataAPIType.VarInt())
                .AddColumn("TheVector", DataAPIType.Vector(2))
                .AddColumn("TheNonPKValue", DataAPIType.Text())
                .AddCompositePrimaryKey(new string[] {
                    // ALL PK COLUMN NAMES HERE
                    "TheAscii",
                    "TheBigInt",
                    "TheDateOnly",
                    "TheBoolean",
                    "TheBlob",
                    "TheDecimal",
                    "TheDouble",
                    "TheFloat",
                    "TheInet",
                    "TheInt",
                    "TheSmallint",
                    "TheText",
                    "TheTime",
                    "TheTimestamp",
                    "TheTinyint",
                    "TheUuid",
                    "TheVarint",
                    "TheVector",
                });
            var table = await fixture.Database.CreateTableAsync<RowWithAllTypesInPK>(tableDefinition);

            // ALL PK VALUE VAR NAMES HERE
            var theAscii = "my ascii";
            var theBigInt = 123456789L;
            var theDateOnly = DateOnly.Parse("2026-07-16");
            var theBoolean = true;
            var theBlob = Encoding.ASCII.GetBytes("my blob.");
            var theDecimal = 12.3456m;
            var theDouble = 12.3456d;
            var theFloat = 12.34f;
            var theInet = IPAddress.Parse("10.1.1.10");
            var theInt = 4096;
            var theSmallint = (short)(12);
            var theText = "my text";
            var theTime = TimeOnly.Parse("11:22:33.456");
            var theTimestamp = new DateTime(
                2026, 7, 17, 12, 34, 56, 789,
                DateTimeKind.Utc
            );
            var theTinyint = (sbyte)(16);
            var theUuid = Guid.Parse("63f4f459-1bba-48ee-9151-197cd545a911");
            var theVarint = 81928192m;
            var theVector = new float[] { 0.01f, -0.02f };
            var row1 = new RowWithAllTypesInPK()
            {
                // ALL PK COLUMN VALUE SETTINGS TO ROW HERE
                TheAscii = theAscii,
                TheBigInt = theBigInt,
                TheDateOnly = theDateOnly,
                TheBoolean = theBoolean,
                TheBlob = theBlob,
                TheDecimal = theDecimal,
                TheDouble = theDouble,
                TheFloat = theFloat,
                TheInet = theInet,
                TheInt = theInt,
                TheSmallint = theSmallint,
                TheText = theText,
                TheTime = theTime,
                TheTimestamp = theTimestamp,
                TheTinyint = theTinyint,
                TheUuid = theUuid,
                TheVarint = theVarint,
                TheVector = theVector,
                TheNonPKValue = "Test Row",
            };
            var rows = new List<RowWithAllTypesInPK> { row1 };
            var result = await table.InsertManyAsync(rows);
            Assert.Equal(rows.Count, result.InsertedCount);
            Assert.Equal(rows[0].TheAscii, result.InsertedIdTuples[0][0]);
            Assert.Equal(rows[0].TheBigInt, result.InsertedIdTuples[0][1]);
            Assert.Equal(rows[0].TheDateOnly, result.InsertedIdTuples[0][2]);
            Assert.Equal(rows[0].TheBoolean, result.InsertedIdTuples[0][3]);
            Assert.Equal(rows[0].TheBlob, result.InsertedIdTuples[0][4]);
            Assert.Equal(rows[0].TheDecimal, result.InsertedIdTuples[0][5]);
            Assert.Equal(rows[0].TheDouble, result.InsertedIdTuples[0][6]);
            Assert.Equal(rows[0].TheFloat, result.InsertedIdTuples[0][7]);
            Assert.Equal(rows[0].TheInet, result.InsertedIdTuples[0][8]);
            Assert.Equal(rows[0].TheInt, result.InsertedIdTuples[0][9]);
            Assert.Equal(rows[0].TheSmallint, result.InsertedIdTuples[0][10]);
            Assert.Equal(rows[0].TheText, result.InsertedIdTuples[0][11]);
            Assert.Equal(rows[0].TheTime, result.InsertedIdTuples[0][12]);
            Assert.Equal(rows[0].TheTimestamp, result.InsertedIdTuples[0][13]);
            Assert.Equal(rows[0].TheTinyint, result.InsertedIdTuples[0][14]);
            Assert.Equal(rows[0].TheUuid, result.InsertedIdTuples[0][15]);
            Assert.Equal(rows[0].TheVarint, result.InsertedIdTuples[0][16]);
            Assert.Equal(rows[0].TheVector, result.InsertedIdTuples[0][17]);
        }
        finally
        {
            await fixture.Database.DropTableAsync<RowWithAllTypesInPK>();
        }
    }

    [Fact]
    public async Task InsertAllTypesOutsidePKRowsTyped()
    {
        try
        {
            var tableDefinition = new TableDefinition()
                .AddColumn("ThePK", DataAPIType.Text())
                // ALL NON-PK COLUMNS HERE
                .AddColumn("TheAscii", DataAPIType.Ascii())
                .AddColumn("TheBigInt", DataAPIType.BigInt())
                .AddColumn("TheDateOnly", DataAPIType.Date())
                .AddColumn("TheBoolean", DataAPIType.Boolean())
                .AddColumn("TheBlob", DataAPIType.Blob())
                .AddColumn("TheDecimal", DataAPIType.Decimal())
                .AddColumn("TheDouble", DataAPIType.Double())
                .AddColumn("TheFloat", DataAPIType.Float())
                .AddColumn("TheInet", DataAPIType.Inet())
                .AddColumn("TheInt", DataAPIType.Int())
                .AddColumn("TheSmallint", DataAPIType.SmallInt())
                .AddColumn("TheText", DataAPIType.Text())
                .AddColumn("TheTime", DataAPIType.Time())
                .AddColumn("TheTimestamp", DataAPIType.Timestamp())
                .AddColumn("TheTinyint", DataAPIType.TinyInt())
                .AddColumn("TheUuid", DataAPIType.Uuid())
                .AddColumn("TheVarint", DataAPIType.VarInt())
                .AddColumn("TheVector", DataAPIType.Vector(2))
                .AddCompositePrimaryKey(new string[] { "ThePK" }
            );
            var table = await fixture.Database.CreateTableAsync<RowWithAllTypesOutsidePK>(tableDefinition);

            // ALL NON-PK VALUE VAR NAMES HERE
            var theAscii = "my ascii";
            var theBigInt = 123456789L;
            var theDateOnly = DateOnly.Parse("2026-07-16");
            var theBoolean = true;
            var theBlob = Encoding.ASCII.GetBytes("my blob.");
            var theDecimal = 12.3456m;
            var theDouble = 12.3456d;
            var theFloat = 12.34f;
            var theInet = IPAddress.Parse("10.1.1.10");
            var theInt = 4096;
            var theSmallint = (short)(12);
            var theText = "my text";
            var theTime = TimeOnly.Parse("11:22:33.456");
            var theTimestamp = new DateTime(
                2026, 7, 17, 12, 34, 56, 789,
                DateTimeKind.Utc
            );
            var theTinyint = (sbyte)(16);
            var theUuid = Guid.Parse("63f4f459-1bba-48ee-9151-197cd545a911");
            var theVarint = 81928192m;
            var theVector = new float[] { 0.01f, -0.02f };
            var row1 = new RowWithAllTypesOutsidePK()
            {
                ThePK = "row with values",
                // ALL NON-PK COLUMN VALUE SETTINGS TO ROW HERE
                TheAscii = theAscii,
                TheBigInt = theBigInt,
                TheDateOnly = theDateOnly,
                TheBoolean = theBoolean,
                TheBlob = theBlob,
                TheDecimal = theDecimal,
                TheDouble = theDouble,
                TheFloat = theFloat,
                TheInet = theInet,
                TheInt = theInt,
                TheSmallint = theSmallint,
                TheText = theText,
                TheTime = theTime,
                TheTimestamp = theTimestamp,
                TheTinyint = theTinyint,
                TheUuid = theUuid,
                TheVarint = theVarint,
                TheVector = theVector,
            };
            var rowN = new RowWithAllTypesOutsidePK()
            {
                ThePK = "row with nulls",
                // ALL NON-PK COLUMN VALUE SETTINGS TO ROW HERE
                TheAscii = null,
                TheBigInt = null,
                TheDateOnly = null,
                TheBoolean = null,
                TheBlob = null,
                TheDecimal = null,
                TheDouble = null,
                TheFloat = null,
                TheInet = null,
                TheInt = null,
                TheSmallint = null,
                TheText = null,
                TheTime = null,
                TheTimestamp = null,
                TheTinyint = null,
                TheUuid = null,
                TheVarint = null,
                TheVector = null,
            };
            var rows = new List<RowWithAllTypesOutsidePK> { row1, rowN };
            var result = await table.InsertManyAsync(rows);
            Assert.Equal(rows.Count, result.InsertedCount);
            Assert.Equal(rows[0].ThePK, result.InsertedIdTuples[0][0]);

            var builder = Builders<RowWithAllTypesOutsidePK>.TableFilter;

            // read back and verify everything matches. 1: row with values
            var filter1 = builder.Eq(r => r.ThePK, "row with values");
            var readRow1 = await table.FindOneAsync(filter1);
            Assert.NotNull(readRow1);
            Assert.Equal(row1.ThePK, readRow1.ThePK);
            Assert.Equal(row1.TheAscii, readRow1.TheAscii);
            Assert.Equal(row1.TheBigInt, readRow1.TheBigInt);
            Assert.Equal(row1.TheDateOnly, readRow1.TheDateOnly);
            Assert.Equal(row1.TheBoolean, readRow1.TheBoolean);
            Assert.Equal(row1.TheBlob, readRow1.TheBlob);
            Assert.Equal(row1.TheDecimal, readRow1.TheDecimal);
            Assert.Equal(row1.TheDouble, readRow1.TheDouble);
            Assert.Equal(row1.TheFloat, readRow1.TheFloat);
            Assert.Equal(row1.TheInet, readRow1.TheInet);
            Assert.Equal(row1.TheInt, readRow1.TheInt);
            Assert.Equal(row1.TheSmallint, readRow1.TheSmallint);
            Assert.Equal(row1.TheText, readRow1.TheText);
            Assert.Equal(row1.TheTime, readRow1.TheTime);
            Assert.Equal(row1.TheTimestamp, readRow1.TheTimestamp);
            Assert.Equal(row1.TheTinyint, readRow1.TheTinyint);
            Assert.Equal(row1.TheUuid, readRow1.TheUuid);
            Assert.Equal(row1.TheVarint, readRow1.TheVarint);
            Assert.Equal(row1.TheVector, readRow1.TheVector);

            // read back and verify everything matches. 2: row with nulls
            var filterN = builder.Eq(r => r.ThePK, "row with nulls");
            var readRowN = await table.FindOneAsync(filterN);
            Assert.NotNull(readRowN);
            Assert.Equal(rowN.ThePK, readRowN.ThePK);
            Assert.Equal(rowN.TheAscii, readRowN.TheAscii);
            Assert.Equal(rowN.TheBigInt, readRowN.TheBigInt);
            Assert.Equal(rowN.TheDateOnly, readRowN.TheDateOnly);
            Assert.Equal(rowN.TheBoolean, readRowN.TheBoolean);
            Assert.Equal(rowN.TheBlob, readRowN.TheBlob);
            Assert.Equal(rowN.TheDecimal, readRowN.TheDecimal);
            Assert.Equal(rowN.TheDouble, readRowN.TheDouble);
            Assert.Equal(rowN.TheFloat, readRowN.TheFloat);
            Assert.Equal(rowN.TheInet, readRowN.TheInet);
            Assert.Equal(rowN.TheInt, readRowN.TheInt);
            Assert.Equal(rowN.TheSmallint, readRowN.TheSmallint);
            Assert.Equal(rowN.TheText, readRowN.TheText);
            Assert.Equal(rowN.TheTime, readRowN.TheTime);
            Assert.Equal(rowN.TheTimestamp, readRowN.TheTimestamp);
            Assert.Equal(rowN.TheTinyint, readRowN.TheTinyint);
            Assert.Equal(rowN.TheUuid, readRowN.TheUuid);
            Assert.Equal(rowN.TheVarint, readRowN.TheVarint);
            Assert.Equal(rowN.TheVector, readRowN.TheVector);
        }
        finally
        {
            await fixture.Database.DropTableAsync<RowWithAllTypesOutsidePK>();
        }
    }

    [Fact]
    public async Task InsertAllTypesOutsidePKRowsUntyped()
    {
        try
        {
            var tableDefinition = new TableDefinition()
                .AddColumn("ThePK", DataAPIType.Text())
                // ALL NON-PK COLUMNS HERE
                .AddColumn("TheAscii", DataAPIType.Ascii())
                .AddColumn("TheBigInt", DataAPIType.BigInt())
                .AddColumn("TheDateOnly", DataAPIType.Date())
                .AddColumn("TheBoolean", DataAPIType.Boolean())
                .AddColumn("TheBlob", DataAPIType.Blob())
                .AddColumn("TheDecimal", DataAPIType.Decimal())
                .AddColumn("TheDouble", DataAPIType.Double())
                .AddColumn("TheFloat", DataAPIType.Float())
                .AddColumn("TheInet", DataAPIType.Inet())
                .AddColumn("TheInt", DataAPIType.Int())
                .AddColumn("TheSmallint", DataAPIType.SmallInt())
                .AddColumn("TheText", DataAPIType.Text())
                .AddColumn("TheTime", DataAPIType.Time())
                .AddColumn("TheTimestamp", DataAPIType.Timestamp())
                .AddColumn("TheTinyint", DataAPIType.TinyInt())
                .AddColumn("TheUuid", DataAPIType.Uuid())
                .AddColumn("TheVarint", DataAPIType.VarInt())
                .AddColumn("TheVector", DataAPIType.Vector(2))
                .AddCompositePrimaryKey(new string[] { "ThePK" }
            );
            var table = await fixture.Database.CreateTableAsync(
                "insertAllTypesOutsidePKRowsUntypedTest",
                tableDefinition
            );

            // ALL NON-PK VALUE VAR NAMES HERE
            var theAscii = "my ascii";
            var theBigInt = 123456789L;
            var theDateOnly = DateOnly.Parse("2026-07-16");
            var theBoolean = true;
            var theBlob = Encoding.ASCII.GetBytes("my blob.");
            var theDecimal = 12.3456m;
            var theDouble = 12.3456d;
            var theFloat = 12.34f;
            var theInet = IPAddress.Parse("10.1.1.10");
            var theInt = 4096;
            var theSmallint = (short)(12);
            var theText = "my text";
            var theTime = TimeOnly.Parse("11:22:33.456");
            var theTimestamp = new DateTime(
                2026, 7, 17, 12, 34, 56, 789,
                DateTimeKind.Utc
            );
            var theTinyint = (sbyte)(16);
            var theUuid = Guid.Parse("63f4f459-1bba-48ee-9151-197cd545a911");
            var theVarint = 81928192m;
            var theVector = new float[] { 0.01f, -0.02f };
            var row1 = new Row()
            {
                { "ThePK", "row with values" },
                { "TheAscii", theAscii },
                { "TheBigInt", theBigInt },
                { "TheDateOnly", theDateOnly },
                { "TheBoolean", theBoolean },
                { "TheBlob", theBlob },
                { "TheDecimal", theDecimal },
                { "TheDouble", theDouble },
                { "TheFloat", theFloat },
                { "TheInet", theInet },
                { "TheInt", theInt },
                { "TheSmallint", theSmallint },
                { "TheText", theText },
                { "TheTime", theTime },
                { "TheTimestamp", theTimestamp },
                { "TheTinyint", theTinyint },
                { "TheUuid", theUuid },
                { "TheVarint", theVarint },
                { "TheVector", theVector },
            };
            var rowN = new Row()
            {
                { "ThePK", "row with nulls" },
                { "TheAscii", null },
                { "TheBigInt", null },
                { "TheDateOnly", null },
                { "TheBoolean", null },
                { "TheBlob", null },
                { "TheDecimal", null },
                { "TheDouble", null },
                { "TheFloat", null },
                { "TheInet", null },
                { "TheInt", null },
                { "TheSmallint", null },
                { "TheText", null },
                { "TheTime", null },
                { "TheTimestamp", null },
                { "TheTinyint", null },
                { "TheUuid", null },
                { "TheVarint", null },
                { "TheVector", null },
            };
            var rows = new List<Row> { row1, rowN };
            var result = await table.InsertManyAsync(rows);
            Assert.Equal(rows.Count, result.InsertedCount);
            Assert.Equal(rows[0]["ThePK"], result.InsertedIdTuples[0][0]);

            var builder = Builders<Row>.TableFilter;

            // read back and verify everything matches. 1: row with values
            var filter1 = builder.Eq("ThePK", "row with values");
            var readRow1 = await table.FindOneAsync(filter1);
            Assert.NotNull(readRow1);
            Assert.Equal(row1["ThePK"], readRow1["ThePK"]);
            Assert.Equal(row1["TheAscii"], readRow1["TheAscii"]);
            Assert.Equal(row1["TheBigInt"], readRow1["TheBigInt"]);
            Assert.Equal(row1["TheDateOnly"], readRow1["TheDateOnly"]);
            Assert.Equal(row1["TheBoolean"], readRow1["TheBoolean"]);
            Assert.Equal(row1["TheBlob"], readRow1["TheBlob"]);
            Assert.Equal(row1["TheDecimal"], readRow1["TheDecimal"]);
            Assert.Equal(row1["TheDouble"], readRow1["TheDouble"]);
            Assert.Equal(row1["TheFloat"], readRow1["TheFloat"]);
            Assert.Equal(row1["TheInet"], readRow1["TheInet"]);
            Assert.Equal(row1["TheInt"], readRow1["TheInt"]);
            Assert.Equal(row1["TheSmallint"], readRow1["TheSmallint"]);
            Assert.Equal(row1["TheText"], readRow1["TheText"]);
            Assert.Equal(row1["TheTime"], readRow1["TheTime"]);
            Assert.Equal(row1["TheTimestamp"], readRow1["TheTimestamp"]);
            Assert.Equal(row1["TheTinyint"], readRow1["TheTinyint"]);
            Assert.Equal(row1["TheUuid"], readRow1["TheUuid"]);
            Assert.Equal(row1["TheVarint"], readRow1["TheVarint"]);
            // Must tolerate float-precision rounding for "TheVector":
            float[] row1Vec = (float [])row1["TheVector"];
            float[] readRow1Vec = ((System.Text.Json.JsonElement)readRow1["TheVector"])
                .EnumerateArray()
                .Select(e => e.GetSingle())
                .ToArray();
            Assert.True(Math.Abs(row1Vec[0] - readRow1Vec[0]) < 1e-6);
            Assert.True(Math.Abs(row1Vec[1] - readRow1Vec[1]) < 1e-6);

            // read back and verify everything matches. 2: row with nulls
            var filterN = builder.Eq("ThePK", "row with nulls");
            var readRowN = await table.FindOneAsync(filterN);
            Assert.NotNull(readRowN);
            Assert.Equal(rowN["ThePK"], readRowN["ThePK"]);
            Assert.Equal(rowN["TheAscii"], readRowN["TheAscii"]);
            Assert.Equal(rowN["TheBigInt"], readRowN["TheBigInt"]);
            Assert.Equal(rowN["TheDateOnly"], readRowN["TheDateOnly"]);
            Assert.Equal(rowN["TheBoolean"], readRowN["TheBoolean"]);
            Assert.Equal(rowN["TheBlob"], readRowN["TheBlob"]);
            Assert.Equal(rowN["TheDecimal"], readRowN["TheDecimal"]);
            Assert.Equal(rowN["TheDouble"], readRowN["TheDouble"]);
            Assert.Equal(rowN["TheFloat"], readRowN["TheFloat"]);
            Assert.Equal(rowN["TheInet"], readRowN["TheInet"]);
            Assert.Equal(rowN["TheInt"], readRowN["TheInt"]);
            Assert.Equal(rowN["TheSmallint"], readRowN["TheSmallint"]);
            Assert.Equal(rowN["TheText"], readRowN["TheText"]);
            Assert.Equal(rowN["TheTime"], readRowN["TheTime"]);
            Assert.Equal(rowN["TheTimestamp"], readRowN["TheTimestamp"]);
            Assert.Equal(rowN["TheTinyint"], readRowN["TheTinyint"]);
            Assert.Equal(rowN["TheUuid"], readRowN["TheUuid"]);
            Assert.Equal(rowN["TheVarint"], readRowN["TheVarint"]);
            Assert.Equal(rowN["TheVector"], readRowN["TheVector"]);
        }
        finally
        {
            await fixture.Database.DropTableAsync("insertAllTypesOutsidePKRowsUntypedTest");
        }
    }

    /* IMPORTANT: run these two tests after CQL manual setup (and cleanup afterwards)
        -- Manual test setup:
        CREATE TABLE table_with_timeuuid (id TEXT PRIMARY KEY, the_tuid TIMEUUID);
        INSERT INTO table_with_timeuuid (id , the_tuid ) VALUES ('0', 183f8300-8458-11f1-8202-41b65ed1b8e7);

        -- Verify with:
        -- SELECT * FROM table_with_timeuuid;

        -- Finally execute this:
        -- DROP TABLE table_with_timeuuid;
    */
    [Fact(Skip="Requires manual CQL setup, this test to be launched manually.")]
    public async Task ReadTimeUUIDTyped()
    {
        var table = fixture.Database.GetTable<RowWithTimeUUID>("table_with_timeuuid");
        var readRow = await table.FindOneAsync();
        Assert.NotNull(readRow);
        Assert.Equal("0", readRow.id);
        Assert.Equal(new TimeUuid(new Guid("183f8300-8458-11f1-8202-41b65ed1b8e7")), readRow.the_tuid);
    }

    [Fact(Skip="Requires manual CQL setup, this test to be launched manually.")]
    public async Task ReadTimeUUIDUntyped()
    {
        var table = fixture.Database.GetTable("table_with_timeuuid");
        var readRow = await table.FindOneAsync();
        Assert.NotNull(readRow);
        Assert.Equal("0", readRow["id"]);
        Assert.Equal(new TimeUuid(new Guid("183f8300-8458-11f1-8202-41b65ed1b8e7")), readRow["the_tuid"]);
    }

    [Fact]
    [SkipWhenNotAstra] // TODO why is this not throwing on HCD in CI? It throws just fine when running on HCD locally
    public async Task InsertManyCommandOptions()
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
                DueDate = DateTime.UtcNow - TimeSpan.FromDays(1),
                Genres = new HashSet<string> { "History", "Biography" }
            };
            var row2 = new RowBook()
            {
                Title = "Desert Peace",
                Author = "Walter Dray",
                NumberOfPages = 355,
                DueDate = DateTime.UtcNow - TimeSpan.FromDays(2),
                Genres = new HashSet<string> { "Fiction" }
            };
            var rows = new List<RowBook> { row1, row2 };

            // passing generic command options:
            await Assert.ThrowsAsync<BulkOperationException<TableInsertManyResult>>( async () =>
            {
                await table.InsertManyAsync(
                    rows, new TableInsertManyOptions() { Token = "blibbli" });
            });
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
                    DueDate = DateTime.UtcNow - TimeSpan.FromDays(1),
                    Genres = new HashSet<string> { "History", "Biography" }
                };
                rows.Add(row);
            }
            var result = await table.InsertManyAsync(rows);
            Assert.Equal(rows.Count, result.InsertedCount);
            Assert.Equal(rows.Count, result.InsertedIdTuples.Count);
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
                    DueDate = DateTime.UtcNow - TimeSpan.FromDays(1),
                    Genres = new HashSet<string> { "History", "Biography" }
                };
                rows.Add(row);
            }
            rows.Add(new RowBook()
            {
                Title = "Title1",
                Author = "This Should Update",
                NumberOfPages = 1000,
                DueDate = DateTime.UtcNow,
                Genres = new HashSet<string> { "Fiction" }
            });
            var options = new TableInsertManyOptions
            {
                Ordered = true,
                ChunkSize = 2
            };
            var result = await table.InsertManyAsync(rows, options);
            Assert.Equal(rows.Count, result.InsertedCount);
            Assert.Equal(rows.Count, result.InsertedIdTuples.Count);
            Assert.Contains(result.InsertedIdTuples, r => r.Contains("Title1") && r.Contains(1000));
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
        var builder = Builders<RowBook>.TableFilter;
        var filter = builder.Gt(so => so.NumberOfPages, 430) & builder.Gt(so => so.DueDate, DateTime.UtcNow - TimeSpan.FromDays(20));
        var results = table.Find(filter).ToList();
        Assert.Equal(69, results.Count);
    }

    [Fact]
    public void LogicalAnd_AstraStyle()
    {
        var table = fixture.SearchTable;
        var builder = Builders<RowBook>.TableFilter;
        var filter = builder.And(
            builder.Gt(so => so.NumberOfPages, 430),
            builder.Gt(so => so.DueDate, DateTime.UtcNow - TimeSpan.FromDays(20)));
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

    [SkipWhenNotAstra]
    [Fact]
    public void FindMany_Vectorize()
    {
        var table = fixture.SearchTableVectorize;
        var sorter = Builders<RowBookVectorize>.TableSort;
        var sort = sorter.Vectorize(b => b.Author, "Walter Dray");
        var results = table.Find().Sort(sort).ToList();
        Assert.Equal("Desert Peace", results.First().Title);
    }

    [Fact]
    public async Task FindOne_Sort()
    {
        var table = fixture.SearchTable;
        var sorter = Builders<RowBook>.TableSort;
        var sort = sorter.Descending(b => b.Title);
        var projection = Builders<RowBook>.Projection.Include(b => b.Title);
        var result = await table.FindOneAsync(null, new TableFindOneOptions<RowBook>() { Sort = sort, Projection = projection });
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
        var results = table.Find().Sort(sort).Project(projection).Skip(2).Limit(5).ToList();
        Assert.Equal(5, results.Count());
        // due to 'Computed...' and 'Desert...', the third is 'Title 0' here
        Assert.Equal("Title 0", results.First().Title);
        Assert.Null(results.First().DueDate);
        Assert.NotEqual(default(int), results.First().NumberOfPages);
    }

    [Fact]
    public void FindOne_Find_SortByVector()
    {
        var table = fixture.SearchTableWithVector4;
        var sorter = Builders<RowWithVector4>.TableSort;
        var sort = sorter.Vector(r => r.Vector, new float[] {0.0f, 0.0f, 0.01f, 0.99f});
        var projection = Builders<RowWithVector4>.Projection.Exclude(r => r.Vector);

        var findResults = table.Find().Sort(sort).Project(projection).Limit(1);
        Assert.Equal(1, findResults.Count());
        findResults.Rewind();
        Assert.Equal("last_component", findResults.First().Id);
        
        var findOneResult = table.FindOne(
            null,
            new TableFindOneOptions<RowWithVector4>() {
                Sort = sort, Projection = projection
            }
        );
        Assert.Equal("last_component", findOneResult.Id);
    }

    [Fact]
    public async Task Update_Test()
    {
        var table = fixture.SearchTable;
        var filter = Builders<RowBook>.TableFilter.And(
            new[] {
                Builders<RowBook>.TableFilter.Eq(x => x.Title, "Title 30"),
                Builders<RowBook>.TableFilter.Eq(x => x.NumberOfPages, 430)
            });

        var update = Builders<RowBook>.TableUpdate.Set(x => x.Rating, 3.07)
            .Set(x => x.Genres, new HashSet<string> { "SetItem1", "SetItem2" })
            .Unset(x => x.DueDate);
        await table.UpdateOneAsync(filter, update);
        var updatedDocument = await table.FindOneAsync(filter);
        Assert.Equal(3.07f, updatedDocument.Rating);
        Assert.Equal(new HashSet<string> { "SetItem1", "SetItem2" }, updatedDocument.Genres);
        Assert.Equal(default, updatedDocument.DueDate);
    }

    [Fact]
    public async Task Update_Test_PrimaryKeyFilterBuilder()
    {
        var table = fixture.SearchTable;
        var filter = Builders<RowBook>.TableFilter.And(
            new[] {
                Builders<RowBook>.TableFilter.Eq(x => x.Title, "Title 30"),
                Builders<RowBook>.TableFilter.Eq(x => x.NumberOfPages, 430)
            });
        var update = Builders<RowBook>.TableUpdate.Set(x => x.Rating, 3.07)
            .Set(x => x.Genres, new HashSet<string> { "SetItem1", "SetItem2" })
            .Unset(x => x.DueDate);
        await table.UpdateOneAsync(filter, update);
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
                    DueDate = DateTime.UtcNow - TimeSpan.FromDays(1),
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
                    DueDate = DateTime.UtcNow - TimeSpan.FromDays(1),
                    Genres = (i % 2 == 0)
                        ? new HashSet<string> { "History", "Biography" }
                        : new HashSet<string> { "Fiction", "History" },
                    Rating = (float)new Random().NextDouble()
                };
                rows.Add(row);
            }
            var table = await fixture.Database.CreateTableAsync<RowBookSinglePrimaryKey>(tableName);
            await table.CreateIndexAsync("testDeleteOne_number_of_pages_index", (b) => b.NumberOfPages);
            await table.InsertManyAsync(rows);
            var filter = Builders<RowBookSinglePrimaryKey>.TableFilter
                .Eq(so => so.Title, "Title 1");
            var findResult = await table.FindOneAsync(filter);
            Assert.Equal("Title 1", findResult.Title);
            await table.DeleteOneAsync(filter);
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
                    DueDate = DateTime.UtcNow - TimeSpan.FromDays(1),
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
                    DueDate = DateTime.UtcNow - TimeSpan.FromDays(1),
                    Genres = (i % 2 == 0)
                        ? new HashSet<string> { "History", "Biography" }
                        : new HashSet<string> { "Fiction", "History" },
                    Rating = (float)new Random().NextDouble()
                };
                rows.Add(row);
            }
            var table = await fixture.Database.CreateTableAsync<RowBookSinglePrimaryKey>(tableName);
            await table.CreateIndexAsync("testDeleteOne_number_of_pages_index", (b) => b.NumberOfPages);
            await table.InsertManyAsync(rows);
            var filter = Builders<RowBookSinglePrimaryKey>.TableFilter
            .Eq(so => so.Title, "Title 1");
            var findResult = table.Find(filter).ToList();
            Assert.Single(findResult);
            await table.DeleteManyAsync(filter);
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

            var filter = Builders<CompositePrimaryKey>.TableFilter.And(
            new[] {
                Builders<CompositePrimaryKey>.TableFilter.Eq(x => x.KeyOne, "KeyOne3"),
                Builders<CompositePrimaryKey>.TableFilter.Eq(x => x.KeyTwo, "KeyTwo3")
            });

            var findResult = table.Find(filter).ToList();
            Assert.Single(findResult);
            await table.DeleteManyAsync(filter);
            var deletedResult = table.Find(filter).ToList();
            Assert.Empty(deletedResult);
        }
        catch (Exception ex)
        {
            var msg = ex.Message;
            throw;
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
            var filterBuilder = Builders<CompoundPrimaryKey>.TableFilter;
            var filter = filterBuilder.And(
                new[] {
                    filterBuilder.Eq(x => x.KeyOne,"KeyOne3"),
                    filterBuilder.Eq(x => x.KeyTwo,"KeyTwo3"),
                    filterBuilder.Eq(x => x.SortOneAscending,"SortOneAscending3"),
                    filterBuilder.Eq(x => x.SortTwoDescending, "SortTwoDescending3")
                });
            var findResult = table.Find(filter).ToList();
            Assert.Single(findResult);
            await table.DeleteManyAsync(filter);
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
            await table.DeleteManyAsync(null);
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
        var builder = Builders<Row>.TableFilter;
        var filter = builder.Gte("Id", 10) & builder.Eq("IdTwo", "IdTwo_20");

        var results = fixture.UntypedTableSinglePrimaryKey.Find(filter).ToList();
        Assert.Equal(1, results.Count);
        results = fixture.UntypedTableCompositePrimaryKey.Find(filter).ToList();
        Assert.Equal(1, results.Count);
        results = fixture.UntypedTableCompoundPrimaryKey.Find(filter).ToList();
        Assert.Equal(1, results.Count);

    }

    [Fact]
    public void LogicalAnd_AstraStyle_Untyped()
    {
        var builder = Builders<Row>.TableFilter;
        var filter = builder.And(builder.Gt("Id", 10), builder.Eq("IdTwo", "IdTwo_20"));

        var results = fixture.UntypedTableSinglePrimaryKey.Find(filter).ToList();
        Assert.Equal(1, results.Count);
        results = fixture.UntypedTableCompositePrimaryKey.Find(filter).ToList();
        Assert.Equal(1, results.Count);
        results = fixture.UntypedTableCompoundPrimaryKey.Find(filter).ToList();
        Assert.Equal(1, results.Count);
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

    [SkipWhenNotAstra]
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

    [SkipWhenNotAstra]
    [Fact]
    public async Task FindOne_Vectorize_Untyped()
    {
        var sorter = Builders<Row>.TableSort;
        var sort = sorter.Vectorize("Vectorize", "String To Vectorize 22");
        var results = await fixture.UntypedTableSinglePrimaryKey.FindOneAsync(null,
            new TableFindOneOptions<Row>() { Sort = sort, IncludeSimilarity = true });
        Assert.Equal("Name_22", results["Name"].ToString());
        results = await fixture.UntypedTableCompositePrimaryKey.FindOneAsync(null,
            new TableFindOneOptions<Row>() { Sort = sort, IncludeSimilarity = true });
        Assert.Equal("Name_22", results["Name"].ToString());
        results = await fixture.UntypedTableCompoundPrimaryKey.FindOneAsync(null,
            new TableFindOneOptions<Row>() { Sort = sort, IncludeSimilarity = true });
        Assert.Equal("Name_22", results["Name"].ToString());
    }

    [Fact]
    public async Task FindOne_Sort_Untyped()
    {
        var sorter = Builders<Row>.TableSort;
        var sort = sorter.Descending("Name");
        var projection = Builders<Row>.Projection.Include("Name");
        var result = await fixture.UntypedTableSinglePrimaryKey.FindOneAsync(null, new TableFindOneOptions<Row>() { Sort = sort, Projection = projection });
        Assert.Equal("Name_9", result["Name"].ToString());
        result = await fixture.UntypedTableCompositePrimaryKey.FindOneAsync(null, new TableFindOneOptions<Row>() { Sort = sort, Projection = projection });
        Assert.Equal("Name_9", result["Name"].ToString());
        result = await fixture.UntypedTableCompoundPrimaryKey.FindOneAsync(null, new TableFindOneOptions<Row>() { Sort = sort, Projection = projection });
        Assert.Equal("Name_9", result["Name"].ToString());
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
        results = fixture.UntypedTableCompositePrimaryKey.Find().Sort(sort).Project(projection).Skip(2).Limit(5).ToList();
        Assert.Equal(5, results.Count());
        Assert.Equal("Name_7", results.First()["Name"].ToString());
        results = fixture.UntypedTableCompoundPrimaryKey.Find().Sort(sort).Project(projection).Skip(2).Limit(5).ToList();
        Assert.Equal(5, results.Count());
        Assert.Equal("Name_7", results.First()["Name"].ToString());
    }

    [Fact]
    public async Task Update_Test_Untyped()
    {
        var filter = Builders<Row>.TableFilter.Eq("Id", 3);
        var update = Builders<Row>.TableUpdate.Set("Name", "Name_3_Updated");
        await fixture.UntypedTableSinglePrimaryKey.UpdateOneAsync(filter, update);
        var updatedDocument = await fixture.UntypedTableSinglePrimaryKey.FindOneAsync(filter);
        Assert.Equal("Name_3_Updated", updatedDocument["Name"].ToString());

        filter = Builders<Row>.TableFilter.And(
            new[] {
                Builders<Row>.TableFilter.Eq("Id", 3),
                Builders<Row>.TableFilter.Eq("IdTwo", "IdTwo_3")
            });

        await fixture.UntypedTableCompositePrimaryKey.UpdateOneAsync(filter, update);
        updatedDocument = await fixture.UntypedTableCompositePrimaryKey.FindOneAsync(filter);
        Assert.Equal("Name_3_Updated", updatedDocument["Name"].ToString());

        filter = Builders<Row>.TableFilter.And(
                new[] {
                    Builders<Row>.TableFilter.Eq("Id", 3),
                    Builders<Row>.TableFilter.Eq("IdTwo", "IdTwo_3"),
                    Builders<Row>.TableFilter.Eq("SortOneAscending", "SortOne_3"),
                    Builders<Row>.TableFilter.Eq("SortTwoDescending", "SortTwo_47")
                });
        await fixture.UntypedTableCompoundPrimaryKey.UpdateOneAsync(filter, update);
        updatedDocument = await fixture.UntypedTableCompoundPrimaryKey.FindOneAsync(filter);
        Assert.Equal("Name_3_Updated", updatedDocument["Name"].ToString());
    }

    [SkipWhenNotAstra]
    [Fact]
    public async Task FindOne_Lexical()
    {
        var tableName = "tableFindOneWithLexical";
        try
        {
            List<SimpleObjectWithLexical> items = new List<SimpleObjectWithLexical>() {
                new()
                {
                    Id = 0,
                    Name = "This is about a cat.",
                },
                new()
                {
                    Id = 1,
                    Name = "This is about a dog.",
                },
                new()
                {
                    Id = 2,
                    Name = "This is about a horse.",
                },
            };

            var table = await fixture.Database.CreateTableAsync<SimpleObjectWithLexical>(tableName);
            await table.CreateTextIndexAsync("b_idx", (b) => b.LexicalValue, Builders.TableIndex.Text());
            var insertResult = await table.InsertManyAsync(items);
            Assert.Equal(items.Count, insertResult.InsertedIdTuples.Count);
            var filter = Builders<SimpleObjectWithLexical>.TableFilter.LexicalMatch((b) => b.LexicalValue, "dog");
            var findOptions = new TableFindOneOptions<SimpleObjectWithLexical>()
            {
                Sort = Builders<SimpleObjectWithLexical>.TableSort.Lexical((b) => b.LexicalValue, "dog"),
            };

            var result = await table.FindOneAsync(filter, findOptions);
            Assert.NotNull(result);
            Assert.Equal(1, result.Id);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }
        finally
        {
            await fixture.Database.DropTableAsync(tableName);
        }
    }

}

