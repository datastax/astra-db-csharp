using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Commands;
using DataStax.AstraDB.DataApi.Core.Enumeration;
using DataStax.AstraDB.DataApi.Core.Results;
using DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;
using DataStax.AstraDB.DataApi.SerDes;
using System.Linq;
using MongoDB.Bson;
using System.Text;
using UUIDNext;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests;

[Collection("CollectionCursor")]
public class CollectionCursorTests
{

    private readonly CollectionCursorFixture _fixture;
    Collection<CursorTestDocument> filledCollection;

    public CollectionCursorTests(AssemblyFixture assemblyFixture, CollectionCursorFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Test_CollectionCursor_IdleProperties()
    {
        var filledCollection = _fixture.FilledCollection;

        // pristine cursor properties
        var cur = filledCollection.Find();
        Assert.Equal(CursorState.Idle, cur.State);
        Assert.Equal(0, cur.Consumed);
        Assert.Empty(cur.ConsumeBuffer(3));
        Assert.Equal(0, cur.Buffered());
        Assert.Equal(0, cur.Consumed);

        var toClose = cur.Clone();
        toClose.Dispose();
        toClose.Dispose();
        Assert.Equal(CursorState.Closed, toClose.State);
        Assert.Equal(CursorState.Idle, cur.State);

        await Assert.ThrowsAsync<CursorException>( async () =>
        {
            await foreach (var item in toClose) { /* moot */ }
        });
        Assert.Throws<CursorException>(() =>
        {
            foreach (var item in toClose) { /* moot */ }
        });
        await Assert.ThrowsAsync<CursorException>( async () => 
            await toClose.ToListAsync()
        );
        Assert.Throws<CursorException>( () => 
            toClose.ToList()
        );
        // TODO: c# does not error on "MoveNextAsync" for closed cursor (python does). Do we care?

        // rewinding
        cur.Rewind();
        Assert.Equal(CursorState.Idle, cur.State);
        Assert.Equal(0, cur.Consumed);
        Assert.Equal(0, cur.Buffered());

        // various fluent-api methods
        cur.Filter(Builders<CursorTestDocument>.CollectionFilter.Eq(d => d.PInt, 789));
        cur.Project(Builders<CursorTestDocument>.Projection.Include(d => d.PInt));
        cur.Sort(Builders<CursorTestDocument>.Sort.Ascending(d => d.PInt));
        cur.Limit(1);
        cur.IncludeSimilarity(false);
        cur.IncludeSortVector(true);
        cur.Skip(1);
    }

}
