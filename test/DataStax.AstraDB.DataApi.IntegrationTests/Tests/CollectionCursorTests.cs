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

    [Fact]
    public async Task Test_CollectionCursor_ClosedProperties()
    {
        var filledCollection = _fixture.FilledCollection;

        var cur0 = filledCollection.Find();
        cur0.Dispose();
        cur0.Rewind();
        Assert.Equal(CursorState.Idle, cur0.State);

        var cur1 = filledCollection.Find();
        Assert.Equal(0, cur1.Consumed);
        await foreach (var item in cur1) { /* moot */ }
        Assert.Equal(CursorState.Closed, cur1.State);
        Assert.Empty(cur1.ConsumeBuffer(2));
        Assert.Equal(_fixture.FilledCollectionCount, cur1.Consumed);
        Assert.Equal(0, cur1.Buffered());
        var cloned = cur1.Clone();
        Assert.Equal(0, cloned.Consumed);
        Assert.Equal(0, cloned.Buffered());
        Assert.Equal(CursorState.Idle, cloned.State);

        // Closed cursors can't receive fluent-API edits:

        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Filter(
        //     Builders<CursorTestDocument>.CollectionFilter.Empty()
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Project(
        //     Builders<CursorTestDocument>.Projection.Include(d => d.PText)
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Sort(
        //     Builders<CursorTestDocument>.Sort.Ascending(d => d.PText)
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Limit(1); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.Skip(1); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.IncludeSimilarity(true); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur1.IncludeSortVector(true); });

        // (note the full prefix required otherwise ambiguous a/sync unresolved)
        await Assert.ThrowsAsync<CursorException>(async () => { 
            await System.Linq.AsyncEnumerable.Select(cur1, doc => doc.PText).ToListAsync();
         });

    }

    [Fact]
    public async Task Test_CollectionCursor_StartedProperties()
    {
        var filledCollection = _fixture.FilledCollection;

        var cur = filledCollection.Find();
        await cur.MoveNextAsync();
        // now: 19 in buffer, one consumed:
        Assert.Equal(1, cur.Consumed);
        Assert.Equal(19, cur.Buffered());
        Assert.Equal(3, cur.ConsumeBuffer(3).Count);
        // 16 in buffer, 4 consumed:
        Assert.Equal(4, cur.Consumed);
        Assert.Equal(16, cur.Buffered());
        // from time to time the buffer is empty:
        for(int i=0; i<16; i++){
            await cur.MoveNextAsync();
        }
        Assert.Equal(0, cur.Buffered());
        Assert.Equal(0, cur.ConsumeBuffer(3).Count);
        Assert.Equal(20, cur.Consumed);
        Assert.Equal(0, cur.Buffered());

        // Started cursors can't receive fluent-API edits:

        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Filter(
        //     Builders<CursorTestDocument>.CollectionFilter.Empty()
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Project(
        //     Builders<CursorTestDocument>.Projection.Include(d => d.PText)
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Sort(
        //     Builders<CursorTestDocument>.Sort.Ascending(d => d.PText)
        // ); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Limit(1); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.Skip(1); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.IncludeSimilarity(true); });
        // TODO does not throw (and should)
        // Assert.Throws<CursorException>(() => { cur.IncludeSortVector(true); });

        // TODO: this one *does not throw* (other clients don't admit setting mapping *after* started)
        //       But in this case "we're in LINQ's hands" and can't really do much about it. Are we ok?
        //
        // (note the full prefix required otherwise ambiguous a/sync unresolved)
        //await Assert.ThrowsAsync<CursorException>(async () => { 
            await System.Linq.AsyncEnumerable.Select(cur, doc => doc.PText).ToListAsync();
        // });

    }

}
