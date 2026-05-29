using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core.Query;

namespace DataStax.AstraDB.DataApi.UnitTests;

public class CollectionFindAndRerankOptionsTests
{
    [Fact]
    public void Test_CollectionFindAndRerankOptions_HybridLimits()
    {
        // testing good/bad HybridLimits combinations
        Assert.Throws<ArgumentException>(
            () => new CollectionFindAndRerankOptions<Document>() {
                HybridLimits = 10, VectorLimit = 10, LexicalLimit = 10
            }.ToPayload(Builders<Document>.CollectionFilter.Empty())
        );
        Assert.Throws<ArgumentException>(
            () => new CollectionFindAndRerankOptions<Document>() {
                HybridLimits = 10, VectorLimit = 10, LexicalLimit = null
            }.ToPayload(Builders<Document>.CollectionFilter.Empty())
        );
        Assert.Throws<ArgumentException>(
            () => new CollectionFindAndRerankOptions<Document>() {
                HybridLimits = 10, VectorLimit = null, LexicalLimit = 10
            }.ToPayload(Builders<Document>.CollectionFilter.Empty())
        );
        new CollectionFindAndRerankOptions<Document>() {
            HybridLimits = 10, VectorLimit = null, LexicalLimit = null
        }.ToPayload(Builders<Document>.CollectionFilter.Empty());
        new CollectionFindAndRerankOptions<Document>() {
            HybridLimits = null, VectorLimit = 10, LexicalLimit = 10
        }.ToPayload(Builders<Document>.CollectionFilter.Empty());
        Assert.Throws<ArgumentException>(
            () => new CollectionFindAndRerankOptions<Document>() {
                HybridLimits = null, VectorLimit = 10, LexicalLimit = null
            }.ToPayload(Builders<Document>.CollectionFilter.Empty())
        );
        Assert.Throws<ArgumentException>(
            () => new CollectionFindAndRerankOptions<Document>() {
                HybridLimits = null, VectorLimit = null, LexicalLimit = 10
            }.ToPayload(Builders<Document>.CollectionFilter.Empty())
        );
        new CollectionFindAndRerankOptions<Document>() {
            HybridLimits = null, VectorLimit = null, LexicalLimit = null
        }.ToPayload(Builders<Document>.CollectionFilter.Empty());
    }

}