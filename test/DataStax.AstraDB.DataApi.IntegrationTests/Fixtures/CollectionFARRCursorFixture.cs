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

using DataStax.AstraDB.DataApi.Collections;
using DataStax.AstraDB.DataApi.Core;
using DataStax.AstraDB.DataApi.Core.Query;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace DataStax.AstraDB.DataApi.IntegrationTests.Fixtures;

[CollectionDefinition("CollectionFARRCursor")]
public class CollectionFARRCursorCollection : ICollectionFixture<AssemblyFixture>, ICollectionFixture<CollectionFARRCursorFixture>
{
}

public class CollectionFARRCursorFixture : BaseFixture, IAsyncLifetime
{
    private string headerRerankingAPIKey;
    private string headerEmbeddingAPIKey;

    // used by a test that does a untyped GetCollection
    public CreateCollectionOptions GetCollectionVectorOptions { get; private set; }

    public CollectionFARRCursorFixture(AssemblyFixture assemblyFixture) : base(assemblyFixture, "collectionFARRCursor")
    {
    }

    public Collection<FARRCursorTestVectorDocument> FilledVectorCollection { get; private set; }
    public Collection<FARRCursorTestVectorizeDocument> FilledVectorizeCollection { get; private set; }
    public int FilledVectorCollectionCount { get; private set; }
    public int FilledVectorizeCollectionCount { get; private set; }

    private bool IsAstra()
    {
        var destination = Database.Client.ClientOptions.Destination;
        return destination == DataAPIDestination.ASTRA;
    }

    public async ValueTask InitializeAsync()
    {
        headerRerankingAPIKey = Environment.GetEnvironmentVariable("HEADER_RERANKING_API_KEY_NVIDIA") ?? "kaboom";
        headerEmbeddingAPIKey = Environment.GetEnvironmentVariable("HEADER_EMBEDDING_API_KEY_VOYAGEAI") ?? "kaboom";
        GetCollectionVectorOptions = IsAstra() ? new () : new () { RerankingAPIKey = headerRerankingAPIKey };
        await CreateFilledVectorCollection();
        await CreateFilledVectorizeCollection();
    }

    public async ValueTask DisposeAsync()
    {
        await Database.DropCollectionAsync<FARRCursorTestVectorDocument>();
        await Database.DropCollectionAsync<FARRCursorTestVectorizeDocument>();
    }

    private const int NUM_DOCS = 5;

    private async Task CreateFilledVectorCollection()
    {

        var collectionDefinition = new CollectionDefinition
        {
            Lexical = new LexicalOptions
            {
                Analyzer = new AnalyzerOptions
                {
                    Tokenizer = new TokenizerOptions
                    {
                        Name = "standard"
                    }
                }
            },
            Rerank = new RerankOptions
            {
                Enabled = true,
                Service = new RerankServiceOptions
                {
                    ModelName = "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                    Provider = "nvidia"
                }
            }
        };
        var collection = IsAstra() ?
            (await Database.CreateCollectionAsync<FARRCursorTestVectorDocument>(collectionDefinition)) :
            (await Database.CreateCollectionAsync<FARRCursorTestVectorDocument>(
                collectionDefinition,
                new () { RerankingAPIKey = headerRerankingAPIKey }
            )) ;

        await collection.DeleteManyAsync(Builders<FARRCursorTestVectorDocument>.CollectionFilter.Empty());

        var testDocuments = new List<FARRCursorTestVectorDocument>();
        for (int i = 0; i < NUM_DOCS; i++)
        {
            testDocuments.Add(new FARRCursorTestVectorDocument
            {
                Id = $"doc_{i + 1}",
                PText = $"text of document number {i + 1}",
                PInt = i + 1,
                Vector = new float[] { 1.0f, 1.0f / (i + 1) }
            });
        }
        
        await collection.InsertManyAsync(testDocuments);

        FilledVectorCollection = collection;
        FilledVectorCollectionCount = NUM_DOCS;
    }

    private async Task CreateFilledVectorizeCollection()
    {

        var collectionDefinition = new CollectionDefinition
        {
            Lexical = new LexicalOptions
            {
                Analyzer = new AnalyzerOptions
                {
                    Tokenizer = new TokenizerOptions
                    {
                        Name = "standard"
                    }
                }
            },
            Rerank = new RerankOptions
            {
                Enabled = true,
                Service = new RerankServiceOptions
                {
                    ModelName = "nvidia/llama-3.2-nv-rerankqa-1b-v2",
                    Provider = "nvidia"
                }
            }
        };
        var collection = IsAstra() ?
            ( await Database.CreateCollectionAsync<FARRCursorTestVectorizeDocument>(
                collectionDefinition,
                new () { EmbeddingAPIKey = headerEmbeddingAPIKey }
            ) ) :
            ( await Database.CreateCollectionAsync<FARRCursorTestVectorizeDocument>(
                collectionDefinition,
                new () { EmbeddingAPIKey = headerEmbeddingAPIKey, RerankingAPIKey = headerRerankingAPIKey }
            ) ) ;

        await collection.DeleteManyAsync(Builders<FARRCursorTestVectorizeDocument>.CollectionFilter.Empty());

        var testDocuments = new List<FARRCursorTestVectorizeDocument>();
        for (int i = 0; i < NUM_DOCS; i++)
        {
            testDocuments.Add(new FARRCursorTestVectorizeDocument
            {
                Id = $"doc_{i + 1}",
                PText = $"text of document number {i + 1}",
                PInt = i + 1,
            });
        }
        
        await collection.InsertManyAsync(testDocuments);

        FilledVectorizeCollection = collection;
        FilledVectorizeCollectionCount = NUM_DOCS;
    }

}
