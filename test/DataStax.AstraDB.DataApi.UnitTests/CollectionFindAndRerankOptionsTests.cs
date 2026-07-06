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