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

namespace DataStax.AstraDB.DataApi.Core;

/// <summary>
/// The data source to connect to.
/// </summary>
public enum DataApiDestination
{
    /// <summary>DataStax Astra DB (managed cloud service).</summary>
    ASTRA,
    /// <summary>DataStax Enterprise (DSE).</summary>
    DSE,
    /// <summary>HCD (Hyper-Converged Database).</summary>
    HCD,
    /// <summary>Apache Cassandra.</summary>
    CASSANDRA,
    /// <summary>Another data source not covered by the predefined values.</summary>
    OTHER
}
