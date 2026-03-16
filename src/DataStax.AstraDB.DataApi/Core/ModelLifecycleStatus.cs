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
/// Status filter for reranking providers or embedding providers.
/// </summary>
public enum ModelLifecycleStatus
{
  /// <summary>
  /// Only supported providers (default)
  /// </summary>
  Supported,
  /// <summary>
  /// Only deprecated providers
  /// </summary>
  Deprecated,
  /// <summary>
  /// Only end-of-life providers
  /// </summary>
  EndOfLife,
  /// <summary>
  /// All providers
  /// </summary>
  All
}

internal static class Extensions
{
    internal static string ToString(ModelLifecycleStatus status)
    {
        return status switch
      {
        ModelLifecycleStatus.Supported => "SUPPORTED",
        ModelLifecycleStatus.Deprecated => "DEPRECATED",
        ModelLifecycleStatus.EndOfLife => "END_OF_LIFE",
        ModelLifecycleStatus.All => "",
        _ => "SUPPORTED",
      };
    }
}