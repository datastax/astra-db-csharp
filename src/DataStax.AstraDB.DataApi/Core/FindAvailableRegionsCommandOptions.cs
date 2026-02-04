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
/// Filter Available Regions by type.
/// </summary>
internal enum RegionTypeFilter
{
  /// <summary>
  /// All region types
  /// </summary>
  All,
  /// <summary>
  /// Only Vector regions
  /// </summary>
  Vector,
  /// <summary>
  /// Only Serverless regions
  /// </summary>
  Serverless
}

/// <summary>
/// Options for Find Available Regions command.
/// </summary>
public class FindAvailableRegionsCommandOptions : CommandOptions
{
  /// <summary>
  /// If true, only return regions that can be used by the calling organization.
  /// </summary>
  public bool OnlyOrgEnabledRegions { get; set; } = true;

  /// <summary>
  /// Currently clients only support returning Vector regions.
  /// </summary>
  internal RegionTypeFilter RegionType { get; set; } = RegionTypeFilter.Vector;

}