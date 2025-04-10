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

internal class UpdateOperator
{
    internal const string CurrentDate = "$currentDate";
    internal const string Increment = "$inc";
    internal const string Min = "$min";
    internal const string Max = "$max";
    internal const string Multiply = "$mul";
    internal const string Rename = "$rename";
    internal const string Set = "$set";
    internal const string SetOnInsert = "$setOnInsert";
    internal const string Unset = "$unset";
    internal const string AddToSet = "$addToSet";
    internal const string Pop = "$pop";
    internal const string Push = "$push";
    internal const string Each = "$each";
    internal const string Position = "$position";
}
