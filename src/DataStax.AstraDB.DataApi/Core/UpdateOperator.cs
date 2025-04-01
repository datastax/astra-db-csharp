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

public class UpdateOperator
{
    public const string CurrentDate = "$currentDate";
    public const string Increment = "$inc";
    public const string Min = "$min";
    public const string Max = "$max";
    public const string Multiply = "$mul";
    public const string Rename = "$rename";
    public const string Set = "$set";
    public const string SetOnInsert = "$setOnInsert";
    public const string Unset = "$unset";
    public const string AddToSet = "$addToSet";
    public const string Pop = "$pop";
    public const string Push = "$push";
    public const string Each = "$each";
    public const string Position = "$position";
}
