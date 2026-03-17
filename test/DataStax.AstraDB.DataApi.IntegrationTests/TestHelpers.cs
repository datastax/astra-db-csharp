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

using System;
using Xunit;

public static class DateTimeAssert
{
    /// <summary>
    /// Asserts two DateTime values are equal to millisecond precision.
    /// Use this when comparing values that have been round-tripped through the API,
    /// which stores timestamps at millisecond precision.
    /// </summary>
    public static void EqualToMs(DateTime expected, DateTime actual)
    {
        Assert.Equal(
            expected.Ticks / TimeSpan.TicksPerMillisecond,
            actual.Ticks / TimeSpan.TicksPerMillisecond
        );
    }

    /// <summary>
    /// Asserts two nullable DateTime values are equal to millisecond precision.
    /// </summary>
    public static void EqualToMs(DateTime? expected, DateTime? actual)
    {
        if (expected == null && actual == null) return;
        Assert.NotNull(expected);
        Assert.NotNull(actual);
        EqualToMs(expected.Value, actual.Value);
    }
}
