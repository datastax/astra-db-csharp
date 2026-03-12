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

using DataStax.AstraDB.DataApi.Utils;

namespace DataStax.AstraDB.DataApi.UnitTests;

public class FieldEscapingTests
{
    [Fact]
    public void EscapeFieldNames_ReturnsEmptyString_ForEmptyArrays()
    {
        Assert.Equal("", FieldEscaping.EscapeFieldNames());
        Assert.Equal("", FieldEscaping.EscapeFieldNames(Array.Empty<string>()));
        Assert.Equal("", FieldEscaping.EscapeFieldNames(new List<string>()));
        Assert.Equal("", FieldEscaping.EscapeFieldNames((IEnumerable<string>)null));
    }

    [Fact]
    public void EscapeFieldNames_EscapesPathSegments_WithSpecialCharacters()
    {
        var result = FieldEscaping.EscapeFieldNames("a&", "b..", "0", "c&d");
        Assert.Equal("a&&.b&.&..0.c&&d", result);
    }

    [Fact]
    public void EscapeFieldNames_EscapesPathSegments_WithIEnumerable()
    {
        var segments = new List<string> { "a&", "b..", "0", "c&d" };
        var result = FieldEscaping.EscapeFieldNames(segments);
        Assert.Equal("a&&.b&.&..0.c&&d", result);
    }

    [Fact]
    public void EscapeFieldNames_EscapesSingleDot()
    {
        var result = FieldEscaping.EscapeFieldNames("field.name");
        Assert.Equal("field&.name", result);
    }

    [Fact]
    public void EscapeFieldNames_EscapesSingleAmpersand()
    {
        var result = FieldEscaping.EscapeFieldNames("field&name");
        Assert.Equal("field&&name", result);
    }

    [Fact]
    public void EscapeFieldNames_EscapesMultipleSegments()
    {
        var result = FieldEscaping.EscapeFieldNames("user", "address", "street");
        Assert.Equal("user.address.street", result);
    }

    [Fact]
    public void EscapeFieldNames_EscapesSegmentsWithBothSpecialChars()
    {
        var result = FieldEscaping.EscapeFieldNames("a.b&c");
        Assert.Equal("a&.b&&c", result);
    }

    [Fact]
    public void EscapeFieldNames_HandlesNumericSegments()
    {
        var result = FieldEscaping.EscapeFieldNames("array", "0", "field");
        Assert.Equal("array.0.field", result);
    }

    [Fact]
    public void EscapeFieldNames_HandlesConsecutiveSpecialChars()
    {
        var result = FieldEscaping.EscapeFieldNames("a&&", "b..");
        Assert.Equal("a&&&&.b&.&.", result);
    }

    [Fact]
    public void EscapeFieldNames_HandlesSingleSegment()
    {
        Assert.Equal("simple", FieldEscaping.EscapeFieldNames("simple"));
        Assert.Equal("with&&ampersand", FieldEscaping.EscapeFieldNames("with&ampersand"));
        Assert.Equal("with&.dot", FieldEscaping.EscapeFieldNames("with.dot"));
    }

    [Fact]
    public void UnescapeFieldPath_ReturnsEmptyArray_ForEmptyString()
    {
        var result = FieldEscaping.UnescapeFieldPath("");
        Assert.Empty(result);
    }

    [Fact]
    public void UnescapeFieldPath_ReturnsEmptyArray_ForNull()
    {
        var result = FieldEscaping.UnescapeFieldPath(null);
        Assert.Empty(result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesSimplePath()
    {
        var result = FieldEscaping.UnescapeFieldPath("a.a");
        Assert.Equal(new[] { "a", "a" }, result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesEscapedDot()
    {
        var result = FieldEscaping.UnescapeFieldPath("a&.");
        Assert.Equal(new[] { "a." }, result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesEscapedDotInMiddle()
    {
        var result = FieldEscaping.UnescapeFieldPath("a&.a");
        Assert.Equal(new[] { "a.a" }, result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesMultipleEscapedChars()
    {
        var result = FieldEscaping.UnescapeFieldPath("a&.a&&&.a");
        Assert.Equal(new[] { "a.a&.a" }, result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesComplexPath()
    {
        var result = FieldEscaping.UnescapeFieldPath("a&&&.b&.c&&&&d");
        Assert.Equal(new[] { "a&.b.c&&d" }, result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesPathWithNumericSegment()
    {
        var result = FieldEscaping.UnescapeFieldPath("p.0");
        Assert.Equal(new[] { "p", "0" }, result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesEscapedAmpersandAndDot()
    {
        var result = FieldEscaping.UnescapeFieldPath("&&.&.");
        Assert.Equal(new[] { "&", "." }, result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesDoubleAmpersand()
    {
        var result = FieldEscaping.UnescapeFieldPath("&&");
        Assert.Equal(new[] { "&" }, result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesEscapedDotOnly()
    {
        var result = FieldEscaping.UnescapeFieldPath("&.");
        Assert.Equal(new[] { "." }, result);
    }

    [Fact]
    public void UnescapeFieldPath_UnescapesComplexMixedPath()
    {
        var result = FieldEscaping.UnescapeFieldPath("tom&&jerry&..&.");
        Assert.Equal(new[] { "tom&jerry.", "." }, result);
    }

    [Fact]
    public void UnescapeFieldPath_HandlesSingleSegmentWithoutSpecialChars()
    {
        var result = FieldEscaping.UnescapeFieldPath("simple");
        Assert.Equal(new[] { "simple" }, result);
    }

    [Fact]
    public void UnescapeFieldPath_HandlesMultipleSegments()
    {
        var result = FieldEscaping.UnescapeFieldPath("user.address.street");
        Assert.Equal(new[] { "user", "address", "street" }, result);
    }

    [Fact]
    public void UnescapeFieldPath_ThrowsException_WhenPathStartsWithDot()
    {
        var ex = Assert.Throws<ArgumentException>(() => 
            FieldEscaping.UnescapeFieldPath(".field"));
        Assert.Contains("'.' may not appear at the beginning of the path", ex.Message);
    }

    [Fact]
    public void UnescapeFieldPath_ThrowsException_WhenPathEndsWithDot()
    {
        var ex = Assert.Throws<ArgumentException>(() => 
            FieldEscaping.UnescapeFieldPath("field."));
        Assert.Contains("'.' may not appear at the end of the path", ex.Message);
    }

    [Fact]
    public void UnescapeFieldPath_ThrowsException_WhenPathHasEmptySegment()
    {
        var ex = Assert.Throws<ArgumentException>(() => 
            FieldEscaping.UnescapeFieldPath("field..another"));
        Assert.Contains("empty segment found", ex.Message);
    }

    [Fact]
    public void UnescapeFieldPath_ThrowsException_WhenPathEndsWithAmpersand()
    {
        var ex = Assert.Throws<ArgumentException>(() => 
            FieldEscaping.UnescapeFieldPath("field&"));
        Assert.Contains("'&' may not appear at the end of the path", ex.Message);
    }

    [Fact]
    public void UnescapeFieldPath_ThrowsException_WhenAmpersandNotFollowedByValidChar()
    {
        var ex = Assert.Throws<ArgumentException>(() => 
            FieldEscaping.UnescapeFieldPath("field&x"));
        Assert.Contains("'&' may not appear alone", ex.Message);
    }

    [Fact]
    public void UnescapeFieldPath_ThrowsException_WhenAmpersandFollowedByInvalidChar()
    {
        var ex = Assert.Throws<ArgumentException>(() => 
            FieldEscaping.UnescapeFieldPath("a&b"));
        Assert.Contains("'&' may not appear alone", ex.Message);
    }

    [Fact]
    public void RoundTrip_EscapeAndUnescape_ProducesOriginalSegments()
    {
        var original = new[] { "user", "address.street", "apt&unit" };
        var escaped = FieldEscaping.EscapeFieldNames(original);
        var unescaped = FieldEscaping.UnescapeFieldPath(escaped);
        
        Assert.Equal(original, unescaped);
    }

    [Fact]
    public void RoundTrip_WithComplexSpecialCharacters()
    {
        var original = new[] { "a&b", "c.d", "e&.f" };
        var escaped = FieldEscaping.EscapeFieldNames(original);
        var unescaped = FieldEscaping.UnescapeFieldPath(escaped);
        
        Assert.Equal(original, unescaped);
    }

    [Fact]
    public void RoundTrip_WithConsecutiveSpecialChars()
    {
        var original = new[] { "a&&", "b..", "c&." };
        var escaped = FieldEscaping.EscapeFieldNames(original);
        var unescaped = FieldEscaping.UnescapeFieldPath(escaped);
        
        Assert.Equal(original, unescaped);
    }
}
