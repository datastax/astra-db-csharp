// using System;
// using System.Text.Json;
// using System.Text.Json.Serialization;
// using NodaTime;
// using NodaTime.Text;

// public class PeriodConverter : JsonConverter<Period>
// {
//     private static readonly PeriodPattern Pattern = PeriodPattern.Roundtrip;

//     public override Period Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
//     {
//         if (reader.TokenType != JsonTokenType.String)
//         {
//             throw new JsonException($"Expected a string for ISO 8601 duration, but got {reader.TokenType}.");
//         }

//         string durationString = reader.GetString() ?? throw new JsonException("ISO 8601 duration string is null.");

//         if (string.IsNullOrEmpty(durationString))
//         {
//             return Period.Zero;
//         }

//         try
//         {
//             return Pattern.Parse(durationString).Value;
//         }
//         catch (FormatException ex)
//         {
//             throw new JsonException($"Invalid ISO 8601 duration format: {durationString}. Expected format like 'P3Y6M4DT12H30M5S'.", ex);
//         }
//     }

//     public override void Write(Utf8JsonWriter writer, Period value, JsonSerializerOptions options)
//     {
//         writer.WriteStringValue(Pattern.Format(value));
//     }
// }