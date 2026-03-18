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

using System;
using System.Buffers.Binary;
using System.Security.Cryptography;

/// <summary>
/// Convenience class for dealing with TimeUuids (UUID v1)
/// </summary>
public readonly struct TimeUuid : IEquatable<TimeUuid>, IComparable<TimeUuid>
{
    private readonly Guid _value;

    private static readonly DateTimeOffset GregorianEpoch =
        new DateTimeOffset(1582, 10, 15, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// Returns the underlying Guid.
    /// </summary>
    public Guid Value => _value;

    /// <summary>
    /// Create a TimeUuid given a Guid.
    /// </summary>
    /// <param name="guid"></param>
    /// <exception cref="ArgumentException"></exception>
    public TimeUuid(Guid guid)
    {
        if (!IsTimeUuid(guid))
            throw new ArgumentException("Guid is not a version 1 UUID.", nameof(guid));

        _value = guid;
    }

    private TimeUuid(Guid guid, bool _)
    {
        _value = guid;
    }

    /// <summary>
    /// Convert a Guid to TimeUuid (if it is indeed a timeuuid)
    /// </summary>
    /// <param name="guid"></param>
    /// <param name="result"></param>
    /// <returns></returns>
    public static bool TryCreate(Guid guid, out TimeUuid result)
    {
        if (IsTimeUuid(guid))
        {
            result = new TimeUuid(guid, true);
            return true;
        }

        result = default;
        return false;
    }

    /// <summary>
    /// Parse a string to a TimeUuid
    /// </summary>
    /// <param name="input"></param>
    /// <returns></returns>
    public static TimeUuid Parse(string input)
        => new TimeUuid(Guid.Parse(input));

    /// <summary>
    /// Attempt parsing a string representation of a uuid to a TimeUuid
    /// </summary>
    /// <param name="input"></param>
    /// <param name="result"></param>
    /// <returns></returns>
    public static bool TryParse(string input, out TimeUuid result)
    {
        if (Guid.TryParse(input, out var g) && IsTimeUuid(g))
        {
            result = new TimeUuid(g, true);
            return true;
        }

        result = default;
        return false;
    }

    /// <summary>
    /// Check a Guid to see if it's a valid TimeUuid
    /// </summary>
    /// <param name="guid"></param>
    /// <returns></returns>
    public static bool IsTimeUuid(Guid guid)
    {
        var bytes = guid.ToByteArray();
        return ((bytes[7] >> 4) & 0x0F) == 1;
    }

    /// <summary>
    /// Returns the timestamp representation of the uuid
    /// </summary>
    public DateTimeOffset Timestamp
    {
        get
        {
            var bytes = _value.ToByteArray();

            long timeLow = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0, 4));
            long timeMid = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4, 2));
            long timeHi = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(6, 2)) & 0x0FFF;

            long timestamp = (timeHi << 48) | (timeMid << 32) | timeLow;

            return GregorianEpoch.AddTicks(timestamp);
        }
    }

    /// <summary>
    /// Returns the unix milliseconds representation of the uuid
    /// </summary>
    public long UnixMilliseconds => Timestamp.ToUnixTimeMilliseconds();

    /// <summary>
    /// The Clock sequence
    /// </summary>
    public ushort ClockSequence
    {
        get
        {
            var bytes = _value.ToByteArray();
            return (ushort)(((bytes[8] & 0x3F) << 8) | bytes[9]);
        }
    }

    /// <summary>
    /// The uuid's node
    /// </summary>
    public byte[] Node
    {
        get
        {
            var bytes = _value.ToByteArray();
            var node = new byte[6];
            Array.Copy(bytes, 10, node, 0, 6);
            return node;
        }
    }

    /// <summary>
    /// Returns the NodeHex
    /// </summary>
    public string NodeHex
    {
        get
        {
            var node = Node;
            return BitConverter.ToString(node).Replace("-", "").ToLowerInvariant();
        }
    }

    /// <summary>
    /// Performs time-relative comparison
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public int CompareTo(TimeUuid other)
    {
        var tsCompare = Timestamp.CompareTo(other.Timestamp);
        if (tsCompare != 0) return tsCompare;
        return _value.CompareTo(other._value);
    }

    private static readonly RandomNumberGenerator _rng = RandomNumberGenerator.Create();
    private static readonly object _lock = new();
    private static long _lastTimestamp;
    private static ushort _clockSeq = GetRandomClockSeq();
    private static readonly byte[] _node = CreateNode();

    /// <summary>
    /// Generate a new TimeUuid
    /// </summary>
    /// <returns></returns>
    public static TimeUuid New()
    {
        lock (_lock)
        {
            long timestamp = GetCurrentTimestamp();

            if (timestamp <= _lastTimestamp)
            {
                _clockSeq++;
            }

            _lastTimestamp = timestamp;

            return new TimeUuid(CreateGuid(timestamp, _clockSeq, _node), true);
        }
    }

    private static long GetCurrentTimestamp()
    {
        return (DateTimeOffset.UtcNow - GregorianEpoch).Ticks;
    }

    private static Guid CreateGuid(long timestamp, ushort clockSeq, byte[] node)
    {
        var bytes = new byte[16];

        uint timeLow = (uint)(timestamp & 0xFFFFFFFF);
        ushort timeMid = (ushort)((timestamp >> 32) & 0xFFFF);
        ushort timeHi = (ushort)((timestamp >> 48) & 0x0FFF);

        timeHi |= (1 << 12);

        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(0, 4), timeLow);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4, 2), timeMid);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(6, 2), timeHi);

        bytes[8] = (byte)((clockSeq >> 8) | 0x80);
        bytes[9] = (byte)(clockSeq & 0xFF);

        Array.Copy(node, 0, bytes, 10, 6);

        return new Guid(bytes);
    }

    private static byte[] CreateNode()
    {
        var node = new byte[6];
        _rng.GetBytes(node);

        node[0] |= 0x01;
        return node;
    }

    private static ushort GetRandomClockSeq()
    {
        var bytes = new byte[2];
        _rng.GetBytes(bytes);
        return (ushort)(((bytes[0] << 8) | bytes[1]) & 0x3FFF);
    }

    /// <summary>
    /// TimeUuid to Guid
    /// </summary>
    /// <param name="t"></param>
    public static implicit operator Guid(TimeUuid t) => t._value;
    /// <summary>
    /// Guid to TimeUuid
    /// </summary>
    /// <param name="g"></param>
    public static explicit operator TimeUuid(Guid g) => new TimeUuid(g);

    /// <summary>
    /// ToString
    /// </summary>
    /// <returns></returns>
    public override string ToString() => _value.ToString();

    /// <summary>
    /// Equals
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public bool Equals(TimeUuid other) => _value.Equals(other._value);
    /// <summary>
    /// Equals
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public override bool Equals(object obj) => obj is TimeUuid other && Equals(other);
    /// <summary>
    /// Get hash code
    /// </summary>
    /// <returns></returns>
    public override int GetHashCode() => _value.GetHashCode();
}