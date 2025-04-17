namespace Mofucat.DataToolkit;

using System;
using System.Buffers;
using System.Data;

#pragma warning disable CA1725
public sealed class MappingDataReader : IDataReader
{
    private struct Entry
    {
        public int SourceIndex;

        public Type? ConvertType;

        public Func<object, object>? Converter;
    }

    private readonly IDataReader source;

    private readonly int fieldCount;

    private Entry[] entries;

    //--------------------------------------------------------------------------------
    // Property
    //--------------------------------------------------------------------------------

    public int FieldCount => fieldCount;

    public int Depth => source.Depth;

    public bool IsClosed { get; private set; }

    public int RecordsAffected => -1;

    public object this[int i] => GetValue(i);

    public object this[string name] => GetValue(GetOrdinal(name));

    //--------------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------------

    public MappingDataReader(IDataReader source)
    {
        // TODO
        this.source = source;
        fieldCount = source.FieldCount;
        entries = ArrayPool<Entry>.Shared.Rent(fieldCount);

        // TODO
        for (var i = 0; i < fieldCount; i++)
        {
            ref var entry = ref entries[i];
            entry.SourceIndex = i;
            entry.ConvertType = null;
            entry.Converter = null;
        }
    }

    public void Dispose()
    {
        if (IsClosed)
        {
            return;
        }

        source.Close();
        source.Dispose();

        if (entries.Length > 0)
        {
            ArrayPool<Entry>.Shared.Return(entries, true);
            entries = [];
        }

        IsClosed = true;
    }

    public void Close()
    {
        Dispose();
    }

    //--------------------------------------------------------------------------------
    // Iterator
    //--------------------------------------------------------------------------------

    public bool Read() => source.Read();

    public bool NextResult() => source.NextResult();

    //--------------------------------------------------------------------------------
    // Metadata
    //--------------------------------------------------------------------------------

    public IDataReader GetData(int i) => throw new NotSupportedException();

    public DataTable GetSchemaTable() => throw new NotSupportedException();

    public string GetDataTypeName(int i)
    {
        ref var entry = ref entries[i];
        return entry.ConvertType?.Name ?? source.GetDataTypeName(entry.SourceIndex);
    }

    public Type GetFieldType(int i)
    {
        ref var entry = ref entries[i];
        return entry.ConvertType ?? source.GetFieldType(entry.SourceIndex);
    }

    public string GetName(int i)
    {
        ref var entry = ref entries[i];
        return source.GetName(entry.SourceIndex);
    }

    public int GetOrdinal(string name)
    {
        for (var i = 0; i < fieldCount; i++)
        {
            if (String.Equals(GetName(i), name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }
        return -1;
    }

    //--------------------------------------------------------------------------------
    // Value
    //--------------------------------------------------------------------------------

    public bool IsDBNull(int i)
    {
        ref var entry = ref entries[i];
        return source.IsDBNull(entry.SourceIndex);
    }

    public object GetValue(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetValue(entry.SourceIndex);
    }

    public int GetValues(object[] values)
    {
        for (var i = 0; i < fieldCount; i++)
        {
            ref var entry = ref entries[i];
            values[i] = entry.Converter is not null
                ? entry.Converter(source.GetValue(entry.SourceIndex))
                : source.GetValue(entry.SourceIndex);
        }
        return fieldCount;
    }

    public bool GetBoolean(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (bool)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetBoolean(entry.SourceIndex);
    }

    public byte GetByte(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (byte)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetByte(entry.SourceIndex);
    }

    public char GetChar(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (char)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetChar(entry.SourceIndex);
    }

    public short GetInt16(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (short)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetInt16(entry.SourceIndex);
    }

    public int GetInt32(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (int)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetInt32(entry.SourceIndex);
    }

    public long GetInt64(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (long)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetInt64(entry.SourceIndex);
    }

    public float GetFloat(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (float)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetFloat(entry.SourceIndex);
    }

    public double GetDouble(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (double)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetDouble(entry.SourceIndex);
    }

    public decimal GetDecimal(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (decimal)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetDecimal(entry.SourceIndex);
    }

    public DateTime GetDateTime(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (DateTime)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetDateTime(entry.SourceIndex);
    }

    public Guid GetGuid(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (Guid)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetGuid(entry.SourceIndex);
    }

    public string GetString(int i)
    {
        ref var entry = ref entries[i];
        return entry.Converter is not null
            ? (string)entry.Converter(source.GetValue(entry.SourceIndex))
            : source.GetString(entry.SourceIndex);
    }

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferOffset, int length)
    {
        ref var entry = ref entries[i];
        if (entry.Converter is not null)
        {
            var array = (byte[])entry.Converter(source.GetValue(entry.SourceIndex));
            var count = Math.Min(length, array.Length - (int)fieldOffset);
            if (count > 0)
            {
                array.AsSpan((int)fieldOffset, count).CopyTo(buffer);
            }
            return count;
        }

        return source.GetBytes(entry.SourceIndex, fieldOffset, buffer, bufferOffset, length);
    }

    public long GetChars(int i, long fieldOffset, char[]? buffer, int bufferOffset, int length)
    {
        ref var entry = ref entries[i];
        if (entry.Converter is not null)
        {
            var array = (char[])entry.Converter(source.GetValue(entry.SourceIndex));
            var count = Math.Min(length, array.Length - (int)fieldOffset);
            if (count > 0)
            {
                array.AsSpan((int)fieldOffset, count).CopyTo(buffer);
            }
            return count;
        }

        return source.GetChars(entry.SourceIndex, fieldOffset, buffer, bufferOffset, length);
    }
}
