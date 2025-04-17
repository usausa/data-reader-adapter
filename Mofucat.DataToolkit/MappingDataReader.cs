namespace Mofucat.DataToolkit;

using System;
using System.Buffers;
using System.Data;

#pragma warning disable CA1725
public sealed class MappingDataReader : IDataReader
{
    private struct Entry
    {
        public readonly int SourceIndex;

        public readonly Type? ConvertType;

        public readonly Func<object, object>? Converter;
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

    public string GetDataTypeName(int i) => throw new NotImplementedException();

    public Type GetFieldType(int i) => throw new NotImplementedException();

    public string GetName(int i) => throw new NotImplementedException();

    public int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }

    //--------------------------------------------------------------------------------
    // Value
    //--------------------------------------------------------------------------------

    public bool IsDBNull(int i)
    {
        throw new NotImplementedException();
    }

    public object GetValue(int i)
    {
        throw new NotImplementedException();
    }

    public int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }

    // TODO Optimize？ source equals? / without convert typed or with cast convert ?

    public bool GetBoolean(int i)
    {
        throw new NotImplementedException();
    }

    public byte GetByte(int i)
    {
        throw new NotImplementedException();
    }

    public char GetChar(int i)
    {
        throw new NotImplementedException();
    }

    public short GetInt16(int i)
    {
        throw new NotImplementedException();
    }

    public int GetInt32(int i)
    {
        throw new NotImplementedException();
    }

    public long GetInt64(int i)
    {
        throw new NotImplementedException();
    }

    public float GetFloat(int i)
    {
        throw new NotImplementedException();
    }

    public double GetDouble(int i)
    {
        throw new NotImplementedException();
    }

    public decimal GetDecimal(int i)
    {
        throw new NotImplementedException();
    }

    public DateTime GetDateTime(int i)
    {
        throw new NotImplementedException();
    }

    public Guid GetGuid(int i)
    {
        throw new NotImplementedException();
    }

    public string GetString(int i)
    {
        throw new NotImplementedException();
    }

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
    {
        throw new NotImplementedException();
    }

    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
    {
        throw new NotImplementedException();
    }
}
