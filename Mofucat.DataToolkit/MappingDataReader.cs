namespace Mofucat.DataToolkit;

using System;
using System.Buffers;
using System.Data;

#pragma warning disable CA1725
public sealed class MappingDataReader : IDataReader
{
    private struct Mapping
    {
        public readonly int SourceIndex;

        public readonly Type? ConvertType;

        public readonly Func<object, object>? Converter;
    }

    private Mapping[] mappings;

    //--------------------------------------------------------------------------------
    // Property
    //--------------------------------------------------------------------------------

    public int FieldCount => throw new NotImplementedException();

    public int Depth => throw new NotImplementedException();

    public bool IsClosed => throw new NotImplementedException();

    public int RecordsAffected => -1;

    public object this[int i] => GetValue(i);

    public object this[string name] => GetValue(GetOrdinal(name));

    //--------------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------------

    public MappingDataReader()
    {
        mappings = ArrayPool<Mapping>.Shared.Rent(1);
    }

    public void Dispose()
    {
        // TODO source
        //reader.Dispose();
        if (mappings.Length > 0)
        {
            ArrayPool<Mapping>.Shared.Return(mappings, true);
            mappings = [];
        }
    }

    public void Close()
    {
        // TODO source
        //reader.Close();
    }

    //--------------------------------------------------------------------------------
    // Iterator
    //--------------------------------------------------------------------------------

    public bool Read()
    {
        throw new NotImplementedException();
    }

    public bool NextResult()
    {
        throw new NotImplementedException();
    }

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
