namespace DataReaderAdapter;

using System.Data;
using System.Globalization;
using System.Runtime.CompilerServices;

#pragma warning disable CA1725
public sealed class AvroDataReaderAdapter : IDataReader
{
    // TODO

    //--------------------------------------------------------------------------------
    // Property
    //--------------------------------------------------------------------------------

    public int FieldCount { get; }

    public int Depth => 0;

    public bool IsClosed { get; private set; }

    public int RecordsAffected => -1;

    public object this[int i] => GetValue(i);

    public object this[string name] => GetValue(GetOrdinal(name));

    //--------------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------------

    // TODO Stream, option
    public AvroDataReaderAdapter()
    {
        FieldCount = 0;
    }

    public void Dispose()
    {
        Close();
    }

    public void Close()
    {
        if (!IsClosed)
        {
            // TODO ?
            IsClosed = true;
        }
    }

    //--------------------------------------------------------------------------------
    // Iterator
    //--------------------------------------------------------------------------------

    // TODO Next & Next ?
    public bool Read() => true;

    public bool NextResult() => false;

    //--------------------------------------------------------------------------------
    // Metadata
    //--------------------------------------------------------------------------------

    public IDataReader GetData(int i) => throw new NotSupportedException();

    public DataTable GetSchemaTable() => throw new NotSupportedException();

    public string GetDataTypeName(int i) => string.Empty; // TODOproperties[i].Name;

    public Type GetFieldType(int i) => typeof(object); // TODO properties[i].PropertyType;

    public string GetName(int i) => string.Empty; // TODO properties[i].Name;

    public int GetOrdinal(string name)
    {
        //for (var i = 0; i < properties.Length; i++)
        //{
        //    if (String.Equals(properties[i].Name, name, StringComparison.OrdinalIgnoreCase))
        //    {
        //        return i;
        //    }
        //}
        return -1;
    }

    //--------------------------------------------------------------------------------
    // Value
    //--------------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable CA1822
    // ReSharper disable once MemberCanBeMadeStatic.Local
    // ReSharper disable once UnusedParameter.Local
    private object? GetObjectValue(int i) => null; // TODO accessors[i](source.Current!);
#pragma warning restore CA1822

    public bool IsDBNull(int i) => GetObjectValue(i) is null;

    public object GetValue(int i) => GetObjectValue(i) ?? DBNull.Value;

    public int GetValues(object[] values)
    {
        // TODO
        //for (var i = 0; i < accessors.Length; i++)
        //{
        //    values[i] = GetObjectValue(i) ?? DBNull.Value;
        //}
        //return accessors.Length;
        return 0;
    }

    public bool GetBoolean(int i)
    {
        var value = GetObjectValue(i);
        return value is bool t ? t : Convert.ToBoolean(value, CultureInfo.InvariantCulture);
    }

    public byte GetByte(int i)
    {
        var value = GetObjectValue(i);
        return value is byte t ? t : Convert.ToByte(value, CultureInfo.InvariantCulture);
    }

    public char GetChar(int i)
    {
        var value = GetObjectValue(i);
        return value is char t ? t : Convert.ToChar(value, CultureInfo.InvariantCulture);
    }

    public short GetInt16(int i)
    {
        var value = GetObjectValue(i);
        return value is short t ? t : Convert.ToInt16(value, CultureInfo.InvariantCulture);
    }

    public int GetInt32(int i)
    {
        var value = GetObjectValue(i);
        return value is int t ? t : Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    public long GetInt64(int i)
    {
        var value = GetObjectValue(i);
        return value is long t ? t : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    public float GetFloat(int i)
    {
        var value = GetObjectValue(i);
        return value is float t ? t : Convert.ToSingle(value, CultureInfo.InvariantCulture);
    }

    public double GetDouble(int i)
    {
        var value = GetObjectValue(i);
        return value is double t ? t : Convert.ToDouble(value, CultureInfo.InvariantCulture);
    }

    public decimal GetDecimal(int i)
    {
        var value = GetObjectValue(i);
        return value is decimal t ? t : Convert.ToDecimal(value, CultureInfo.InvariantCulture);
    }

    public DateTime GetDateTime(int i)
    {
        var value = GetObjectValue(i);
        return value is DateTime t ? t : Convert.ToDateTime(value, CultureInfo.InvariantCulture);
    }

    public Guid GetGuid(int i)
    {
        // TODO ?
        var value = GetObjectValue(i);
        if (value is Guid t)
        {
            return t;
        }
        if (value is string str)
        {
            return Guid.Parse(str, CultureInfo.InvariantCulture);
        }

        var name = value?.GetType().Name ?? "null";
        throw new NotSupportedException($"Convert to Guid is not supported. type=[{name}]");
    }

    public string GetString(int i)
    {
        var value = GetObjectValue(i);
        return value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture)!;
    }

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferOffset, int length)
    {
        // TODO ?
        var value = GetObjectValue(i);
        if (value is byte[] array)
        {
            var count = Math.Min(length, array.Length - (int)fieldOffset);
            if (count > 0)
            {
                array.AsSpan((int)fieldOffset, count).CopyTo(buffer);
            }
            return count;
        }

        var name = value?.GetType().Name ?? "null";
        throw new NotSupportedException($"Convert to Guid is not supported. type=[{name}]");
    }

    public long GetChars(int i, long fieldOffset, char[]? buffer, int bufferOffset, int length)
    {
        // TODO ?
        var value = GetObjectValue(i);
        if (value is char[] array)
        {
            var count = Math.Min(length, array.Length - (int)fieldOffset);
            if (count > 0)
            {
                array.AsSpan((int)fieldOffset, count).CopyTo(buffer);
            }
            return count;
        }

        var name = value?.GetType().Name ?? "null";
        throw new NotSupportedException($"Convert to Guid is not supported. type=[{name}]");
    }
}
