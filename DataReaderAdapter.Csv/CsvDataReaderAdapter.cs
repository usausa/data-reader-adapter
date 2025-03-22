namespace DataReaderAdapter;

using System.Data;
using System.Globalization;
using System.Runtime.CompilerServices;

using CsvHelper;

#pragma warning disable CA1725
public sealed class CsvDataReaderAdapter : IDataReader
{
    private readonly int[] indexes;

    private readonly string[] names;

    private readonly bool[] emptyAsNulls;

    private readonly string[] nullValues;

    private readonly CsvReader reader;

    //--------------------------------------------------------------------------------
    // Property
    //--------------------------------------------------------------------------------

    public int FieldCount => indexes.Length;

    public int Depth => 0;

    public bool IsClosed { get; private set; }

    public int RecordsAffected => -1;

    public object this[int i] => GetValue(i);

    public object this[string name] => GetValue(GetOrdinal(name));

    //--------------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------------

    public CsvDataReaderAdapter(CsvDataReaderOption option, CsvReader reader)
    {
        if (option.HasHeaderRecord)
        {
            reader.Read();
            reader.ReadHeader();
        }

        indexes = new int[option.Columns.Count];
        names = new string[option.Columns.Count];
        emptyAsNulls = new bool[option.Columns.Count];
        nullValues = new string[option.Columns.Count];
        for (var i = 0; i < option.Columns.Count; i++)
        {
            var column = option.Columns[i];
            indexes[i] = column.Index ?? reader.GetFieldIndex(column.Name!);
            names[i] = column.Name ?? string.Empty;
            emptyAsNulls[i] = String.IsNullOrEmpty(column.Fallback) && (column.EmptyAsNull ?? option.EmptyAsNull);
            nullValues[i] = column.Fallback ?? string.Empty;
        }

        this.reader = reader;
    }

    public void Dispose()
    {
        Close();
    }

    public void Close()
    {
        if (!IsClosed)
        {
            reader.Dispose();
            IsClosed = true;
        }
    }

    public bool Read() => reader.Read();

    public bool NextResult() => false;

    //--------------------------------------------------------------------------------
    // Metadata
    //--------------------------------------------------------------------------------

    public IDataReader GetData(int i) => throw new NotSupportedException();

    public DataTable GetSchemaTable() => throw new NotSupportedException();

    public string GetDataTypeName(int i) => nameof(String);

    public Type GetFieldType(int i) => typeof(string);

    public string GetName(int i) => names[i];

    public int GetOrdinal(string name)
    {
        for (var i = 0; i < names.Length; i++)
        {
            if (String.Equals(names[i], name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }
        return -1;
    }
    //--------------------------------------------------------------------------------
    // Value
    //--------------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string? GetStringValue(int i)
    {
        var value = reader.GetField(indexes[i]);
        if (!String.IsNullOrEmpty(value))
        {
            return value;
        }
        if (emptyAsNulls[i])
        {
            return null;
        }
        return nullValues[i];
    }

    public bool IsDBNull(int i) => GetStringValue(i) is null;

    public object GetValue(int i) => (object?)GetStringValue(i) ?? DBNull.Value;

    public int GetValues(object[] values)
    {
        for (var i = 0; i < indexes.Length; i++)
        {
            values[i] = GetValue(i);
        }
        return indexes.Length;
    }

    public bool GetBoolean(int i) => Boolean.Parse(GetStringValue(i)!);

    public byte GetByte(int i) => Byte.Parse(GetStringValue(i)!, CultureInfo.InvariantCulture);

    public char GetChar(int i) => Char.Parse(GetStringValue(i)!);

    public decimal GetDecimal(int i) => Decimal.Parse(GetStringValue(i)!, CultureInfo.InvariantCulture);

    public short GetInt16(int i) => Int16.Parse(GetStringValue(i)!, CultureInfo.InvariantCulture);

    public int GetInt32(int i) => Int32.Parse(GetStringValue(i)!, CultureInfo.InvariantCulture);

    public long GetInt64(int i) => Int64.Parse(GetStringValue(i)!, CultureInfo.InvariantCulture);

    public float GetFloat(int i) => Single.Parse(GetStringValue(i)!, CultureInfo.InvariantCulture);

    public double GetDouble(int i) => Double.Parse(GetStringValue(i)!, CultureInfo.InvariantCulture);

    public DateTime GetDateTime(int i) => DateTime.Parse(GetStringValue(i)!, CultureInfo.InvariantCulture);

    public Guid GetGuid(int i) => Guid.Parse(GetStringValue(i)!, CultureInfo.InvariantCulture);

    public string GetString(int i) => GetStringValue(i) ?? string.Empty;

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var array = Convert.FromHexString(GetStringValue(i)!);
        var count = Math.Min(length, array.Length - (int)fieldOffset);
        if (count > 0)
        {
            array.AsSpan((int)fieldOffset, count).CopyTo(buffer);
        }
        return count;
    }

    public long GetChars(int i, long fieldOffset, char[]? buffer, int bufferOffset, int length)
    {
        var span = GetStringValue(i)!.AsSpan();
        var count = Math.Min(length, span.Length - (int)fieldOffset);
        if (count > 0)
        {
            span.Slice((int)fieldOffset, count).CopyTo(buffer);
        }
        return count;
    }
}
