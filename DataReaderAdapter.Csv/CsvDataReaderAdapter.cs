namespace DataReaderAdapter;

using System.Data;
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

    // TODO support ?

    public bool GetBoolean(int i) => throw new NotSupportedException();

    public byte GetByte(int i) => throw new NotSupportedException();

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();

    public char GetChar(int i) => throw new NotSupportedException();

    public long GetChars(int i, long fieldOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();

    public DateTime GetDateTime(int i) => throw new NotSupportedException();

    public decimal GetDecimal(int i) => throw new NotSupportedException();

    public double GetDouble(int i) => throw new NotSupportedException();

    public float GetFloat(int i) => throw new NotSupportedException();

    public Guid GetGuid(int i) => throw new NotSupportedException();

    public short GetInt16(int i) => throw new NotSupportedException();

    public int GetInt32(int i) => throw new NotSupportedException();

    public long GetInt64(int i) => throw new NotSupportedException();

    public string GetString(int i) => GetStringValue(i) ?? string.Empty;
}
