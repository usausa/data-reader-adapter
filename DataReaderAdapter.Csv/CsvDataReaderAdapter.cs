namespace DataReaderAdapter;

using System.Data;

using CsvHelper;

// TODO
#pragma warning disable CA1725
public sealed class CsvDataReaderAdapter : IDataReader
{
    private readonly CsvReader reader;

    private readonly int[] indexes;

    public int FieldCount => indexes.Length;

    public int Depth => throw new NotSupportedException();

    public bool IsClosed => false;

    public int RecordsAffected => -1;

    public object this[int i] => throw new NotSupportedException();

    public object this[string name] => throw new NotSupportedException();

    public CsvDataReaderAdapter(CsvReader reader, IEnumerable<string> columns)
    {
        this.reader = reader;
        reader.Read();
        reader.ReadHeader();
        indexes = columns.Select(x => reader.GetFieldIndex(x)).ToArray();
    }

    public void Dispose()
    {
        reader.Dispose();
    }

    public void Close()
    {
    }

    public bool Read() => reader.Read();

    public bool NextResult() => throw new NotSupportedException();

    public bool IsDBNull(int i) => throw new NotSupportedException();

    public object GetValue(int i) => throw new NotSupportedException();

    public int GetValues(object[] values)
    {
        for (var i = 0; i < indexes.Length; i++)
        {
            values[i] = reader.GetField(indexes[i])!;
        }

        return indexes.Length;
    }

    public IDataReader GetData(int i) => throw new NotSupportedException();

    public DataTable GetSchemaTable() => throw new NotSupportedException();

    public string GetDataTypeName(int i) => throw new NotSupportedException();

    public Type GetFieldType(int i) => throw new NotSupportedException();

    public string GetName(int i) => throw new NotSupportedException();

    public int GetOrdinal(string name) => throw new NotSupportedException();

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

    public string GetString(int i) => throw new NotSupportedException();
}
