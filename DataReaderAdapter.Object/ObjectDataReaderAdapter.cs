namespace DataReaderAdapter;

using System.Data;
using System.Reflection;
using System.Runtime.CompilerServices;

#pragma warning disable CA1725
public sealed class ObjectDataReaderAdapter<T> : IDataReader
{
    private static readonly ObjectDataReaderOption<T> DefaultOption = new();

    private readonly PropertyInfo[] properties;

    private readonly Func<T, object?>[] accessors;

    private readonly IEnumerator<T> source;

    //--------------------------------------------------------------------------------
    // Property
    //--------------------------------------------------------------------------------

    public int FieldCount => accessors.Length;

    public int Depth => 0;

    public bool IsClosed { get; private set; }

    public int RecordsAffected => -1;

    public object this[int i] => GetValue(i);

    public object this[string name] => GetValue(GetOrdinal(name));

    //--------------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------------

    public ObjectDataReaderAdapter(IEnumerable<T> source)
        : this(DefaultOption, source)
    {
    }

    public ObjectDataReaderAdapter(ObjectDataReaderOption<T> option, IEnumerable<T> source)
    {
        properties = option.PropertySelector().ToArray();
        accessors = properties.Select(option.AccessorFactory).ToArray();
        this.source = source.GetEnumerator();
    }

    public void Dispose()
    {
        Close();
    }

    public void Close()
    {
        if (!IsClosed)
        {
            source.Dispose();
            IsClosed = true;
        }
    }

    //--------------------------------------------------------------------------------
    // Iterator
    //--------------------------------------------------------------------------------

    public bool Read() => source.MoveNext();

    public bool NextResult() => false;

    //--------------------------------------------------------------------------------
    // Metadata
    //--------------------------------------------------------------------------------

    public IDataReader GetData(int i) => throw new NotSupportedException();

    public DataTable GetSchemaTable() => throw new NotSupportedException();

    public string GetDataTypeName(int i) => properties[i].Name;

    public Type GetFieldType(int i) => properties[i].PropertyType;

    public string GetName(int i) => properties[i].Name;

    public int GetOrdinal(string name)
    {
        for (var i = 0; i < properties.Length; i++)
        {
            if (String.Equals(properties[i].Name, name, StringComparison.OrdinalIgnoreCase))
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
    private object? GetObjectValue(int i) => accessors[i](source.Current!);

    public bool IsDBNull(int i) => GetObjectValue(i) is null;

    public object GetValue(int i) => GetObjectValue(i) ?? DBNull.Value;

    public int GetValues(object[] values)
    {
        for (var i = 0; i < accessors.Length; i++)
        {
            values[i] = GetObjectValue(i) ?? DBNull.Value;
        }
        return accessors.Length;
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

    public string GetString(int i) => throw new NotSupportedException();
}
