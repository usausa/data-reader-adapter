namespace DataReaderAdapter;

using System;
using System.Data;
using System.Globalization;
using System.Runtime.CompilerServices;

using Avro;
using Avro.File;
using Avro.Generic;

#pragma warning disable CA1725
public sealed class AvroDataReaderAdapter : IDataReader
{
    private static readonly AvroDataReaderOption DefaultOption = new();

    private readonly string[] names;

    private readonly Type[] types;

    private readonly Func<object, object?>?[] converters;

    private readonly object?[] currentValues;

    private readonly IFileReader<GenericRecord> reader;

    private GenericRecord currentRecord = default!;

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

    public AvroDataReaderAdapter(Stream stream)
        : this(DefaultOption, stream)
    {
    }

    public AvroDataReaderAdapter(AvroDataReaderOption option, Stream stream)
    {
        reader = DataFileReader<GenericRecord>.OpenReader(stream);
        var scheme = (RecordSchema)reader.GetSchema();

        FieldCount = scheme.Fields.Count;
        names = new string[scheme.Fields.Count];
        types = new Type[scheme.Fields.Count];
        converters = new Func<object, object?>?[scheme.Fields.Count];
        currentValues = new object?[scheme.Fields.Count];

        for (var i = 0; i < scheme.Fields.Count; i++)
        {
            var field = scheme.Fields[i];
            var type = ResolveType(field);
            var converter = option.ResolveConverter(field.Name, type);

            names[i] = field.Name;
            types[i] = converter?.Type ?? type;
            converters[i] = converter?.Factory;
        }
    }

    private static Type ResolveType(Field field)
    {
        if (field.Schema is PrimitiveSchema primitiveSchema)
        {
            return ConvertType(field.Name, primitiveSchema.Tag);
        }
        if (field.Schema is UnionSchema unionSchema)
        {
            foreach (var schema in unionSchema.Schemas)
            {
                if ((schema is PrimitiveSchema ps) && (ps.Tag != Schema.Type.Null))
                {
                    return ConvertType(field.Name, ps.Tag);
                }
            }
        }

        throw new NotSupportedException($"Unsupported Avro type. field=[{field.Name}]");

        Type ConvertType(string name, Schema.Type type) => type switch
        {
            Schema.Type.Boolean => typeof(bool),
            Schema.Type.Int => typeof(int),
            Schema.Type.Long => typeof(long),
            Schema.Type.Float => typeof(float),
            Schema.Type.Double => typeof(double),
            Schema.Type.Bytes => typeof(byte[]),
            Schema.Type.String => typeof(string),
            Schema.Type.Fixed => typeof(byte[]),
            _ => throw new NotSupportedException($"Unsupported Avro type. field=[{name}], type=[{type}]")
        };
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

    //--------------------------------------------------------------------------------
    // Iterator
    //--------------------------------------------------------------------------------

    public bool Read()
    {
        if (!reader.HasNext())
        {
            return false;
        }

        currentRecord = reader.Next();

        for (var i = 0; i < names.Length; i++)
        {
            var value = currentRecord[names[i]];
            var converter = converters[i];
            currentValues[i] = converter is not null ? converter(value) : value;
        }

        return true;
    }

    public bool NextResult() => false;

    //--------------------------------------------------------------------------------
    // Metadata
    //--------------------------------------------------------------------------------

    public IDataReader GetData(int i) => throw new NotSupportedException();

    public DataTable GetSchemaTable() => throw new NotSupportedException();

    public string GetDataTypeName(int i) => types[i].Name;

    public Type GetFieldType(int i) => types[i];

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
    private object? GetObjectValue(int i) => currentValues[i];

    public bool IsDBNull(int i) => GetObjectValue(i) is null;

    public object GetValue(int i) => GetObjectValue(i) ?? DBNull.Value;

    public int GetValues(object[] values)
    {
        for (var i = 0; i < names.Length; i++)
        {
            values[i] = GetValue(i);
        }
        return names.Length;
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
