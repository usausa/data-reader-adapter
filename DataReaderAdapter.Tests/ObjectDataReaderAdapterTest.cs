namespace DataReaderAdapter.Tests;

public class ObjectDataReaderAdapterTest
{
    [Fact]
    public void TestBasic()
    {
        var list = new List<Data>
        {
            new() { IntValue = 1, NullableIntValue = 10, StringValue = "A" },
            new() { IntValue = 2, StringValue = "B" },
            new() { IntValue = 3, NullableIntValue = 30 }
        };

        using var reader = new ObjectDataReaderAdapter<Data>(list);

        // Assert
        Assert.Equal(3, reader.FieldCount);

        var intIndex = reader.GetOrdinal(nameof(Data.IntValue));
        var nullableIntIndex = reader.GetOrdinal(nameof(Data.NullableIntValue));
        var stringIndex = reader.GetOrdinal(nameof(Data.StringValue));
        Assert.Equal(0, intIndex);
        Assert.Equal(1, nullableIntIndex);
        Assert.Equal(2, stringIndex);

        // 1st
        Assert.True(reader.Read());

        Assert.False(reader.IsDBNull(nullableIntIndex));
        Assert.False(reader.IsDBNull(stringIndex));

        var values = new object[reader.FieldCount];
        reader.GetValues(values);
        Assert.Equal(1, values[intIndex]);
        Assert.Equal(10, values[nullableIntIndex]);
        Assert.Equal("A", values[stringIndex]);

        // 2nd
        Assert.True(reader.Read());

        Assert.True(reader.IsDBNull(nullableIntIndex));
        Assert.False(reader.IsDBNull(stringIndex));

        Assert.Equal(2, reader.GetValue(intIndex));
        Assert.Equal(DBNull.Value, reader.GetValue(nullableIntIndex));
        Assert.Equal("B", reader.GetValue(stringIndex));

        // 3rd
        Assert.True(reader.Read());

        Assert.False(reader.IsDBNull(nullableIntIndex));
        Assert.True(reader.IsDBNull(stringIndex));

        Assert.Equal(3, reader.GetValue(intIndex));
        Assert.Equal(30, reader.GetValue(nullableIntIndex));
        Assert.Equal(DBNull.Value, reader.GetValue(stringIndex));

        // Completed
        Assert.False(reader.Read());
    }

    private sealed class Data
    {
        public int IntValue { get; set; }

        public int? NullableIntValue { get; set; }

        public string? StringValue { get; set; }
    }
}
