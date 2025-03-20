namespace DataReaderAdapter.Tests;

using System.Globalization;

using CsvHelper;

public class CsvDataReaderAdapterTest
{
    [Fact]
    public void TestBasic()
    {
        var content =
            "Col1,Col2,Col3,Col4,Col5\n" +
            "1,,10,,A\n" +
            "2,,,,B\n" +
            "3,,30,,";
        using var csv = new CsvReader(new StringReader(content), CultureInfo.InvariantCulture);

        var option = new CsvDataReaderOption();
        option.AddColumn("Col1");
        option.AddColumn("Col3", emptyAsNull: true);
        option.AddColumn("Col5", emptyAsNull: true);

        using var reader = new CsvDataReaderAdapter(option, csv);

        // Assert
        Assert.Equal(3, reader.FieldCount);

        // 1st
        Assert.True(reader.Read());

        Assert.False(reader.IsDBNull(1));
        Assert.False(reader.IsDBNull(2));

        var values = new object[reader.FieldCount];
        reader.GetValues(values);
        Assert.Equal("1", values[0]);
        Assert.Equal("10", values[1]);
        Assert.Equal("A", values[2]);

        // 2nd
        Assert.True(reader.Read());

        Assert.True(reader.IsDBNull(1));
        Assert.False(reader.IsDBNull(2));

        Assert.Equal("2", reader.GetValue(0));
        Assert.Equal(DBNull.Value, reader.GetValue(1));
        Assert.Equal("B", reader.GetValue(2));

        // 3rd
        Assert.True(reader.Read());

        Assert.False(reader.IsDBNull(1));
        Assert.True(reader.IsDBNull(2));

        Assert.Equal("3", reader.GetValue(0));
        Assert.Equal("30", reader.GetValue(1));
        Assert.Equal(DBNull.Value, reader.GetValue(2));

        // Completed
        Assert.False(reader.Read());
    }
}
