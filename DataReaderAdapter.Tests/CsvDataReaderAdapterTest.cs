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

        var col1Index = reader.GetOrdinal("Col1");
        var col3Index = reader.GetOrdinal("Col3");
        var col5Index = reader.GetOrdinal("Col5");
        Assert.Equal(0, col1Index);
        Assert.Equal(1, col3Index);
        Assert.Equal(2, col5Index);

        // 1st
        Assert.True(reader.Read());

        Assert.False(reader.IsDBNull(col3Index));
        Assert.False(reader.IsDBNull(col5Index));

        var values = new object[reader.FieldCount];
        reader.GetValues(values);
        Assert.Equal("1", values[col1Index]);
        Assert.Equal("10", values[col3Index]);
        Assert.Equal("A", values[col5Index]);

        // 2nd
        Assert.True(reader.Read());

        Assert.True(reader.IsDBNull(col3Index));
        Assert.False(reader.IsDBNull(col5Index));

        Assert.Equal("2", reader.GetValue(col1Index));
        Assert.Equal(DBNull.Value, reader.GetValue(col3Index));
        Assert.Equal("B", reader.GetValue(col5Index));

        // 3rd
        Assert.True(reader.Read());

        Assert.False(reader.IsDBNull(col3Index));
        Assert.True(reader.IsDBNull(col5Index));

        Assert.Equal("3", reader.GetValue(col1Index));
        Assert.Equal("30", reader.GetValue(col3Index));
        Assert.Equal(DBNull.Value, reader.GetValue(col5Index));

        // Completed
        Assert.False(reader.Read());
    }
}
