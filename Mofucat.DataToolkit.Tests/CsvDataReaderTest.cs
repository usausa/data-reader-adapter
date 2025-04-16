//// ReSharper disable UseUtf8StringLiteral
//namespace Mofucat.DataToolkit.Tests;

//using System.Globalization;

//using CsvHelper;

//public class CsvDataReaderTest
//{
//    [Fact]
//    public void TestBasic()
//    {
//        var content =
//            "Col1,Col2,Col3,Col4,Col5\n" +
//            "1,,10,,A\n" +
//            "2,,,,B\n" +
//            "3,,30,,";
//        using var csv = new CsvReader(new StringReader(content), CultureInfo.InvariantCulture);

//        var option = new CsvDataReaderOption();
//        option.AddColumn("Col1");
//        option.AddColumn("Col3", emptyAsNull: true);
//        option.AddColumn("Col5", emptyAsNull: true);

//        using var reader = new CsvDataReader(option, csv);

//        // Assert
//        Assert.Equal(3, reader.FieldCount);

//        var col1Index = reader.GetOrdinal("Col1");
//        var col3Index = reader.GetOrdinal("Col3");
//        var col5Index = reader.GetOrdinal("Col5");
//        Assert.Equal(0, col1Index);
//        Assert.Equal(1, col3Index);
//        Assert.Equal(2, col5Index);

//        // 1st
//        Assert.True(reader.Read());

//        Assert.False(reader.IsDBNull(col3Index));
//        Assert.False(reader.IsDBNull(col5Index));

//        var values = new object[reader.FieldCount];
//        reader.GetValues(values);
//        Assert.Equal("1", values[col1Index]);
//        Assert.Equal("10", values[col3Index]);
//        Assert.Equal("A", values[col5Index]);

//        // 2nd
//        Assert.True(reader.Read());

//        Assert.True(reader.IsDBNull(col3Index));
//        Assert.False(reader.IsDBNull(col5Index));

//        Assert.Equal("2", reader.GetValue(col1Index));
//        Assert.Equal(DBNull.Value, reader.GetValue(col3Index));
//        Assert.Equal("B", reader.GetValue(col5Index));

//        // 3rd
//        Assert.True(reader.Read());

//        Assert.False(reader.IsDBNull(col3Index));
//        Assert.True(reader.IsDBNull(col5Index));

//        Assert.Equal("3", reader.GetValue(col1Index));
//        Assert.Equal("30", reader.GetValue(col3Index));
//        Assert.Equal(DBNull.Value, reader.GetValue(col5Index));

//        // Completed
//        Assert.False(reader.Read());
//    }

//    [Fact]
//    public void TestConvert()
//    {
//        var content =
//            "Col1\n" +
//            "true\n" +
//            "1\n" +
//            "A\n" +
//            "2000-12-31 23:59:59\n" +
//            "00000000-0000-0000-0000-000000000000\n" +
//            "00112233445566778899AABBCCDDEEFF\n" +
//            "0123456789\n";
//        using var csv = new CsvReader(new StringReader(content), CultureInfo.InvariantCulture);

//        var option = new CsvDataReaderOption();
//        option.AddColumn("Col1");

//        using var reader = new CsvDataReader(option, csv);

//        // Assert
//        Assert.True(reader.Read());
//        Assert.True(reader.GetBoolean(0));

//        Assert.True(reader.Read());
//        Assert.Equal(1, reader.GetByte(0));
//        Assert.Equal(1, reader.GetInt16(0));
//        Assert.Equal(1, reader.GetInt32(0));
//        Assert.Equal(1, reader.GetInt64(0));
//        Assert.Equal(1, reader.GetFloat(0));
//        Assert.Equal(1, reader.GetDouble(0));
//        Assert.Equal(1, reader.GetDecimal(0));

//        Assert.True(reader.Read());
//        Assert.Equal('A', reader.GetChar(0));
//        Assert.Equal("A", reader.GetString(0));

//        Assert.True(reader.Read());
//        Assert.Equal(new DateTime(2000, 12, 31, 23, 59, 59), reader.GetDateTime(0));

//        Assert.True(reader.Read());
//        Assert.Equal(Guid.Empty, reader.GetGuid(0));

//        Assert.True(reader.Read());
//        var bytes = new byte[4];
//        reader.GetBytes(0, 4, bytes, 0, bytes.Length);
//        Assert.Equal(new byte[] { 0x44, 0x55, 0x66, 0x77 }, bytes);

//        Assert.True(reader.Read());
//        var chars = new char[4];
//        reader.GetChars(0, 2, chars, 0, chars.Length);
//        Assert.Equal(['2', '3', '4', '5'], chars);
//    }
//}
