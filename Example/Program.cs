using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Data;
using System.Globalization;

using CsvHelper;

using DataReaderAdapter;

using Microsoft.Data.SqlClient;

using MySqlConnector;

using Smart.Data.Mapper;

var rootCommand = new RootCommand("Example");

//--------------------------------------------------------------------------------
// Csv
//--------------------------------------------------------------------------------

var csvCommand = new Command("csv", "CSV example");
rootCommand.Add(csvCommand);

csvCommand.Add(new Command("my", "Load to MySQL")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new CsvDataReaderAdapter(DataHelper.CreateCsvOption(), DataHelper.CreateCsvReader());
        await DataHelper.ImportToMySql(reader);
    })
});

csvCommand.Add(new Command("sql", "Load to SQL Server")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new CsvDataReaderAdapter(DataHelper.CreateCsvOption(), DataHelper.CreateCsvReader());
        await DataHelper.ImportToSql(reader);
    })
});

//--------------------------------------------------------------------------------
// Object
//--------------------------------------------------------------------------------
var objectCommand = new Command("object", "CSV example");
rootCommand.Add(objectCommand);

objectCommand.Add(new Command("my", "Load to MySQL")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new ObjectDataReaderAdapter<Data>(DataHelper.CreateObjectList());
        await DataHelper.ImportToMySql(reader);
    })
});

objectCommand.Add(new Command("sql", "Load to SQL Server")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new ObjectDataReaderAdapter<Data>(DataHelper.CreateObjectList());
        await DataHelper.ImportToSql(reader);
    })
});

//--------------------------------------------------------------------------------
// Run
//--------------------------------------------------------------------------------
var result = await rootCommand.InvokeAsync(args).ConfigureAwait(false);
#if DEBUG
Console.ReadLine();
#endif
return result;

//--------------------------------------------------------------------------------
// Data
//--------------------------------------------------------------------------------

internal sealed class Data
{
    public int Id { get; set; }

    public string? Value { get; set; }
}

internal static class DataHelper
{
    //--------------------------------------------------------------------------------
    // Import
    //--------------------------------------------------------------------------------

    public static async ValueTask ImportToMySql(IDataReader reader)
    {
        await using var con = new MySqlConnection("Server=mysql-server;Database=test;User Id=test;Password=test;AllowLoadLocalInfile=true");
        await con.ExecuteAsync("TRUNCATE TABLE import_data");

        await con.OpenAsync();
        var loader = new MySqlBulkCopy(con)
        {
            DestinationTableName = "import_data"
        };
        await loader.WriteToServerAsync(reader);
    }

    public static async ValueTask ImportToSql(IDataReader reader)
    {
        await using var con = new SqlConnection("Server=mssql-server;Database=test;User Id=test;Password=test;TrustServerCertificate=true");
        await con.ExecuteAsync("TRUNCATE TABLE ImportData");

        await con.OpenAsync();
        using var loader = new SqlBulkCopy(con);
        loader.DestinationTableName = "ImportData";
        await loader.WriteToServerAsync(reader);
    }

    //--------------------------------------------------------------------------------
    // CSV
    //--------------------------------------------------------------------------------

    private const string Content =
        "Col1,Col2,Col3,Col4\n" +
        "1,30,Abc,true\n" +
        "2,,,false";

    public static CsvReader CreateCsvReader() =>
        new(new StringReader(Content), CultureInfo.InvariantCulture);

    public static CsvDataReaderOption CreateCsvOption()
    {
        var option = new CsvDataReaderOption();
        option.AddColumn("Col1");
        option.AddColumn("Col3", emptyAsNull: true);
        return option;
    }

    //--------------------------------------------------------------------------------
    // Object
    //--------------------------------------------------------------------------------

    public static List<Data> CreateObjectList() =>
    [
        new() { Id = 1, Value = "A" },
        new() { Id = 2 }
    ];
}
