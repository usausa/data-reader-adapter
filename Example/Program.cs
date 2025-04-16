using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Data;
//using System.Globalization;

//using CsvHelper;

using Microsoft.Data.SqlClient;

using Mofucat.DataToolkit;

using MySqlConnector;

using Smart.Data.Mapper;

var rootCommand = new RootCommand("Example");

//--------------------------------------------------------------------------------
// Object
//--------------------------------------------------------------------------------
var objectCommand = new Command("object", "Object example");
rootCommand.Add(objectCommand);

var objectImportCommand = new Command("imp", "Import example");
objectCommand.Add(objectImportCommand);

objectImportCommand.Add(new Command("my", "Load to MySQL")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new ObjectDataReader<Data>(DataHelper.CreateObjectList());
        await DataHelper.ImportToMySql(reader);
    })
});

objectImportCommand.Add(new Command("sql", "Load to SQL Server")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new ObjectDataReader<Data>(DataHelper.CreateObjectList());
        await DataHelper.ImportToSql(reader);
    })
});

var objectExportCommand = new Command("exp", "Export example");
objectCommand.Add(objectExportCommand);

// TODO

//--------------------------------------------------------------------------------
// Csv
//--------------------------------------------------------------------------------

//var csvCommand = new Command("csv", "CSV example");
//rootCommand.Add(csvCommand);

//var csvImportCommand = new Command("imp", "Import example");
//csvCommand.Add(csvImportCommand);

//csvImportCommand.Add(new Command("my", "Load to MySQL")
//{
//    Handler = CommandHandler.Create(static async () =>
//    {
//        // TODO fix convert ?
//        using var reader = new CsvDataReader(DataHelper.CreateCsvOption(), DataHelper.CreateCsvReader());
//        await DataHelper.ImportToMySql(reader);
//    })
//});

//csvImportCommand.Add(new Command("sql", "Load to SQL Server")
//{
//    Handler = CommandHandler.Create(static async () =>
//    {
//        using var reader = new CsvDataReader(DataHelper.CreateCsvOption(), DataHelper.CreateCsvReader());
//        await DataHelper.ImportToSql(reader);
//    })
//});

//var csvExportCommand = new Command("exp", "Export example");
//csvCommand.Add(csvExportCommand);

// TODO

//--------------------------------------------------------------------------------
// Avro
//--------------------------------------------------------------------------------

var avroCommand = new Command("avro", "Avro example");
rootCommand.Add(avroCommand);

var avroImportCommand = new Command("imp", "Import example");
avroCommand.Add(avroImportCommand);

avroImportCommand.Add(new Command("my", "Load to MySQL")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new AvroDataReader(DataHelper.CreateAvroOption(), File.OpenRead("data.avro"));
        await DataHelper.ImportToMySql(reader);
    })
});

avroImportCommand.Add(new Command("sql", "Load to SQL Server")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new AvroDataReader(DataHelper.CreateAvroOption(), File.OpenRead("data.avro"));
        await DataHelper.ImportToSql(reader);
    })
});

var avroExportCommand = new Command("exp", "Export example");
avroCommand.Add(avroExportCommand);

// TODO

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

    public string Name { get; set; } = default!;

    public string? Option { get; set; }

    public bool Flag { get; set; }

    public DateTime CreateAt { get; set; }
}

internal static class DataHelper
{
    //--------------------------------------------------------------------------------
    // Import
    //--------------------------------------------------------------------------------

    public static async ValueTask ImportToMySql(IDataReader reader)
    {
        await using var con = new MySqlConnection("Server=mysql-server;Database=test;User Id=test;Password=test;AllowLoadLocalInfile=true");
        await con.ExecuteAsync("TRUNCATE TABLE data");

        await con.OpenAsync();
        var loader = new MySqlBulkCopy(con)
        {
            DestinationTableName = "data"
        };
        await loader.WriteToServerAsync(reader);
    }

    public static async ValueTask ImportToSql(IDataReader reader)
    {
        await using var con = new SqlConnection("Server=mssql-server;Database=test;User Id=test;Password=test;TrustServerCertificate=true");
        await con.ExecuteAsync("TRUNCATE TABLE Data");

        await con.OpenAsync();
        using var loader = new SqlBulkCopy(con);
        loader.DestinationTableName = "Data";
        await loader.WriteToServerAsync(reader);
    }

    //--------------------------------------------------------------------------------
    // CSV
    //--------------------------------------------------------------------------------

    //private const string Content =
    //    "Col1,Col2,Col3,Col4,Col5,Col6\n" +
    //    "1,30,Data-1,option,true,2000-12-31 23:59:59\n" +
    //    "2,,Data-2,,false,2000-12-31 23:59:59";

    //public static CsvReader CreateCsvReader() =>
    //    new(new StringReader(Content), CultureInfo.InvariantCulture);

    //public static CsvDataReaderOption CreateCsvOption()
    //{
    //    var option = new CsvDataReaderOption();
    //    option.AddColumn("Col1");
    //    option.AddColumn("Col3");
    //    option.AddColumn("Col4", emptyAsNull: true);
    //    option.AddColumn("Col5");
    //    option.AddColumn("Col6");
    //    return option;
    //}

    //--------------------------------------------------------------------------------
    // Object
    //--------------------------------------------------------------------------------

    public static List<Data> CreateObjectList() =>
    [
        new() { Id = 1, Name = "Data-1", Option = "option", Flag = true, CreateAt = DateTime.Now },
        new() { Id = 2, Name = "Data-2", Flag = false, CreateAt = DateTime.Now }
    ];

    //--------------------------------------------------------------------------------
    // Avro
    //--------------------------------------------------------------------------------

    public static AvroDataReaderOption CreateAvroOption()
    {
        var option = new AvroDataReaderOption();
        option.AddConverter<long, DateTime>(s => s == "create_at" ? x => new DateTime(x) : null);
        return option;
    }
}
