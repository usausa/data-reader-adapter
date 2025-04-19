using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Data;
using System.Globalization;

using CsvHelper;
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

//--------------------------------------------------------------------------------
// Mapping
//--------------------------------------------------------------------------------

var mapCommand = new Command("map", "Mapping example");
rootCommand.Add(mapCommand);

var mapImportCommand = new Command("imp", "Import example");
mapCommand.Add(mapImportCommand);

mapImportCommand.Add(new Command("my", "Load to MySQL")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new MappingDataReader(DataHelper.CreateCsvOption(), DataHelper.CreateCsvReader());
        await DataHelper.ImportToMySql(reader);
    })
});

mapImportCommand.Add(new Command("sql", "Load to SQL Server")
{
    Handler = CommandHandler.Create(static async () =>
    {
        using var reader = new MappingDataReader(DataHelper.CreateCsvOption(), DataHelper.CreateCsvReader());
        await DataHelper.ImportToSql(reader);
    })
});

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
    // Mapping
    //--------------------------------------------------------------------------------

    private const string Content =
        "Col1,Col2,Col3,Col4,Col5,Col6\n" +
        "1,30,Data-1,option,true,2000-12-31 23:59:59\n" +
        "2,,Data-2,,false,2000-12-31 23:59:59";

#pragma warning disable CA2000
    public static CsvDataReader CreateCsvReader() =>
        new(new CsvReader(new StringReader(Content), CultureInfo.InvariantCulture));
#pragma warning restore CA2000

    public static MappingDataReaderOption CreateCsvOption()
    {
        var option = new MappingDataReaderOption();
        option.AddColumn("Col1");
        option.AddColumn("Col3");
        option.AddColumn<string, string?>("Col4", static x => String.IsNullOrEmpty(x) ? null : x);
        option.AddColumn<string, bool>("Col5", Boolean.Parse);
        option.AddColumn<string, DateTime>("Col6", DateTime.Parse);
        return option;
    }

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
