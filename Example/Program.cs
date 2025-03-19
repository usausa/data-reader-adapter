using DataReaderAdapter;

using Microsoft.Data.SqlClient;

using MySqlConnector;

using Smart.Data.Mapper;

var list = new List<Data>
{
    new() { Id = 1, Value = "A" },
    new() { Id = 2 }
};

#pragma warning disable CA1031
try
{
    await using var con1 = new SqlConnection("Server=mssql-server;Database=test;User Id=test;Password=test;TrustServerCertificate=true");
    using var r = await con1.ExecuteReaderAsync("select * from Data2");
    while (r.Read())
    {
        Console.WriteLine(r.GetDataTypeName(0));
        Console.WriteLine(r.GetDataTypeName(1));
    }
    r.Close();

    await con1.ExecuteAsync("truncate table Data2");

    using var reader1 = new ObjectDataReaderAdapter<Data>(new ObjectDataReaderOption<Data>(), list);

    await con1.OpenAsync();
    using var loader1 = new SqlBulkCopy(con1);
    loader1.DestinationTableName = "Data2";
    await loader1.WriteToServerAsync(reader1);

    await using var con2 = new MySqlConnection("Server=mysql-server;Database=test;User Id=test;Password=test;AllowLoadLocalInfile=true");
    await con2.ExecuteAsync("truncate table Data2");

    using var reader2 = new ObjectDataReaderAdapter<Data>(new ObjectDataReaderOption<Data>(), list);

    await con2.OpenAsync();
    var loader2 = new MySqlBulkCopy(con2)
    {
        DestinationTableName = "Data2"
    };
    await loader2.WriteToServerAsync(reader2);
}
catch (Exception ex)
{
    Console.WriteLine(ex);
}
#pragma warning restore CA1031

internal class Data
{
    public int Id { get; set; }

    public string? Value { get; set; }
}
