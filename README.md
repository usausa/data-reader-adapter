# DataReaderAdapter

## ObjectDataReaderAdapter

[![NuGet](https://img.shields.io/nuget/v/DataReaderAdapter.Object.svg)](https://www.nuget.org/packages/DataReaderAdapter.Object)

```csharp
// Object
var list = new List<Data>
[
    new() { Id = 1, Value = "Name-1" },
    new() { Id = 2 }
];

// ReaderAdapter 
using var reader = new ObjectDataReaderAdapter<Data>(list);

// BulkCopy
await using var con = new SqlConnection(ConnectionString);
await con.OpenAsync();
using var loader = new SqlBulkCopy(con);
loader.DestinationTableName = "Data";
await loader.WriteToServerAsync(reader);
```

## CsvDataReaderAdapter

[![NuGet](https://img.shields.io/nuget/v/DataReaderAdapter.Csv.svg)](https://www.nuget.org/packages/DataReaderAdapter.Csv)

```csharp
// CSV
var content = 
    "Col1,Col2,Col3\n" +
    "1,10,Name-1\n" +
    "2,20,\n";
using var csv = new CsvReader(new StringReader(content), CultureInfo.InvariantCulture);

// ReaderAdapter 
var option = new CsvDataReaderOption();
option.AddColumn("Col1");
option.AddColumn("Col3", emptyAsNull: true);
using var reader = new CsvDataReaderAdapter(option, csv);

// BulkCopy
await using var con = new SqlConnection(ConnectionString);
await con.OpenAsync();
using var loader = new SqlBulkCopy(con);
loader.DestinationTableName = "Data";
await loader.WriteToServerAsync(reader);
```
