namespace DataReaderAdapter;

internal sealed class ColumnInfo
{
    public int? Index { get; set; }

    public string? Name { get; set; }

    public bool? EmptyAsNull { get; set; }

    public string? Fallback { get; set; }
}

public sealed class CsvDataReaderOption
{
    internal List<ColumnInfo> Columns { get; } = new();

    public bool HasHeaderRecord { get; set; } = true;

    public bool EmptyAsNull { get; set; }

    public void AddColumn(string name, bool? emptyAsNull = null, string? fallback = null)
    {
        Columns.Add(new ColumnInfo { Name = name, EmptyAsNull = emptyAsNull, Fallback = fallback });
    }

    public void AddColumn(int index, bool? emptyAsNull = null, string? fallback = null)
    {
        Columns.Add(new ColumnInfo { Index = index, EmptyAsNull = emptyAsNull, Fallback = fallback });
    }

    public void AddColumn(int index, string name, bool? emptyAsNull = null, string? fallback = null)
    {
        Columns.Add(new ColumnInfo { Index = index, Name = name, EmptyAsNull = emptyAsNull, Fallback = fallback });
    }
}
