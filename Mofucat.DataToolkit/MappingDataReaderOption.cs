namespace Mofucat.DataToolkit;

internal sealed class MappingDataColumn
{
    public int? Index { get; set; }

    public string? Name { get; set; }

    public Type? ConvertType { get; set; }

    public Func<object, object>? Converter { get; set; }
}

public sealed class MappingDataReaderOption
{
    internal List<MappingDataColumn> Columns { get; } = new();

    // TODO
}
