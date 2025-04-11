namespace DataReaderAdapter;

public sealed class AvroDataReaderOption
{
    private readonly List<Func<string, Type, Func<object, object?>?>> converterFactories = new();

    internal Func<object, object?>? ResolveConverter(string name, Type type)
    {
        foreach (var factory in converterFactories)
        {
            var converter = factory(name, type);
            if (converter != null)
            {
                return converter;
            }
        }
        return null;
    }

    public void AddConverter(Func<string, Type, Func<object, object?>?> factory)
    {
        converterFactories.Add(factory);
    }
}
