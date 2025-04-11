namespace DataReaderAdapter;

public sealed class AvroDataReaderOption
{
    private readonly List<(Type Type, Func<string, Type, Func<object, object?>?> Factory)> entries = new();

    internal (Type Type, Func<object, object?> Factory)? ResolveConverter(string name, Type type)
    {
        foreach (var entry in entries)
        {
            var converter = entry.Factory(name, type);
            if (converter != null)
            {
                return (entry.Type, converter);
            }
        }
        return null;
    }

    public void AddConverter<T>(Func<string, Type, Func<object, T>?> factory)
    {
        entries.Add((Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T), (s, t) =>
        {
            var f = factory(s, t);
            if (f is null)
            {
                return null;
            }

            return x => f(x);
        }));
    }
}
