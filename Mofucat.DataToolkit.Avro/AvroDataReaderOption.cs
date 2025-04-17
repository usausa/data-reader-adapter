namespace Mofucat.DataToolkit;

// TODO DateTime default converter

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

    public void AddConverter<TSource, TDestination>(Func<string, Func<TSource, TDestination>?> factory)
    {
        entries.Add((Nullable.GetUnderlyingType(typeof(TDestination)) ?? typeof(TDestination), (s, t) =>
        {
            if (t != typeof(TSource))
            {
                return null;
            }

            var f = factory(s);
            if (f is null)
            {
                return null;
            }

            return x => f((TSource)x);
        }));
    }
}
