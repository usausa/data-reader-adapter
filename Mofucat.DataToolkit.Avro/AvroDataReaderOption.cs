namespace Mofucat.DataToolkit;

public sealed class AvroDataReaderOption
{
    private readonly List<(Type Type, Func<string, Type, Func<object, object?>?> Factory)> entries = new();

    internal (Type Type, Func<object, object?> Factory)? ResolveConverter(string name, Type type)
    {
        for (var i = entries.Count - 1; i >= 0; i--)
        {
            var entry = entries[i];
            var converter = entry.Factory(name, type);
            if (converter is not null)
            {
                return (entry.Type, converter);
            }
        }
        return null;
    }

    public AvroDataReaderOption AddConverter<TSource, TDestination>(Func<string, Func<TSource, TDestination>?> factory)
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
        return this;
    }

    public AvroDataReaderOption ClearConverters()
    {
        entries.Clear();
        return this;
    }

    public static AvroDataReaderOption OfDefault()
    {
        var option = new AvroDataReaderOption();
        option.AddConverter<long, DateTime>(static _ => static x => new DateTime(x, DateTimeKind.Utc).ToLocalTime());
        return option;
    }
}
