namespace DataToolkit;

using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

// TODO Default selector with attribute

public sealed class ObjectDataReaderOption<T>
{
    private static readonly ConcurrentDictionary<PropertyInfo, Func<T, object?>> Accessors = new();

    public Func<IEnumerable<PropertyInfo>> PropertySelector { get; set; } =
        static () => typeof(T).GetProperties();

    public Func<PropertyInfo, Func<T, object?>> AccessorFactory { get; set; } = DefaultFactory;

    private static Func<T, object?> DefaultFactory(PropertyInfo pi)
    {
        if (!Accessors.TryGetValue(pi, out var func))
        {
            var parameter = Expression.Parameter(typeof(T), "x");
            var body = Expression.Convert(Expression.Property(parameter, pi), typeof(object));
            func = Expression.Lambda<Func<T, object?>>(body, parameter).Compile();
            Accessors[pi] = func;
        }
        return func;
    }
}
