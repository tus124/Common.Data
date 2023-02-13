using System.Data;

namespace Common.Data.Item;

public class Parameter
{
    public string Name { get; set; }
    public object Value { get; set; }
    public DbType? Type { get; set; }
    public virtual ParameterDirection ParameterDirection { get; set; }
    public int? Size { get; set; }
    public string TypeName { get; set; }
}
