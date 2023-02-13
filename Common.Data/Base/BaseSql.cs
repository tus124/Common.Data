using Common.Data.Item;
using Dapper;
using MonkeyCache.FileStore;
using System.Configuration;
using System.Data;

namespace Common.Data.Base;
/// <summary>
/// using dapper to invoke sql queries, store procedures, and etc.
/// </summary>
public abstract class BaseSql
{
    private string cacheKey;
    private List<Parameter> parameterList = new List<Parameter>();
    private TimeSpan cacheDuration = TimeSpan.FromMinutes(15);


    private string AppId => AppDomain.CurrentDomain.FriendlyName;


    // Properties
    public string ConnectionString { get; set; }
    public string Command { get; set; }
    public CommandType? CommandType { get; set; }
    public int? Timeout { get; set; }
    public TimeSpan CacheDuration { get; set; } = TimeSpan.FromMinutes(15);
    public bool CacheResults { get; set; }
    public bool SlideCache { get; set; }
    public bool ClearCacheOnExec { get; set; }
    public string StoreProcedureDataToClear { get; set; }
    public List<Parameter> Parameters { get; set; } 




    public BaseSql()
    {
        Barrel.ApplicationId = this.AppId;
    }

    public BaseSql(string connectionString)
    {
        this.ConnectionString = connectionString;
    }

    public BaseSql(string connectionString, string command) : this(connectionString)
    {
        this.Command = command;
    }
     
    public BaseSql(string connectionString, string command, CommandType? commandType)
        : this(connectionString, command)
    {
        this.CommandType = commandType;
    }

    public BaseSql(string connectionString, string command, List<Parameter> parameters)
        : this(connectionString, command)
    {
        this.Parameters.AddRange(parameters);
    }
    

    private string CacheKey
    {
        get
        {
            if(string.IsNullOrEmpty(this.cacheKey))
            {
                this.cacheKey = this.ComputeKey();
            }
            return this.CacheKey;
        }
    }

    private string ComputeKey()
    {
        var key = this.Command;
        long total = 0;
        foreach (Parameter p in this.Parameters) 
        {
            total += p.Name.GetHashCode();
            total += p.Value?.GetHashCode() ?? 0;
        }

        key += "-" + total.ToString();

        return key;
    }

    public virtual void AddParameter(Parameter parameter)
    {
        parameter.Size = parameter.Size ?? -1;
        parameter.Size = parameter.Size == 0 ? -1 : parameter.Size;

        this.Parameters.Add(parameter);
    }

    public virtual void AddParameter(string name, object value, DbType? type, ParameterDirection parameterDirection, int? size, string typeName)
    {
        size = size ?? -1;
        size = size == 0 ? -1 : size;

        this.Parameters.Add(new Parameter
        {
            Name = name,
            ParameterDirection = parameterDirection,
            Value = value,
            Size = size,
            Type = type,
            TypeName = typeName
        });
    }

    public virtual void AddParameter(string name, object value, DbType type, ParameterDirection parameterDirection, int? size)
    {
        size = size ?? -1;
        size = size == 0 ? -1 : size;

        this.Parameters.Add(new Parameter
        {
            Name = name,
            ParameterDirection = parameterDirection,
            Value = value,
            Size = size,
            Type = type
        });
    }

    public virtual void AddParameter(string name, object value, DbType type, ParameterDirection parameterDirection)
    {
        this.AddParameter(name, value, type, parameterDirection, null);
    }

    public virtual void AddParameter(string name, object value, string typeName)
    {
        this.AddParameter(name, value, null, ParameterDirection.Input, null, typeName);
    }

    public void SetConnectionString(string name)
    {
        this.ConnectionString = this.GetConnectionString(name);
    }

    public void SetConnectionString(string name, string password)
    {
        this.ConnectionString = this.GetConnectionString(name, password);
    }

    public void ClearCacheRelatedTo(string cacheName = "")
    {
        if (!string.IsNullOrWhiteSpace(this.StoreProcedureDataToClear) || !string.IsNullOrWhiteSpace(cacheName))
        {
            var name = this.StoreProcedureDataToClear ?? cacheName;
            Barrel.Current.Empty(Barrel.Current.GetKeys().Where(k => name.Contains("-") ? k.ToLower() == name.ToLower() : k.StartsWith(name + "-", StringComparison.CurrentCultureIgnoreCase)).ToArray());
        }
    }

    public void ClearCacheKey()
    { 
        this.ClearCacheRelatedTo(this.CacheKey);
    }

    public Parameter Get(string name)
    {
        return this.Parameters.Where(p => p.Name == name).First();
    }

    public List<Parameter> Get(ParameterDirection direction)
    {
        var parameters = new List<Parameter>();
        var results = this.Parameters.Where(p => p.ParameterDirection == direction);
        if(results != null)
        {
            parameters = results.ToList();
        }
        return parameters;
    }


    public void CacheOutput<T>(T data)
    {
        try
        {
            if(this.CacheResults && data != null)
            {
                if(this.SlideCache  || !Barrel.Current.Exists(this.CacheKey))
                {
                    Barrel.Current.Add(this.CacheKey, data, this.CacheDuration);
                }
            }
        }
        catch
        {
            // File is locked
        }
    }

    protected bool CacheValid()
    {
        try
        {
            Barrel.Current.EmptyExpired();
            return  this.CacheResults && !Barrel.Current.IsExpired(this.CacheKey);
        }
        catch
        {
            return false;
        }
    }

    protected T Cache<T>()
    {
        try
        {
            var o = Barrel.Current.Get<T>(this.CacheKey);
            this.CacheOutput(o);
            return o;
        }
        catch
        {
            return default(T);
        }
    }


    protected string GetConnectionString(string name)
    {
        string connectionString = string.Empty;
        var cs = ConfigurationManager.ConnectionStrings[name];
        if(cs != null)
        {
            connectionString = cs.ConnectionString;
        }
        return connectionString;
    }

    protected string GetConnectionString(string name, string password)
    {
        string connectionString = this.GetConnectionString(name);
        connectionString = string.Format(connectionString, password);

        return connectionString;
    }

    protected void UpdateParameters(ref Parameter parameter, DynamicParameters dynamicParameters)
    {
        parameter.Value = dynamicParameters.Get<object>(parameter.Name);
    }


}
