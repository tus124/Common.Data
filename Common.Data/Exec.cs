using Common.Data.Item;
using Dapper;
using System.Data;

namespace Common.Data;

public partial class SqlClient : Base.BaseSql, IDisposable
{
    public int Exec()
    {
        this.CreateConnection();
        int rows;

        try
        {
            var p = new DynamicParameters();
            
            if(this.Parameters.Count > 0)
            {
                foreach (var item in this.Parameters)
                {
                    if(item.Value is DataTable)
                    {
                        DataTable dt = (DataTable)item.Value;
                        if(!string.IsNullOrEmpty(item.TypeName))
                        {
                            dt.SetTypeName(item.TypeName);
                        }
                        p.Add(item.Name, dt.AsTableValuedParameter(null), null, ParameterDirection.Input, null);
                    }
                    else
                    {
                        p.Add(item.Name, item.Value, item.Type, item.ParameterDirection, item.Size);
                    }
                }
            }

            rows = this.Connection.Execute(this.Command, p, this.transaction, this.Timeout, this.CommandType);
            
            this.Parameters.ForEach(item => this.UpdateParameters(ref item, p));
            if(this.ClearCacheOnExec)
            {
                this.ClearCacheRelatedTo();
            }
        }
        catch
        {
            throw;
        }
        finally
        {
            if(!this.isTransaction)
            {
                this.CloseConnection();
            }
        }
        return rows;
    }


    public int Exec(string command)
    {
        this.Command = command;
        return this.Exec();
    }

    public int Exec(string command, int? timeout)
    {
        this.Command= command;
        this.Timeout = timeout;

        return this.Exec();
    }

    public int Exec(string command, System.Data.CommandType? commandType, int? timeout)
    {
        this.Command = command;
        this.CommandType = commandType;
        this.Timeout = timeout;

        return this.Exec();
    }

    public int Exec(string command, List<Parameter> parms)
    {
        this.Command = command;
        this.Parameters.AddRange(parms);

        return this.Exec();
    }

    public int Exec(string command, List<Parameter> parms, int? timeout)
    {
        this.Command = command;
        this.Parameters.AddRange(parms);
        this.Timeout= timeout;
        return this.Exec();
    }

    public int Exec(string command, System.Data.CommandType? commandType, List<Parameter> parms)
    {
        this.Command = command;
        this.CommandType = commandType;
        this.Parameters.AddRange(parms);
 
        return this.Exec();
    }

    public int Exec(string command, System.Data.CommandType? commandType, List<Parameter> parms, int? timeout)
    {
        this.Command = command;
        this.CommandType = commandType;
        this.Parameters.AddRange(parms);
        this.Timeout = timeout;

        return this.Exec();
    }

    public int Exec<T>(T data)
    {
        data?.GetType().GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Where(p => p.GetGetMethod() != null && p.GetSetMethod() != null)
            .AsList()
            .ForEach(p => this.Parameters.Add(item: new Item.Parameter { Name = "@" + p.Name, Value = p.GetValue(data) } ));

        return this.Exec();
    }

    public int Exec<T>(string command, T data, int? timeout)
    {
        this.Command = command;
        this.Timeout = timeout;

        return this.Exec<T>(data);
    }

    public int Exec<T>(string command, System.Data.CommandType? commandType, T data)
    {
        this.Command = command;
        this.CommandType = commandType;

        return this.Exec<T>(data);
    }

    public int Exec<T>(string command, System.Data.CommandType? commandType, T data, int? timeout)
    {
        this.Command = command;
        this.CommandType = commandType;
        this.Timeout = timeout;

        return this.Exec<T>(data);
    }

    public object ExecScalar()
    {
        this.CreateConnection();
        object result = null;

        try
        {
            var p = new DynamicParameters();

            if (this.Parameters.Count > 0)
            {
                foreach (var item in this.Parameters)
                {
                    if (item.Value is DataTable)
                    {
                        DataTable dt = (DataTable)item.Value;
                        if (!string.IsNullOrEmpty(item.TypeName))
                        {
                            dt.SetTypeName(item.TypeName);
                        }
                        p.Add(item.Name, dt.AsTableValuedParameter(null), null, ParameterDirection.Input, null);
                    }
                    else
                    {
                        p.Add(item.Name, item.Value, item.Type, item.ParameterDirection, item.Size);
                    }
                }
            }

            result = this.Connection.ExecuteScalar(this.Command, p, this.transaction, this.Timeout, this.CommandType);
            this.Parameters.ForEach(item => this.UpdateParameters(ref item, p));
            if (this.ClearCacheOnExec)
            {
                this.ClearCacheRelatedTo();
            }
        }
        catch
        {
            throw;
        }
        finally
        {
            if (!this.isTransaction)
            {
                this.CloseConnection();
            }
        }
        return result;
    }

    public object ExecScalar(string command)
    {
        this.Command = command;
        return this.ExecScalar();
    }

    public object ExecScalar(string command, int? timeout)
    {
        this.Command = command;
        this.Timeout = timeout;

        return this.ExecScalar();
    }

    public object ExecScalar(string command, System.Data.CommandType? commandType)
    {
        this.Command = command;
        this.CommandType = commandType;

        return this.ExecScalar();
    }

    public object ExecScalar(string command, System.Data.CommandType? commandType, int? timeout)
    {
        this.Command = command;
        this.CommandType = commandType;
        this.Timeout = timeout;

        return this.ExecScalar();
    }

    public object ExecScalar(string command, List<Parameter> parms)
    {
        this.Command = command;
        this.Parameters.AddRange(parms);

        return this.ExecScalar();
    }

    public object ExecScalar(string command, List<Parameter> parms, int? timeout)
    {
        this.Command = command;
        this.Parameters.AddRange(parms);
        this.Timeout = timeout;

        return this.ExecScalar();
    }

    public object ExecScalar(string command, System.Data.CommandType? commandType, List<Parameter> parms)
    {
        this.Command = command;
        this.CommandType = commandType;
        this.Parameters.AddRange(parms);

        return this.ExecScalar();
    }

    public object ExecScalar(string command, System.Data.CommandType? commandType, List<Parameter> parms, int? timeout)
    {
        this.Command = command;
        this.CommandType = commandType;
        this.Parameters.AddRange(parms);
        this.Timeout = timeout;

        return this.ExecScalar();
    }
}
