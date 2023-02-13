using Common.Data.Item;
using Dapper;
using System.Data;

namespace Common.Data;

public partial class SqlClient : Base.BaseSql, IDisposable
{
    public IEnumerable<T> Query<T>()
    {
        if(this.CacheValid())
        {
            var o = this.Cache<IEnumerable<T>>();
            if(o?.Any() ?? false)
            {
                return o;
            }
        }

        this.CreateConnection();
        var output = (IEnumerable<T>)null;

        try
        {
            var p = new DynamicParameters();

            if(this.Parameters != null && this.Parameters.Count > 0)
            {
                foreach(var param in this.Parameters)
                {
                    if(param.Value is DataTable)
                    {
                        DataTable dt = (DataTable)param.Value;

                        if (!string.IsNullOrEmpty(param.TypeName))
                        {
                            dt.SetTypeName(param.TypeName);
                        }

                        p.Add(param.Name, dt.AsTableValuedParameter(null), null, ParameterDirection.Input, null);
                    }
                    else
                    {
                        p.Add(param.Name, param.Value, param.Type, param.ParameterDirection, param.Size);
                    }
                }

                output = this.Connection.Query<T>(this.Command, p, this.transaction, true, this.Timeout, this.CommandType);
                this.Parameters.ForEach(x => this.UpdateParameters(ref x, p));
                this.CacheOutput(output);
            }
            else
            {
                output = this.Connection.Query<T>(this.Command, p, this.transaction, true, this.Timeout, this.CommandType);
            }
           

            return output;
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
    }

    public T QueryFirst<T>()
    {
        if (this.CacheValid())
        {
            var o = this.Cache<T>();
            if (o != null)
            {
                return o;
            }
        }

        this.CreateConnection();
        T output = default(T);

        try
        {
            var p = new DynamicParameters();

            if (this.Parameters.Count > 0)
            {
                foreach (var param in this.Parameters)
                {
                    if (param.Value is DataTable)
                    {
                        DataTable dt = (DataTable)param.Value;

                        if (!string.IsNullOrEmpty(param.TypeName))
                        {
                            dt.SetTypeName(param.TypeName);
                        }

                        p.Add(param.Name, dt.AsTableValuedParameter(null), null, ParameterDirection.Input, null);
                    }
                    else
                    {
                        p.Add(param.Name, param.Value, param.Type, param.ParameterDirection, param.Size);
                    }
                }
            }

            output = this.Connection.QueryFirstOrDefault<T>(this.Command, p, this.transaction, this.Timeout, this.CommandType);
            this.Parameters.ForEach(x => this.UpdateParameters(ref x, p));
            this.CacheOutput<T>(output);

           
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

        return output;
    }

    public T QueryFirst<T>(string command)
    {
        this.Command = command;
        return this.QueryFirst<T>();
    }

    public T QueryFirst<T>(string command, int? timeout)
    {
        this.Command = command;
        this.Timeout = timeout;

        return this.QueryFirst<T>();
    }

    public T QueryFirst<T>(string command, List<Parameter> param)
    {
        this.Command = command;
        this.Parameters.AddRange(param);

        return this.QueryFirst<T>();
    }

    public T QueryFirst<T>(string command, List<Parameter> param, int? timeout)
    {
        this.Command = command;
        this.Parameters.AddRange(param);
        this.Timeout = timeout;

        return this.QueryFirst<T>();
    }

    public dynamic QueryFirst()
    {
        this.CreateConnection();
        dynamic output = null;

        try
        {
            var p = new DynamicParameters();

            if(this.CacheValid())
            {
                var o = this.Cache<dynamic>();
                if(o != null)
                {
                    return o;
                }
            }

            if (this.Parameters.Count > 0)
            {
                foreach (var param in this.Parameters)
                {
                    p.Add(param.Name, param.Value, param.Type, param.ParameterDirection, param.Size);
                }
            }

            output = this.Connection.QueryFirstOrDefault(this.Command, p, this.transaction, this.Timeout, this.CommandType);
            this.Parameters.ForEach(x => this.UpdateParameters(ref x, p));
            this.CacheOutput<dynamic>(output);
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
        return output;
    }





    public dynamic QueryFirst(string command)
    {
        this.Command = command;
        return this.QueryFirst();
    }

    public dynamic QueryFirst(string command, int? timeout)
    {
        this.Command = command;
        this.Timeout = timeout;

        return this.QueryFirst();
    }

    public dynamic QueryFirst(string command, List<Parameter> param)
    {
        this.Command = command;
        this.Parameters.AddRange(param);

        return this.QueryFirst();
    }

    public dynamic QueryFirst(string command, List<Parameter> param, int? timeout)
    {
        this.Command = command;
        this.Parameters.AddRange(param);
        this.Timeout = timeout;

        return this.QueryFirst();
    }


    public IEnumerable<dynamic> Query()
    {
        if (this.CacheValid())
        {
            var o = this.Cache<IEnumerable<dynamic>>();
            if (o?.Any() ?? false)
            {
                return o;
            }
        }

        this.CreateConnection();
        var output = (IEnumerable<dynamic>)null;


        try
        {
            var p = new DynamicParameters();

            if (this.Parameters.Count > 0)
            {
                foreach (var param in this.Parameters)
                {
                    p.Add(param.Name, param.Value, param.Type, param.ParameterDirection, param.Size);
                }
            }

            output = this.Connection.Query(this.Command, p, this.transaction, true, this.Timeout, this.CommandType);
            this.Parameters.ForEach(x => this.UpdateParameters(ref x, p));
            this.CacheOutput<IEnumerable<dynamic>>(output);
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
        return output;
    }



    public IEnumerable<T> Query<T>(string command)
    {
        this.Command = command;
        return this.Query<T>();
    }

    public IEnumerable<T> Query<T>(string command, int? timeout)
    {
        this.Command = command;
        this.Timeout = timeout;

        return this.Query<T>();
    }

    public IEnumerable<T> Query<T>(string command, List<Parameter> param)
    {
        this.Command = command;
        this.Parameters.AddRange(param);

        return this.Query<T>();
    }

    public IEnumerable<T> Query<T>(string command, List<Parameter> param, int? timeout)
    {
        this.Command = command;
        this.Parameters.AddRange(param);
        this.Timeout = timeout;

        return this.Query<T>();
    }





    public IEnumerable<dynamic> Query(string command)
    {
        this.Command = command;
        return this.Query();
    }

    public IEnumerable<dynamic> Query(string command, int? timeout)
    {
        this.Command = command;
        this.Timeout = timeout;

        return this.Query();
    }

    public IEnumerable<dynamic> Query(string command, List<Parameter> param)
    {
        this.Command = command;
        this.Parameters.AddRange(param);

        return this.Query();
    }

    public IEnumerable<dynamic> Query(string command, List<Parameter> param, int? timeout)
    {
        this.Command = command;
        this.Parameters.AddRange(param);
        this.Timeout = timeout;

        return this.Query();
    }
}
