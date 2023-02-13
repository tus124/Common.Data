using Common.Data.Item;
using System.Data;
using System.Data.SqlClient;

namespace Common.Data;

public partial class SqlClient : Base.BaseSql, IDisposable
{
    private SqlTransaction transaction;
    private bool disposedValue = false;
    private bool isTransaction = false;

    public SqlClient()
    {
    }

    public SqlClient(string connection) : base(connection)
    {
    }

    public SqlClient(string connection, string command, List<Parameter> parms) : base(connection, command, parms)
    {
    }

    public SqlConnection Connection { get; set; }

    public void Dispose()
    {
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    public void BeginTransaction()
    {
        this.CreateConnection();
        this.transaction = this.Connection.BeginTransaction();
    }

    protected void CreateConnection()
    {
        this.isTransaction = this.Connection?.State == ConnectionState.Open;
        if(!this.isTransaction)
        {
            this.Connection = new SqlConnection(this.ConnectionString);
        }

        if(this.Connection?.State == System.Data.ConnectionState.Closed)
        {
            this.Connection.Open();
        }
    }

    protected void CloseConnection()
    {
        if(this.Connection?.State == System.Data.ConnectionState.Open)
        {
            this.Connection?.Close();
        }
    }
    protected virtual void Dispose(bool disposing)
    {
        if(!this.disposedValue)
        {
            if(disposing)
            {
                this.Connection?.Dispose();
                this.transaction?.Dispose();
            }
            this.disposedValue = true;
        }
    }


    public void Commit()
    {
        this.transaction.Commit();
        this.CloseConnection();
    }

    public void Rollback()
    {
        this.transaction.Rollback();
        this.CloseConnection();
    }
}
