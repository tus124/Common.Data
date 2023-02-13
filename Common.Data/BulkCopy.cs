using System.Data;
using System.Data.SqlClient;

namespace Common.Data;

public partial class SqlClient : Base.BaseSql, IDisposable
{
    public void BulkCopy(int batchSize, string destinationTableName, DataTable data, bool deleteDestinationTable = false, bool skipDatabaseUpdate = false)
    {
        this.BeginTransaction();
        try
        {
            if(deleteDestinationTable)
            {
                var sql = "DELETE FROM " + deleteDestinationTable;
                this.CommandType = System.Data.CommandType.Text;
                this.Exec(sql);
            }

            using (var sqlCopy = new SqlBulkCopy(this.Connection, SqlBulkCopyOptions.Default, this.transaction))
            {
                sqlCopy.BatchSize = batchSize;
                sqlCopy.DestinationTableName = destinationTableName;
                sqlCopy.WriteToServer(data);
            }

            if (!skipDatabaseUpdate)
            {
                this.Commit();
            }
            else
            {
                this.Rollback();
            }
        }
        catch
        {
            this.Rollback();
            throw;
        }
    }

    public void BulkCopy<T>(int batchSize, string destinationTableName, IEnumerable<T> data, bool deleteDestinationTable = false, bool skipDatabaseUpdate = false)
    {
        this.BulkCopy(batchSize, destinationTableName, data.ToDataTable(), deleteDestinationTable, skipDatabaseUpdate);
    }

    public void BulkCopy<T>(int batchSize, string destinationTableName, IDataReader data, bool deleteDestinationTable = false, bool skipDatabaseUpdate = false)
    {
        var dataTable = new DataTable();
        dataTable.Load(data);
        this.BulkCopy(batchSize, destinationTableName, dataTable, deleteDestinationTable, skipDatabaseUpdate);
    }
}
