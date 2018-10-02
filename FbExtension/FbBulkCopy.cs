using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace FirebirdSql.Data.FirebirdClient.Extensions
{
    /// <summary>
    /// Lets you efficiently bulk load a Firebird table with data from another source.
    /// It uses table-less select * from rdb$database with union all
    /// 
    /// Author: Herry Hamidjaja
    /// Date: 12/08/2018 
    /// </summary>
    public class FbBulkCopy : IDisposable
    {
        #region "Field(s)"
        private bool _defaultCloseAndDisploseDataReader = true;
        private int _defaultBatchSize = 1000;
        private int _defaultCopyTimeout = 60; // seconds
        private FbConnection _cn;
        private string _tableNm;
        private Dictionary<string, FbColumnTypesExtended> _columns = null;
        public Dictionary<string, FbColumnTypesExtended> ColumnMappings
        {
            get
            {
                return _columns;
            }
        }

        #endregion

        #region "Properties"
        /// <summary>
        /// Name of the destination table in the database.
        /// </summary>
        public string DestinationTableName
        {
            get
            {
                return _tableNm;
            }
            set
            {
                _tableNm = value;
                if (!string.IsNullOrEmpty(value))
                    SetColumnMetaData();
            }
        }
        /// <summary>
        /// Allows control of the incoming DataReader by closing and disposing of it by default after all bulk copy 
        /// operations have completed if set to TRUE, if set to FALSE you need to do your own cleanup (this is useful 
        /// when your DataReader returns more than one result set).  By default this is set to TRUE.
        /// </summary>
        public bool CloseAndDisploseDataReader { get; set; }
        /// <summary>
        /// Batch size data is chunked into when inserting.  The default size is 1,000 records per batch.
        /// </summary>
        public int BatchSize { get; set; }
        /// <summary>
        /// The time in seconds to wait for a batch to load. The default is 30 seconds.
        /// </summary>
        public int BulkCopyTimeout { get; set; }

        #endregion

        #region "Constructors"
        /// <summary>
        /// Initializes a new instance of the SqliteBulkCopy class using the specified open instance of SqliteConnection.
        /// </summary>
        /// <param name="connection"></param>
        public FbBulkCopy(FbConnection connection)
        {
            CloseAndDisploseDataReader = _defaultCloseAndDisploseDataReader;
            this.BatchSize = _defaultBatchSize;
            this.BulkCopyTimeout = _defaultCopyTimeout;
            _cn = connection;
            if (_cn.State == ConnectionState.Closed)
                _cn.Open();
        }

        /// <summary>
        /// Initializes and opens a new instance of SqliteBulkCopy based on the supplied connectionString. 
        /// The constructor uses the SqliteConnection to initialize a new instance of the SqliteBulkCopy class.
        /// </summary>
        /// <param name="connectionString"></param>
        public FbBulkCopy(string connectionString)
        {
            this.CloseAndDisploseDataReader = _defaultCloseAndDisploseDataReader;
            this.BatchSize = _defaultBatchSize;
            this.BulkCopyTimeout = _defaultCopyTimeout;
            _cn = new FbConnection(connectionString);
            if (_cn.State == ConnectionState.Closed)
                _cn.Open();
        }
        #endregion

        #region "Method(s) and Enum(s)"
        /// <summary>
        /// Defines the mapping between a column in a SqliteBulkCopy instance's data source and a column in the instance's destination table.
        /// </summary>
        public enum FbColumnTypesExtended
        {
            //
            // Summary:
            //     A signed integer.
            Integer = 1,
            //
            // Summary:
            //     A floating point value.
            Real = 2,
            //
            // Summary:
            //     A text string.
            Text = 3,
            //
            // Summary:
            //     A blob of data.
            Blob = 4,
            // Summary:
            //     A date column
            Date = 5
        }

        /// <summary>
        /// Get column metadata for a table in a Sqlite database
        /// </summary>
        private void SetColumnMetaData()
        {
            if (!string.IsNullOrEmpty(this.DestinationTableName))
            {
                _columns = new Dictionary<string, FbColumnTypesExtended>();
                /*
                    7 = SMALLINT
                    8 = INTEGER
                    10 = FLOAT
                    12 = DATE
                    13 = TIME
                    14 = CHAR
                    16 = BIGINT
                    27 = DOUBLE PRECISION
                    35 = TIMESTAMP
                    37 = VARCHAR
                    261 = BLOB
                 */
                var sql = string.Format(@"SELECT R.RDB$FIELD_NAME AS name,
                            CASE F.RDB$FIELD_TYPE
                             WHEN 7 THEN 'SMALLINT'
                             WHEN 8 THEN 'INTEGER'
                             WHEN 10 THEN 'FLOAT'
                             WHEN 12 THEN 'DATE'
                             WHEN 13 THEN 'TIME'     
                             WHEN 14 THEN 'CHAR'
                             WHEN 16 THEN 'INT64'
                             WHEN 27 THEN 'DOUBLE'
                             WHEN 35 THEN 'TIMESTAMP'
                             WHEN 37 THEN 'VARCHAR'
                             WHEN 261 THEN 'BLOB'
                             ELSE 'UNKNOWN'
                            END AS type,
                            F.RDB$FIELD_LENGTH AS field_length,
                            CSET.RDB$CHARACTER_SET_NAME AS field_charset
                            FROM RDB$RELATION_FIELDS R
                            LEFT JOIN RDB$FIELDS F ON R.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME
                            LEFT JOIN RDB$CHARACTER_SETS CSET ON F.RDB$CHARACTER_SET_ID = CSET.RDB$CHARACTER_SET_ID
                            WHERE R.RDB$RELATION_NAME= '{0}'
                            ORDER BY R.RDB$FIELD_POSITION", this.DestinationTableName);
                using (var cmd = new FbCommand(sql, _cn) { CommandType = CommandType.Text })
                {
                    using (var dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            var key = dr["name"].ToString().Trim();
                            var typ = new FbColumnTypesExtended();
                            var columnType = dr["type"].ToString().Trim().ToUpper();
                            switch (columnType)
                            {
                                case "INTEGER":
                                case "SMALLINT":
                                case "BIGINT":
                                    typ = FbColumnTypesExtended.Integer;
                                    break;
                                case "DOUBLE":
                                case "FLOAT":
                                    typ = FbColumnTypesExtended.Real;
                                    break;
                                case "CLOB":
                                    typ = FbColumnTypesExtended.Text;
                                    break;
                                case "BLOB":
                                    typ = FbColumnTypesExtended.Blob;
                                    break;
                                case "DATE":
                                case "TIMESTAMP":
                                    typ = FbColumnTypesExtended.Date;
                                    break;
                                default: // look for fringe cases that need logic
                                    if (columnType.StartsWith("CHAR"))
                                        typ = FbColumnTypesExtended.Text;
                                    if (columnType.StartsWith("VARCHAR"))
                                        typ = FbColumnTypesExtended.Text;
                                    if (columnType.StartsWith("NCHAR"))
                                        typ = FbColumnTypesExtended.Text;
                                    break;
                            }
                            _columns.Add(key, typ);
                        }
                    }
                }
                if (_columns == null || _columns.Count < 1)
                    throw new Exception($"{this.DestinationTableName} could not be found in the database");
            }
        }

        /// <summary>
        /// Close and database connections.
        /// </summary>
        public void Close()
        {
            if (_cn.State == ConnectionState.Open)
                _cn.Close();
        }

        /// <summary>
        /// Copies all rows in the supplied IDataReader to a destination table specified 
        /// by the DestinationTableName property of the SqliteBulkCopy object.
        /// </summary>
        /// <param name="reader"></param>
        public void WriteToServer(IDataReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException("reader");

            // build the insert schema and use table-less union all
            var insertClause = new StringBuilder();
            insertClause.Append($"INSERT INTO {this.DestinationTableName} (");
            var first = true;
            foreach (var c in this.ColumnMappings)
            {
                if (first)
                {
                    insertClause.Append(c.Key);
                    first = false;
                }
                else
                    insertClause.Append("," + c.Key);
            }
            insertClause.Append(")");

            first = true;
            var valuesClause = new StringBuilder();
            var currentBatch = 0;
            while (reader.Read())
            {
                // generate insert values block statement
                valuesClause.Append("SELECT ");
                var colFirst = true;
                foreach (var c in this.ColumnMappings)
                {
                    if (!colFirst)
                        valuesClause.Append(",");
                    else
                        colFirst = false;
                    var columnValue = reader[c.Key] == null ? null as string : reader[c.Key].ToSQLString();
                    if (string.IsNullOrEmpty(columnValue))
                        valuesClause.Append("NULL");
                    else
                    {
                        switch (c.Value)
                        {
                            case FbColumnTypesExtended.Date:
                                try
                                {
                                    valuesClause.Append($"'{ DateTime.Parse(columnValue).ToString("yyyy-MM-dd HH:mm:ss") }'");
                                }
                                catch (Exception exp)
                                {
                                    throw new Exception($"Invalid Cast when loading date column [{ c.Key }] in table [{ this.DestinationTableName}] in Sqlite DB with data; value being casted '{ columnValue}', incoming values must be of data format consumable by .NET; error:\n {exp.Message}");
                                }
                                break;
                            case FbColumnTypesExtended.Integer:
                            case FbColumnTypesExtended.Real:
                                valuesClause.Append(columnValue);
                                break;
                            case FbColumnTypesExtended.Blob:
                                valuesClause.Append($"X'{columnValue}'");
                                break;
                            case FbColumnTypesExtended.Text:
                            default:
                                valuesClause.Append($"'{columnValue.Replace("'", "''")}'");
                                break;
                        }
                    }
                }
                valuesClause.Append("FROM rdb$database UNION ALL ");

                currentBatch++;
                // limit the query to under 64kb as the max limit firebird query
                if (valuesClause.Length + insertClause.Length > 60000 || currentBatch == this.BatchSize)
                {
                    valuesClause.Length -= " UNION ALL ".Length;
                    var dml = $"EXECUTE BLOCK AS BEGIN {insertClause.ToString()} {valuesClause.ToString()}; END";
                    valuesClause.Clear();
                    using (var cmd = new FbCommand(dml, _cn) { CommandType = CommandType.Text, CommandTimeout = this.BulkCopyTimeout })
                        cmd.ExecuteNonQuery();
                    currentBatch = 0;
                }
            }
            if (this.CloseAndDisploseDataReader)
            {
                reader.Close();
                reader.Dispose();
            }
            // if any records remain after the read loop has completed then write them to the DB
            if (currentBatch > 0)
            {
                valuesClause.Length -= " UNION ALL ".Length;
                var dml = $"EXECUTE BLOCK AS BEGIN {insertClause.ToString()} {valuesClause.ToString()}; END";
                using (var cmd = new FbCommand(dml, _cn) { CommandType = CommandType.Text, CommandTimeout = this.BulkCopyTimeout })
                    cmd.ExecuteNonQuery();
            }

        }

#pragma warning disable 1998
        /// <summary>
        /// The asynchronous version of WriteToServer, which copies all rows in the supplied IDataReader to a 
        /// destination table specified by the DestinationTableName property of the SqliteBulkCopy object.
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        private async Task WriteToServerAsyncInternal(IDataReader reader)
        {
            WriteToServer(reader);
        }
#pragma warning restore 1998

        /// <summary>
        /// Releases all resources used by the current instance of the SqliteBulkCopy class.
        /// </summary>
        public void Dispose()
        {
            this.Close();
            _cn.Dispose();
        }

        #endregion
    }

    static class FbObjectExtension
    {
        /// <summary>
        /// Provide Extension method to convert object to string and handle array of byte for varbinary.
        /// </summary>
        /// <param name="a"></param>
        /// <returns></returns>
        public static string ToSQLString(this object a)
        {
            if (a.GetType().Name == "Byte[]")
            {
                return BitConverter.ToString(a as byte[]).Replace("-", "");
            }
            return a.ToString();
        }


    }
}
