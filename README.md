# FbBulkCopy
Implementation of FbBulkCopy similar to SqlBulkCopy

# Usage
        using (var conn = new FbConnection(connectionString))
        {
            conn.Open();
            var cmd = conn.CreateCommand();

            // Create destination table
            ...

            // Query source table
            cmd.CommandText = "SELECT * FROM SourceTable";
            var reader = cmd.ExecuteReader();

            var bulkcopy = new FbBulkCopy(conn)
            {
                DestinationTableName = "DestinationTable"
            };
            bulkcopy.WriteToServer(reader);
        }
