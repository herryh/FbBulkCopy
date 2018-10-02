using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.FirebirdClient.Extensions;
using System.IO;
using System.Text;

namespace FbExample
{
    class Program
    {
        static void Main(string[] args)
        {
            var dbPath = "bulktest.fdb";
           
            var connectionString = new FbConnectionStringBuilder
            {
                Database = dbPath,
                ServerType = FbServerType.Embedded,
                UserID = "SYSDBA",
                Password = "masterkey",
                ClientLibrary = "fbclient.dll",
                Dialect = 3
            }.ToString();

            if (!File.Exists(dbPath))
            {
                InitSourceDatabase(connectionString);

            }

            using (var conn = new FbConnection(connectionString))
            {
                conn.Open();
                var cmd = conn.CreateCommand();

                // Create destination table
                cmd.CommandText = "CREATE TABLE D(A int, B varchar(20), C BLOB sub_type 0);";
                cmd.ExecuteNonQuery();

                // Query source table
                cmd.CommandText = "SELECT A,B,C FROM S";
                var reader = cmd.ExecuteReader();

                var bulkcopy = new FbBulkCopy(conn)
                {
                    DestinationTableName = "D"
                };
                bulkcopy.WriteToServer(reader);
            }
        }

        private static void InitSourceDatabase(string connectionString)
        {
            FbConnection.CreateDatabase(connectionString);
            using (var conn = new FbConnection(connectionString))
            {
                conn.Open();
                // create table source
                var cmd = conn.CreateCommand();
                cmd.CommandText = "CREATE TABLE S(A int, B varchar(20), C BLOB sub_type 0);";
                cmd.ExecuteNonQuery();

                var sb = new StringBuilder();
                sb.AppendLine("execute block\r\n as\r\n begin");
                for (var i = 0; i < 5; i++)
                {
                    sb.Append($"insert into S(A,B,C) values('{i}','{i}',x'504B03041400000008009484C74CBA2A2857CB010000F70300002E00240055736572732F68656E6C6579732F417070446174612F4C6F63616C2F54656D702F322F746D70413030422E746D700A0020000000000001001800678B4DE529FED301678B4DE529FED301678B4DE529FED3017D53C18ADB3010FD9581B2D01ED6D8EE66374DDA82490F5B2865A13DEA22CB6347204B6624278425FFDE91ED24DE64A90FD6E8CD9BA79927F42A28D49990D6EB57513B1B4269384845EDF7DA7B28484BB33E329489BA751592858DEB4923C16FDCAF8F9C52CE38E23A4158A5A221449B8AD2F498AE6FA07CB19883BCBD659EC01337EE6F54AFC01337CB97231C830B3BFB924F300711E6F57D3EAFB75D5F816FB5D3F705CECAF3AEAFC013F73CCC5B3F6EE038E571BC9D6C293A49C0F7024300CF25149982FF7C42D50FF0983CC13347E988DDCDF21F011E9214EE99C3D94FF0613CE1FC831783D223581710086B24B48A23691BF420BBCE1C203828AADE043F566C5CDBA20D1E9C855FB2849FD50AB2CF8BFC7199E5D7EA458BA495B4F043CB12034B16DE3BA565D05C4DA806AD8A4F8220A9C100AE86AF4F497AC7B3C7D1A5AD26295DC3F76FCB4B86F01E77D2F4A31497852D92EC0EC9948F393EAF958738DC242295EA4906E4A97858832AC43268915B6C4CAF1C5B61708706B4858EA58741F73A6CC1BB16E3858C423BC98F683081C06FF9A9A0C58A352B50680CF89E769A3B48665E6C8AD9E62F7A2E662B3BC3B6B08343D3F3DC0BFBA26D03B09A4AFEC8483EE3AB8BCF4370FC07504B01022D001400000008009484C74CBA2A2857CB010000F70300002E002400000000000000200000000000000055736572732F68656E6C6579732F417070446174612F4C6F63616C2F54656D702F322F746D70413030422E746D700A0020000000000001001800678B4DE529FED301678B4DE529FED301678B4DE529FED301504B05060000000001000100800000003B0200000000');");
                    
                }
                sb.Append("\r\n");
                sb.Append("end");
                cmd.CommandText = sb.ToString();
                cmd.ExecuteNonQuery();
            }
        }
    }
}
