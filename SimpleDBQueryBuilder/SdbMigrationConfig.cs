using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace GHSoftware.SimpleDb
{
    public class SdbMigrationConfig
    {
        public List<string> Migrations { get; set; } = null;
        public ISdbMigrationCommands MigrationCommands { get; set; }
    }

    public interface ISdbMigrationCommands
    {
        int GetSchemaVersion(DbConnection dbConnection);
        void SetSchemaVersion(DbConnection dbConnection, int newVersion);
    }

}
