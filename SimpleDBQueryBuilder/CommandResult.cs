using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace GHSoftware.SimpleDb
{
    public class CommandResult : IDisposable
    {
        private readonly IDataReader _reader;
        private readonly IDbCommand _dbCommand;

        public CommandResult(IDataReader reader, IDbCommand dbCommand)
        {
            _reader = reader;
            _dbCommand = dbCommand;
        }

        public IDataReader Reader { get { return _reader; } }
        public IDbCommand Command { get { return _dbCommand; } }

        public void Dispose()
        {
            try
            {
                _reader?.Dispose();
                _dbCommand?.Dispose();
            }
            catch (Exception)
            {
            }
        }
    }
}