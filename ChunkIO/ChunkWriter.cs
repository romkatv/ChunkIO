using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    class ChunkWriter : IDisposable
    {
        public ChunkWriter(string fname)
        {
            throw new NotImplementedException();
        }

        public string Name => throw new NotImplementedException();

        public Task Write(UserData userData, byte[] array, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public Task Flush(bool flushToDisk)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
