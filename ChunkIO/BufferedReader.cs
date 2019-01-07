using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    class BufferedReader : IDisposable
    {
        public BufferedReader(string fname)
        {
            throw new NotImplementedException();
        }

        public string Name => throw new NotImplementedException();

        // If false chunks cannot follow true chunks, seeks to the last false chunk if there are any or
        // to the very first chunk otherwise.
        //
        // TODO: Document what it does if there is no ordering guarantee.
        public Task<ChunkStream> ReadAtPartition(Func<ChunkUserData, bool> pred)
        {
            throw new NotImplementedException();
        }

        public Task<ChunkStream> ReadNext()
        {
            throw new NotImplementedException();
        }

        public Task FlushRemoteWriter()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
