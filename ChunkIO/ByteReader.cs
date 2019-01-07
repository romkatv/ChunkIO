using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    class ByteReader : IDisposable
    {
        public string Name => throw new NotImplementedException();
        public long Length => throw new NotImplementedException();
        public void Seek(long position) => throw new NotImplementedException();
        public Task<int> Read(byte[] array, int offset, int count) => throw new NotImplementedException();
        public void Dispose() => throw new NotImplementedException();
    }
}
