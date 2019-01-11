using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    struct ChunkHeader
    {
        public const int Size = UserData.Size + 3 * Encoding.UInt64.Size;

        public UserData UserData { get; set; }
        public ulong PayloadLength { get; set; }
        public ulong PayloadHash { get; set; }
        public ulong HeaderHash { get; set; }
    }

    struct Meter
    {
        public const int Size = 3 * Encoding.UInt64.Size;

        public ulong ChunkBeginPosition { get; set; }
        public ulong ChunkEndPosition { get; set; }
        public ulong MeterHash { get; set; }
    }

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
