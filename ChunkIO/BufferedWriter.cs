using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    struct Triggers
    {
        public long? Size { get; set; }
        public TimeSpan? Age { get; set; }
    }

    class BufferedWriterOptions
    {
        public bool AllowRemoteFlush { get; set; } = true;
        public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;
        public Triggers CloseChunk { get; set; } = new Triggers() { Size = 64 << 10 };
        public Triggers FlushToOS { get; set; }
        public Triggers FlushToDisk { get; set; }
    }

    class BufferedWriter : IDisposable
    {
        public BufferedWriter(string fname) : this(fname, new BufferedWriterOptions()) { }

        public BufferedWriter(string fname, BufferedWriterOptions opt)
        {
            throw new NotImplementedException();
        }

        public string Name => throw new NotImplementedException();

        // Requires: There is no current chunk.
        public ChunkStream NewChunk(ChunkUserData userData)
        {
            throw new NotImplementedException();
        }

        // Returns null if there is no current chunk.
        public ChunkStream GetChunk()
        {
            throw new NotImplementedException();
        }

        // If there is current chunk, waits until it gets unlocked and writes its content to
        // the underlying ChunkWriter. Otherwise does nothing.
        public Task CloseChunk()
        {
            throw new NotImplementedException();
        }

        // 1. If there is current chunk, waits until it gets unlocked and closes it.
        // 2. Flushes the underlying ChunkWriter.
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
