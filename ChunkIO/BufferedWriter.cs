using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    abstract class OutputBuffer : Stream
    {
        public UserData UserData { get; set; }
        public object UserState { get; set; }

        // Called exactly once from an arbitrary thread but never when the buffer
        // is locked. Can be called synchronously from OutputBuffer.Dispose(). After
        // the call the buffer gets asynchronously written to the underlying
        // ChunkWriter. Afterwards the buffer no longer gets used, so there is no
        // need to unsubscribe from the event.
        public event Action OnClose;
    }

    struct Triggers
    {
        public long? Size { get; set; }
        public TimeSpan? Age { get; set; }

        public void Validate()
        {
            if (Size < 0) throw new Exception($"Invalid Triggers.Size: {Size}");
            if (Age < TimeSpan.Zero) throw new Exception($"Invalid Triggers.Age: {Age}");
        }
    }

    class BufferedWriterOptions
    {
        public bool AllowRemoteFlush { get; set; } = true;
        public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;
        public Triggers CloseBuffer { get; set; } = new Triggers() { Size = 64 << 10 };
        public Triggers FlushToOS { get; set; }
        public Triggers FlushToDisk { get; set; }

        public BufferedWriterOptions Clone() => (BufferedWriterOptions)MemberwiseClone();

        public void Validate()
        {
            if (!Enum.IsDefined(typeof(CompressionLevel), CompressionLevel))
            {
                throw new Exception($"Invalid CompressionLevel: {CompressionLevel}");
            }
            CloseBuffer.Validate();
            FlushToOS.Validate();
            FlushToDisk.Validate();
        }
    }

    class BufferedWriter : IDisposable
    {
        public BufferedWriter(string fname) : this(fname, new BufferedWriterOptions()) { }

        public BufferedWriter(string fname, BufferedWriterOptions opt)
        {
            throw new NotImplementedException();
        }

        public string Name => throw new NotImplementedException();

        // Requires: There is no current buffer.
        //
        // Returns the newly created locked buffer. Never null.
        public OutputBuffer NewBuffer()
        {
            throw new NotImplementedException();
        }

        // Returns the locked current buffer or null if there is no current buffer.
        public OutputBuffer GetBuffer()
        {
            throw new NotImplementedException();
        }

        // If there is current buffer, waits until it gets unlocked and writes its content to
        // the underlying ChunkWriter. Otherwise does nothing.
        public Task CloseBuffer()
        {
            throw new NotImplementedException();
        }

        // 1. If there is current buffer, waits until it gets unlocked and closes it.
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
