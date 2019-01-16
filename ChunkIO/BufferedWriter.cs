using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  // Append-only. Neither readable nor seakable.
  //
  // It's illegal to call any method of OutputBuffer when it isn't locked. Locked instances are returned
  // by BufferedWriter.NewBuffer() and BufferedWriter.GetBuffer(). They can be unlocked with
  // OutputBuffer.Dispose().
  //
  // Writes to the buffer never block on IO. All data is stored in memory until the buffer is closed.
  // Flush() doesn't do anything. Dispose() and Close() unlock the buffer (they are equivalent).
  abstract class OutputBuffer : Stream {
    public DateTime CreatedAt { get; } = DateTime.UtcNow;

    public UserData UserData { get; set; }
    public object UserState { get; set; }

    // How many bytes have been written to the buffer via its Stream methods.
    // The meaning of BytesWritten is the same as Length, except that Length isn't available in
    // OutputBuffer because it's not seakable.
    public abstract long BytesWritten { get; }

    // When a buffer is created, CloseAtSize and CloseAtAge are set to
    // BufferedWriterOptions.CloseBuffer.Size and BufferedWriterOptions.Age respectively.
    //
    // A buffer is automatically closed when all of the following conditions are true:
    //
    // 1. The buffer is not locked.
    // 2. BytesWritten >= CloseAtSize || DateTime.UtcNow - CreatedAt >= CloseAtAge.
    //
    // An implication of this is that the buffer won't get closed if the second condition
    // becomes true while the buffer is locked as long as it reverts to false before unlocking.
    public long? CloseAtSize { get; set; }
    public TimeSpan? CloseAtAge { get; set; }

    public abstract void Abandon();

    // Called exactly once from an arbitrary thread but never when the buffer
    // is locked. Can be called synchronously from OutputBuffer.Dispose(). After
    // the call the buffer gets asynchronously written to the underlying
    // ChunkWriter. Afterwards the buffer no longer gets used, so there is no
    // need to unsubscribe from the event.
    public event Action OnClose;
  }

  class Triggers {
    public long? Size { get; set; }
    public TimeSpan? Age { get; set; }

    public Triggers Clone() => (Triggers)MemberwiseClone();

    public void Validate() {
      if (Size < 0) throw new Exception($"Invalid Triggers.Size: {Size}");
      if (Age < TimeSpan.Zero) throw new Exception($"Invalid Triggers.Age: {Age}");
    }
  }

  class BufferedWriterOptions {
    public bool AllowRemoteFlush { get; set; } = true;
    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;
    public Triggers CloseBuffer { get; set; } = new Triggers();
    public Triggers FlushToOS { get; set; } = new Triggers();
    public Triggers FlushToDisk { get; set; } = new Triggers();

    public BufferedWriterOptions Clone() {
      var res = (BufferedWriterOptions)MemberwiseClone();
      res.CloseBuffer = res.CloseBuffer?.Clone();
      res.FlushToOS = res.FlushToOS?.Clone();
      res.FlushToDisk = res.FlushToDisk?.Clone();
      return res;
    }

    public void Validate() {
      if (!Enum.IsDefined(typeof(CompressionLevel), CompressionLevel)) {
        throw new Exception($"Invalid CompressionLevel: {CompressionLevel}");
      }
      CloseBuffer?.Validate();
      FlushToOS?.Validate();
      FlushToDisk?.Validate();
    }
  }

  sealed class BufferedWriter : IDisposable {
    readonly BufferedWriterOptions _opt;
    readonly ChunkWriter _writer;

    public BufferedWriter(string fname) : this(fname, new BufferedWriterOptions()) { }

    public BufferedWriter(string fname, BufferedWriterOptions opt) {
      if (opt == null) throw new ArgumentNullException(nameof(opt));
      opt.Validate();
      _opt = opt.Clone();
      _writer = new ChunkWriter(fname);
    }

    public string Name => throw new NotImplementedException();

    // Requires: There is no current buffer.
    //
    // Returns the newly created locked buffer. Never null.
    public OutputBuffer NewBuffer() {
      throw new NotImplementedException();
    }

    // Returns the locked current buffer or null if there is no current buffer.
    public OutputBuffer GetBuffer() {
      throw new NotImplementedException();
    }

    // If there is current buffer, waits until it gets unlocked and writes its content to
    // the underlying ChunkWriter. Otherwise does nothing.
    public Task CloseBufferAsync() {
      throw new NotImplementedException();
    }

    // 1. If there is current buffer, waits until it gets unlocked and closes it.
    // 2. Flushes the underlying ChunkWriter.
    public Task FlushAsync(bool flushToDisk) {
      throw new NotImplementedException();
    }

    public void Dispose() {
      throw new NotImplementedException();
    }

    class Buffer : OutputBuffer {
      readonly BufferedWriter _writer;
      readonly MemoryStream _output;
      readonly DeflateStream _deflate;
      long _written = 0;

      public Buffer(BufferedWriter writer) {
        _writer = writer;
        _output = new MemoryStream(16 << 10);
        _deflate = new DeflateStream(_output, writer._opt.CompressionLevel, leaveOpen: true);
      }

      public override bool CanWrite => true;
      public override bool CanRead => false;
      public override bool CanSeek => false;
      public override bool CanTimeout => false;
      public override long BytesWritten => _written;

      // TODO: This may not work. Stream might block multiple Dispose() calls.
      protected override void Dispose(bool disposing) => throw new NotImplementedException();

      public override void Abandon() => throw new NotImplementedException();

      public override void Close() => throw new NotImplementedException();

      public override void Write(byte[] buffer, int offset, int count) {
        _deflate.Write(buffer, offset, count);
        _written += count;
      }

      public override void WriteByte(byte value) {
        _deflate.WriteByte(value);
        ++_written;
      }

      public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) {
        await _deflate.WriteAsync(buffer, offset, count, cancellationToken);
        _written += count;
      }

      public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count,
                                              AsyncCallback callback, object state) {
        return new AsyncResult(_deflate.BeginWrite(buffer, offset, count, callback, state), count);
      }

      public override void EndWrite(IAsyncResult asyncResult) {
        _deflate.EndWrite(asyncResult);
        _written += ((AsyncResult)asyncResult).Count;
      }

      public override void Flush() { }
      public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

      public override long Length => throw new NotSupportedException();
      public override int ReadTimeout => throw new NotSupportedException();
      public override int WriteTimeout => throw new NotSupportedException();
      public override long Position {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
      }

      public override void SetLength(long value) => throw new NotSupportedException();
      public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
      public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
      public override int EndRead(IAsyncResult asyncResult) => throw new NotSupportedException();
      public override int ReadByte() => throw new NotSupportedException();
      
      public override IAsyncResult BeginRead(byte[] buffer, int offset, int count,
                                             AsyncCallback callback, object state) {
         throw new NotSupportedException();
      }
      public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken) {
         throw new NotSupportedException();
      }
      public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) {
         throw new NotSupportedException();
      }

      class AsyncResult : IAsyncResult {
        readonly IAsyncResult _r;

        public AsyncResult(IAsyncResult r, int count) {
          _r = r;
          Count = count;
        }

        public int Count { get; }

        public bool IsCompleted => _r.IsCompleted;
        public WaitHandle AsyncWaitHandle => _r.AsyncWaitHandle;
        public object AsyncState => _r.AsyncState;
        public bool CompletedSynchronously => _r.CompletedSynchronously;
      }
    }
  }
}
