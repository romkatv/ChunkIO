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
  interface IOutputBuffer : IDisposable {
    // Append-only. Neither readable nor seakable. Flush() and FlushAsync() have no effect.
    //
    // The user MUST NOT Dispose() or Close() the stream.
    Stream Stream { get; }

    DateTime CreatedAt { get; }

    UserData UserData { get; set; }
    object UserState { get; set; }

    // How many bytes have been written to the buffer via its Stream methods.
    // The meaning of BytesWritten is the same as Length, except that Length isn't available in
    // OutputBuffer because it's not seakable.
    long BytesWritten { get; }

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
    long? CloseAtSize { get; set; }
    TimeSpan? CloseAtAge { get; set; }

    void Abandon();

    // Called exactly once from an arbitrary thread but never when the buffer
    // is locked. Can be called synchronously from OutputBuffer.Dispose(). After
    // the call the buffer gets asynchronously written to the underlying
    // ChunkWriter. Afterwards the buffer no longer gets used, so there is no
    // need to unsubscribe from the event.
    event Action OnClose;
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
    ISlot _slot = null;

    public BufferedWriter(string fname) : this(fname, new BufferedWriterOptions()) { }

    public BufferedWriter(string fname, BufferedWriterOptions opt) {
      if (opt == null) throw new ArgumentNullException(nameof(opt));
      opt.Validate();
      _opt = opt.Clone();
      _writer = new ChunkWriter(fname);
    }

    public string Name => _writer.Name;

    // Returns the newly created locked buffer. Never null.
    //
    // Requires: There is no current buffer.
    public IOutputBuffer NewBuffer() {
      var buf = new Buffer(this);
      ISlot prev = Interlocked.Exchange(ref _slot, buf);
      if (prev is null || prev is Waiter) return buf;
      // If this happens, no recovery is possible. The process has to be killed.
      System.Diagnostics.Debugger.Launch();
      throw new InvalidOperationException("NewBuffer() precondition violation. Now BufferedWriter is broken.");
    }

    // Returns the locked current buffer or null if there is no current buffer.
    public IOutputBuffer GetBuffer() {
      ISlot slot = Interlocked.Exchange(ref _slot, Locked.Instance);
      if (slot is Buffer buf) return buf;
      Unlock();
      return null;
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

    void Unlock() {

    }

    interface ISlot { }

    sealed class Locked : ISlot {
      public static Locked Instance { get; } = new Locked();
    }

    sealed class Waiter : ISlot {
      public Waiter(Action action) { Action = action; }
      public Action Action { get; }
    }

    sealed class Buffer : IOutputBuffer, ISlot {
      readonly BufferedWriter _writer;
      readonly Compressor _compressor;

      public Buffer(BufferedWriter writer) {
        _writer = writer;
        _compressor = new Compressor(writer._opt.CompressionLevel);
        CloseAtSize = writer._opt.CloseBuffer?.Size;
        CloseAtAge = writer._opt.CloseBuffer?.Age;
      }

      public Stream Stream => _compressor;
      public DateTime CreatedAt { get; } = DateTime.UtcNow;
      public UserData UserData { get; set; }
      public object UserState { get; set; }
      public long BytesWritten => _compressor.BytesWritten;

      public long? CloseAtSize { get; set; }
      public TimeSpan? CloseAtAge { get; set; }

      public event Action OnClose;

      public void Abandon() {
        throw new NotImplementedException();
      }

      public void Dispose() {
        throw new NotImplementedException();
      }
    }

    sealed class Compressor : DeflateStream {
      public Compressor(CompressionLevel lvl) : base(new MemoryStream(16 << 10), lvl) { }

      public long BytesWritten { get; internal set; }

      public ArraySegment<byte> GetCompressed() {
        base.Flush();
        var strm = (MemoryStream)BaseStream;
        return new ArraySegment<byte>(strm.GetBuffer(), 0, (int)strm.Length);
      }

      public override void Flush() { }
      public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

      public override void Write(byte[] buffer, int offset, int count) {
        base.Write(buffer, offset, count);
        BytesWritten += count;
      }

      public override void WriteByte(byte value) {
        base.WriteByte(value);
        ++BytesWritten;
      }

      public override async Task WriteAsync(byte[] buffer, int offset, int count,
                                            CancellationToken cancellationToken) {
        await base.WriteAsync(buffer, offset, count, cancellationToken);
        BytesWritten += count;
      }

      public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count,
                                              AsyncCallback callback, object state) {
        return new AsyncResult(base.BeginWrite(buffer, offset, count, callback, state), count);
      }

      public override void EndWrite(IAsyncResult asyncResult) {
        base.EndWrite(asyncResult);
        BytesWritten += ((AsyncResult)asyncResult).Count;
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
