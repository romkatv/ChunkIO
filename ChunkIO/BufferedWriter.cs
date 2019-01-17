using System;
using System.Collections.Generic;
using System.Diagnostics;
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
    // NOTE: It's fairly easy to make CloseAtAge settable the same way CloseAtSize is settable.
    TimeSpan? CloseAtAge { get; }

    void Abandon();
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

  // Thread-compatible but not thread-safe.
  sealed class Timer : IDisposable {
    readonly Action<CancellationToken> _action;
    CancellationTokenSource _cancel = null;
    Task _task = null;

    public Timer(Action<CancellationToken> action) {
      Debug.Assert(action != null);
      _action = action;
    }

    public DateTime? Time { get; internal set; }

    public void Stop() {
      Debug.Assert((_task == null) == (_cancel == null) == (Time == null));
      if (_task == null) return;

      CancellationTokenSource cancel = _cancel;
      cancel.Cancel();
      _task.ContinueWith(_ => cancel.Dispose());
      _task = null;
      _cancel = null;
      Time = null;
    }

    public void Start(DateTime t) {
      Stop();
      Time = t;
      _cancel = new CancellationTokenSource();
      CancellationToken token = _cancel.Token;
      _task = Delay(t, token).ContinueWith(_ => {
        if (!token.IsCancellationRequested) _action.Invoke(token);
      });
    }

    public void Dispose() => Stop();

    static async Task Delay(DateTime t, CancellationToken cancel) {
      while (true) {
        DateTime now = DateTime.UtcNow;
        if (t <= now) return;
        await Task.Delay(ToDelayMs(t - now), cancel);
      }
    }

    static int ToDelayMs(TimeSpan t) => (int)Math.Ceiling(Math.Min(Math.Max(t.TotalMilliseconds, 0), int.MaxValue));
  }

  sealed class TaskChain {
    readonly object _monitor = new object();
    readonly Queue<Func<Task>> _work = new Queue<Func<Task>>();
    bool _running = false;

    public void Add(Func<Task> action) {
      Debug.Assert(action != null);
      bool start = false;
      lock (_monitor) {
        if (!_running) {
          _running = true;
          start = true;
        }
        _work.Enqueue(action);
      }
      if (start) {

      }
    }

    public Task Run(Func<Task> action) {
      var res = new Task(delegate { });
      Add(async () => {
        await action.Invoke();
        res.Start();
      });
      return res;
    }

    void Start(Func<Task> action) => Task.Run(action).ContinueWith(_ => {
      Func<Task> next;
      lock (_monitor) {
        Debug.Assert(_running);
        if (_work.Count == 0) {
          _running = false;
          return;
        }
        next = _work.Dequeue();
      }
      Start(next);
    });
  }

  sealed class BufferedWriter : IDisposable {
    // Immutable or thread-safe fields.
    readonly BufferedWriterOptions _opt;
    readonly object _monitor = new object();
    readonly TaskChain _io = new TaskChain();

    // Fields protected by _monitor.
    readonly List<Action> _waiters = new List<Action>();
    readonly Timer _closeBuffer;
    Buffer _buf = null;
    bool _disposed = false;

    // Fields protected by _io.
    readonly ChunkWriter _writer;
    readonly Timer _flushToOS;
    readonly Timer _flushToDisk;
    long? _bufOS = null;
    long? _bufDisk = null;

    public BufferedWriter(string fname) : this(fname, new BufferedWriterOptions()) { }

    public BufferedWriter(string fname, BufferedWriterOptions opt) {
      if (opt == null) throw new ArgumentNullException(nameof(opt));
      opt.Validate();
      _opt = opt.Clone();
      _writer = new ChunkWriter(fname);
      _closeBuffer = new Timer(cancel => { Task _ = DoCloseBuffer(cancel, flushToDisk: null); });
      _flushToOS = new Timer(LazyFlush(false));
      _flushToDisk = new Timer(LazyFlush(true));
      Action<CancellationToken> LazyFlush(bool flushToDisk) =>
          cancel => _io.Add(() => cancel.IsCancellationRequested ? Task.CompletedTask : DoFlush(flushToDisk));
    }

    public string Name => _writer.Name;

    public event Action<Exception> OnWriteError;

    // Returns the newly created locked buffer. Never null.
    //
    // Requires: There is no current buffer.
    public IOutputBuffer NewBuffer() {
      var res = new Buffer(this);
      try {
        lock (_monitor) {
          if (_buf != null) throw new Exception("NewBuffer() cannot be called when there is a current buffer");
          if (_disposed) throw new ObjectDisposedException("BufferedWriter");
          _buf = res;
        }
      } catch {
        res.Cleanup();
        throw;
      }
      if (res.CloseAtAge.HasValue) _closeBuffer.Start(res.CreatedAt + res.CloseAtAge.Value);
      return res;
    }

    // Returns the locked current buffer or null if there is no current buffer.
    public IOutputBuffer GetBuffer() {
      lock (_monitor) {
        if (_disposed) throw new ObjectDisposedException("BufferedWriter");
        if (_buf != null) {
          _buf.Compressor.EnsureUnlocked();
          _buf.Compressor.IsLocked = true;
        }
        return _buf;
      }
    }

    // If there is current buffer, waits until it gets unlocked and writes its content to
    // the underlying ChunkWriter. Otherwise does nothing.
    public Task CloseBufferAsync() => DoCloseBuffer(CancellationToken.None, flushToDisk: null);

    // 1. If there is current buffer, waits until it gets unlocked and closes it.
    // 2. Flushes the underlying ChunkWriter.
    public async Task FlushAsync(bool flushToDisk) {
      await CloseBufferAsync();
      await DoFlush(flushToDisk);
    }

    public void Dispose() {
      lock (_monitor) {
        if (_disposed) return;
        _buf?.Compressor.EnsureUnlocked();
      }
      FlushAsync(flushToDisk: false).Wait();
      _io.Run(() => Task.CompletedTask).Wait();
      lock (_monitor) _disposed = true;
      _writer.Dispose();
    }

    async Task DoCloseBuffer(CancellationToken cancel, bool? flushToDisk) {
      Buffer buf = await GetBuffer();
      if (buf == null && flushToDisk == null) return;

      await _io.Run(async () => {
        if (buf != null) {
          try {
            ArraySegment<byte> content = buf.Compressor.GetCompressed();
            try {
              await _writer.WriteAsync(buf.UserData, content.Array, content.Offset, content.Count);
            } catch (Exception e) {
              try { OnWriteError?.Invoke(e); } catch { }
              return;
            }
            if (_bufDisk == null && _opt.FlushToDisk?.Age != null) {
              _flushToDisk.Start(DateTime.UtcNow + _opt.FlushToDisk.Age.Value);
            }
            if (_bufOS == null && _opt.FlushToOS?.Age != null) {
              _flushToOS.Start(DateTime.UtcNow + _opt.FlushToOS.Age.Value);
            }
            _bufOS = (_bufOS ?? 0) + buf.BytesWritten;
            _bufDisk = (_bufDisk ?? 0) + buf.BytesWritten;
          } finally {
            buf.Cleanup();
          }
        }

        if (_bufDisk >= _opt.FlushToDisk?.Size || flushToDisk == true) {
          await DoFlush(flushToDisk: true);
        } else if (_bufOS >= _opt.FlushToOS?.Size || flushToDisk == false) {
          await DoFlush(flushToDisk: false);
        }
      });

      async Task<Buffer> GetBuffer() {
        Buffer res = null;
        Exception exception = null;
        var done = new Task(delegate { });
        Try();
        await done;
        if (exception != null) throw exception;
        return res;

        void Try() {
          lock (_monitor) {
            if (_disposed) {
              res = null;
              exception = new ObjectDisposedException("BufferedWriter");
              done.Start();
              return;
            }
            if (res == null) res = _buf;
            if (_buf == null || !ReferenceEquals(res, _buf) || cancel.IsCancellationRequested) {
              res = null;
              done.Start();
              return;
            }
            if (res.Compressor.IsLocked) {
              _waiters.Add(Try);
            } else {
              _buf = null;
              _closeBuffer.Stop();
            }
          }
        }
      }
    }

    async Task DoFlush(bool flushToDisk) {
      Debug.Assert((_bufDisk ?? -1) >= (_bufOS ?? -1));
      if (_bufDisk == null || !flushToDisk && _bufOS == null) return;
      try {
        await _writer.FlushAsync(flushToDisk);
      } catch (Exception e) {
        try { OnWriteError?.Invoke(e); } catch { }
        return;
      }
      _bufOS = null;
      _flushToOS.Stop();
      if (flushToDisk) {
        _bufDisk = null;
        _flushToDisk.Stop();
      }
    }

    void Unlock() {
      Debug.Assert(_buf != null);
      Debug.Assert(_buf.Compressor.IsLocked);
      bool close = _buf.BytesWritten >= _buf.CloseAtSize && !_buf.IsAbandoned;
      if (_buf.IsAbandoned) _buf.Cleanup();
      Action[] waiters = null;
      lock (_monitor) {
        if (_waiters.Count > 0) {
          waiters = _waiters.ToArray();
          _waiters.Clear();
        }
        _buf.Compressor.IsLocked = false;
        if (_buf.IsAbandoned) _buf = null;
      }
      if (close) {
        Task _ = CloseBufferAsync();
      }
      foreach (Action w in waiters) w.Invoke();
    }

    sealed class Buffer : IOutputBuffer {
      readonly BufferedWriter _writer;
      UserData _userData = new UserData();
      long? _closeAtSize;

      public Buffer(BufferedWriter writer) {
        _writer = writer;
        Compressor = new Compressor(writer._opt.CompressionLevel);
        _closeAtSize = writer._opt.CloseBuffer?.Size;
        CloseAtAge = writer._opt.CloseBuffer?.Age;
      }

      public bool IsAbandoned { get; internal set; }
      public Compressor Compressor { get; }
      public Stream Stream => Compressor;
      public DateTime CreatedAt { get; } = DateTime.UtcNow;
      public long BytesWritten => Compressor.BytesWritten;

      public UserData UserData {
        get { return _userData; }
        set {
          Compressor.EnsureLocked();
          _userData = value;
        }
      }

      public long? CloseAtSize {
        get { return _closeAtSize; }
        set {
          Compressor.EnsureLocked();
          _closeAtSize = value;
        }
      }

      public TimeSpan? CloseAtAge { get; }

      public void Abandon() {
        Compressor.EnsureLocked();
        IsAbandoned = true;
      }

      public void Dispose() {
        if (!Compressor.IsLocked) return;
        Debug.Assert(ReferenceEquals(_writer._buf, this));
        _writer.Unlock();
      }

      public void Cleanup() => Compressor.Dispose();
    }

    sealed class Compressor : DeflateStream {
      public Compressor(CompressionLevel lvl) : base(new MemoryStream(16 << 10), lvl) { }

      public bool IsLocked { get; set; } = true;
      public long BytesWritten { get; internal set; }

      public void EnsureLocked() {
        if (!IsLocked) throw new InvalidOperationException("The operation requires a locked buffer");
      }

      public void EnsureUnlocked() {
        if (IsLocked) throw new InvalidOperationException("The operation requires an unlocked buffer");
      }

      public ArraySegment<byte> GetCompressed() {
        EnsureUnlocked();
        base.Flush();
        var strm = (MemoryStream)BaseStream;
        return new ArraySegment<byte>(strm.GetBuffer(), 0, (int)strm.Length);
      }

      public override void Flush() { }
      public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

      public override void Write(byte[] buffer, int offset, int count) {
        EnsureLocked();
        base.Write(buffer, offset, count);
        BytesWritten += count;
      }

      public override void WriteByte(byte value) {
        EnsureLocked();
        base.WriteByte(value);
        ++BytesWritten;
      }

      public override async Task WriteAsync(byte[] buffer, int offset, int count,
                                            CancellationToken cancellationToken) {
        EnsureLocked();
        await base.WriteAsync(buffer, offset, count, cancellationToken);
        BytesWritten += count;
      }

      public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count,
                                              AsyncCallback callback, object state) {
        EnsureLocked();
        return new AsyncResult(base.BeginWrite(buffer, offset, count, callback, state), count);
      }

      public override void EndWrite(IAsyncResult asyncResult) {
        try {
          base.EndWrite(asyncResult);
          BytesWritten += ((AsyncResult)asyncResult).Count;
        } finally {
          EnsureLocked();
        }
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
