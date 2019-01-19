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
  // It's illegal to call any method of OutputBuffer or its Stream when the buffer isn't locked.
  // Locked buffers are returned by BufferedWriter.LockBuffer(). They must be unlocked with
  // OutputBuffer.DisposeAsync() or OutputBuffer.Dispose().
  //
  // Writes to the buffer do not block on IO but DisposeAsync() and Dispose() potentially do.
  interface IOutputBuffer : IDisposable, IAsyncDisposable {
    // Called right before the buffer is closed unless it was abandoned. Afterwards the buffer no
    // longer gets used, so there is no need to unsubscribe from the event. Can fire either
    // synchronously from DisposeAsync() or Dispose(), or from another thread at any time. The buffer is
    // considered locked when the event fires, hence it's legal to write to it. If Abandon() is
    // called from OnClose, the buffer is dropped on the floor.
    event Action OnClose;

    // Flush() and FlushAsync() have no effect. Use BufferedWriter.CloseBufferAsync() and
    // BufferedWriter.FlushAsync() to close and flush the buffer.
    //
    // The user MUST NOT Dispose() or Close() the stream.
    MemoryStream Stream { get; }

    // This buffer was created by the latest call to BufferedWriter.GetBuffer().
    bool IsNew { get; }
    // Time when this buffer was created.
    DateTime CreatedAt { get; }
    // User data of the chunk. Passed through to the underlying ChunkWriter. Set to default(UserData)
    // in new chunks.
    UserData UserData { get; set; }
    // Placeholder for anything the user might need to store alongside the buffer. Set to null in
    // new chunks.
    object UserState { get; set; }

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

    // Drops the buffer without writing it to disk. The user must still unlock the buffer with
    // DisposeAsync() or Dispose() after calling Abandon(). The next call to BufferedWriter.LockBuffer()
    // will create a new buffer.
    //
    // It's legal to call Abandon() from OnClose. If called before OnClose is fired, the latter
    // won't fire.
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

  sealed class BufferedWriter : IDisposable {
    readonly SemaphoreSlim _sem = new SemaphoreSlim(1, 1);
    readonly BufferedWriterOptions _opt;
    readonly ChunkWriter _writer;
    readonly IDisposable _listener;
    readonly Timer _closeBuffer;
    readonly Timer _flushToOS;
    readonly Timer _flushToDisk;
    Buffer _buf = null;
    long? _bufOS = null;
    long? _bufDisk = null;
    volatile bool _disposed = false;

    public BufferedWriter(string fname) : this(fname, new BufferedWriterOptions()) { }

    public BufferedWriter(string fname, BufferedWriterOptions opt) {
      if (opt == null) throw new ArgumentNullException(nameof(opt));
      opt.Validate();
      _opt = opt.Clone();
      _writer = new ChunkWriter(fname);
      _closeBuffer = new Timer(_sem, () => DoCloseBuffer(flushToDisk: null));
      _flushToOS = new Timer(_sem, () => DoFlush(flushToDisk: false));
      _flushToDisk = new Timer(_sem, () => DoFlush(flushToDisk: true));
      if (opt.AllowRemoteFlush) {
        _listener = RemoteFlush.CreateListener(fname, FlushAsync);
      }
    }

    public string Name => _writer.Name;

    // If there is a current buffer, locks and returns it. IOutputBuffer.IsNew is false.
    // Otherwise creates a new buffer, locks and returns it. IOutputBuffer.IsNew is true.
    public async Task<IOutputBuffer> LockBuffer() {
      if (_disposed) throw new ObjectDisposedException("BufferedWriter");
      await _sem.WaitAsync();
      if (_buf != null) return new LockedBuffer(_buf, isNew: false);
      _buf = new Buffer(this);
      await _closeBuffer.RunAt(_buf.CreatedAt + _buf.CloseAtAge.Value);
       return new LockedBuffer(_buf, isNew: true);
    }

    // If there is current buffer, waits until it gets unlocked and writes its content to
    // the underlying ChunkWriter. Otherwise does nothing.
    public async Task CloseBufferAsync() {
      if (_disposed) throw new ObjectDisposedException("BufferedWriter");
      await _sem.WithLock(() => DoCloseBuffer(flushToDisk: null));
    }

    // 1. If there is current buffer, waits until it gets unlocked and closes it.
    // 2. Flushes the underlying ChunkWriter.
    public async Task FlushAsync(bool flushToDisk) {
      if (_disposed) throw new ObjectDisposedException("BufferedWriter");
      await _sem.WithLock(() => DoCloseBuffer(flushToDisk));
    }

    public void Dispose() {
      if (_disposed) return;
      try {
        _listener?.Dispose();
      } finally {
        try {
          _sem.WithLock(async () => {
            try {
              await DoCloseBuffer(flushToDisk: false);
            } finally {
              await _closeBuffer.Stop();
              await _flushToOS.Stop();
              await _flushToDisk.Stop();
              _writer.Dispose();
            }
          }).Wait();
        } finally {
          _sem.Dispose();
          _disposed = true;
        }
      }
    }

    async Task DoCloseBuffer(bool? flushToDisk) {
      if (_buf != null) {
        if (!_buf.IsAbandoned) _buf.FireOnClose();
        if (!_buf.IsAbandoned) {
          ArraySegment<byte> content = Compression.Compress(
              _buf.Stream.GetBuffer(), 0, (int)_buf.Stream.Length, _opt.CompressionLevel);
          await _writer.WriteAsync(_buf.UserData, content.Array, content.Offset, content.Count);
          if (_bufDisk == null && _opt.FlushToDisk?.Age != null) {
            await _flushToDisk.RunAt(DateTime.UtcNow + _opt.FlushToDisk.Age.Value);
          }
          if (_bufOS == null && _opt.FlushToOS?.Age != null) {
            await _flushToOS.RunAt(DateTime.UtcNow + _opt.FlushToOS.Age.Value);
          }
          _bufOS = (_bufOS ?? 0) + _buf.Stream.Length;
          _bufDisk = (_bufDisk ?? 0) + _buf.Stream.Length;
        }
        await _closeBuffer.Stop();
        _buf.Dispose();
        _buf = null;
      }

      if (flushToDisk == true || _bufDisk >= _opt.FlushToDisk?.Size) {
        await DoFlush(flushToDisk: true);
      } else if (flushToDisk == false || _bufOS >= _opt.FlushToOS?.Size) {
        await DoFlush(flushToDisk: false);
      }
    }

    async Task DoFlush(bool flushToDisk) {
      Debug.Assert((_bufDisk ?? -1) >= (_bufOS ?? -1));
      if (_bufDisk == null || !flushToDisk && _bufOS == null) return;
      await _writer.FlushAsync(flushToDisk);
      _bufOS = null;
      await _flushToOS.Stop();
      if (flushToDisk) {
        _bufDisk = null;
        await _flushToDisk.Stop();
      }
    }

    async Task Unlock() {
      try {
        if (_buf.Stream.Length >= _buf.CloseAtSize || _buf.IsAbandoned) {
          await DoCloseBuffer(flushToDisk: null);
        } else if (_buf.CreatedAt + _buf.CloseAtAge != _closeBuffer.Time) {
          await _closeBuffer.RunAt(_buf.CreatedAt + _buf.CloseAtAge);
        }
      } finally {
        _sem.Release();
      }
    }

    sealed class LockedBuffer : IOutputBuffer {
      readonly Buffer _buf;
      bool _locked = true;

      public LockedBuffer(Buffer buf, bool isNew) {
        _buf = buf;
        IsNew = isNew;
      }

      public event Action OnClose {
        add { _buf.OnClose += value; }
        remove { _buf.OnClose -= value; }
      }

      public bool IsNew { get; }
      public MemoryStream Stream => _buf.Stream;
      public DateTime CreatedAt => _buf.CreatedAt;

      public UserData UserData {
        get => _buf.UserData;
        set { _buf.UserData = value; }
      }
      public object UserState {
        get => _buf.UserState;
        set { _buf.UserState = value; }
      }
      public long? CloseAtSize {
        get => _buf.CloseAtSize;
        set { _buf.CloseAtSize = value; }
      }
      public TimeSpan? CloseAtAge {
        get => _buf.CloseAtAge;
        set { _buf.CloseAtAge = value; }
      }

      public void Abandon() { _buf.Abandon(); }
      public void Dispose() => DisposeAsync().Wait();
      public Task DisposeAsync() {
        if (!_locked) return Task.CompletedTask;
        _locked = false;
        return _buf.Unlock();
      }
    }

    sealed class Buffer : IDisposable {
      readonly BufferedWriter _writer;

      public Buffer(BufferedWriter writer) {
        _writer = writer;
        CloseAtSize = writer._opt.CloseBuffer?.Size;
        CloseAtAge = writer._opt.CloseBuffer?.Age;
      }

      public event Action OnClose;

      public MemoryStream Stream { get; } = new MemoryStream(4 << 10);
      public DateTime CreatedAt { get; } = DateTime.UtcNow;
      public UserData UserData { get; set; }
      public object UserState { get; set; }
      public long? CloseAtSize { get; set; }
      public TimeSpan? CloseAtAge { get; set; }
      public bool IsAbandoned { get; internal set; }

      public void Abandon() { IsAbandoned = true; }
      public void Dispose() => Stream.Dispose();
      public void FireOnClose() => OnClose?.Invoke();
      public Task Unlock() => _writer.Unlock();
    }
  }

  static class SemaphoreSlimExtensions {
    public static async Task WithLock(this SemaphoreSlim sem, Func<Task> action) {
      await sem.WaitAsync();
      try {
        await action.Invoke();
      } finally {
        sem.Release();
      }
    }

    public static async Task<T> WithLock<T>(this SemaphoreSlim sem, Func<Task<T>> action) {
      await sem.WaitAsync();
      try {
        return await action.Invoke();
      } finally {
        sem.Release();
      }
    }
  }

  // Thread-compatible but not thread-safe.
  sealed class Timer : IDisposable {
    readonly SemaphoreSlim _sem;
    readonly Func<Task> _action;
    CancellationTokenSource _cancel = null;
    Task _task = null;

    public Timer(SemaphoreSlim sem, Func<Task> action) {
      Debug.Assert(sem != null);
      Debug.Assert(action != null);
      _sem = sem;
      _action = action;
    }

    public DateTime? Time { get; internal set; }

    public async Task Stop() {
      Debug.Assert((_task == null) == (_cancel == null) && (_task == null) == (Time == null));
      if (_task == null) return;

      _cancel.Cancel();
      try { await _task; } catch { }
      _cancel.Dispose();
      _task = null;
      _cancel = null;
      Time = null;
    }

    // The returned task completes when the action is scheduled, which happens almost immediately.
    public async Task RunAt(DateTime? t) {
      await Stop();
      if (t.HasValue) {
        Time = t;
        _cancel = new CancellationTokenSource();
        _task = Run();
      }
    }

    public void Dispose() => Stop().Wait();

    async Task Run() {
      CancellationToken token = _cancel.Token;
      try {
        await Delay(Time.Value, token);
      } catch (TaskCanceledException) {
        return;
      }
      try {
        await _sem.WaitAsync(token);
      } catch (TaskCanceledException) {
        return;
      }
      try {
        await _action.Invoke();
      } finally {
        _sem.Release();
      }
    }

    static async Task Delay(DateTime t, CancellationToken cancel) {
      await Task.Yield();
      while (true) {
        DateTime now = DateTime.UtcNow;
        if (t <= now) return;
        await Task.Delay(ToDelayMs(t - now), cancel);
      }
    }

    static int ToDelayMs(TimeSpan t) => (int)Math.Ceiling(Math.Min(Math.Max(t.TotalMilliseconds, 0), int.MaxValue));
  }
}
