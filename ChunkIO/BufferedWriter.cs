﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  // It's illegal to call any method of OutputBuffer when it isn't locked. Locked instances are returned
  // by BufferedWriter.NewBuffer() and BufferedWriter.GetBuffer(). They must be unlocked with
  // OutputBuffer.DisposeAsync().
  //
  // Writes to the buffer cannot block on IO but DisposeAsync() can.
  interface IOutputBuffer : IAsyncDisposable {
    // Called right before the buffer is closed unless it was abandoned. Afterwards the buffer no
    // longer gets used, so there is no need to unsubscribe from the event. Can be called either
    // synchronously from DisposeAsync() or from another thread at any time. The buffer is considered
    // locked when the event fires.
    event Action OnClose;

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

    // Drops the buffer. OnClose won't get called and the buffer won't be written to disk.
    // You must still unlock the buffer with DisposeAsync() after calling Abandon().
    // The next call to GetBuffer() will return null.
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
    }

    public string Name => _writer.Name;

    // Returns the newly created locked buffer. Never null.
    //
    // Requires: There is no current buffer.
    public async Task<IOutputBuffer> NewBuffer() {
      if (_disposed) throw new ObjectDisposedException("BufferedWriter");
      return await _sem.WithLock(() => {
        if (_buf != null) throw new Exception("Close the current buffer before creating a new one");
        _buf = new Buffer(this);
        _closeBuffer.RunAt(_buf.CreatedAt + _buf.CloseAtAge.Value);
        return Task.FromResult(_buf);
      });
    }

    // Returns the locked current buffer or null if there is no current buffer.
    public async Task<IOutputBuffer> GetBuffer() {
      if (_disposed) throw new ObjectDisposedException("BufferedWriter");
      return await _sem.WithLock(() => Task.FromResult(_buf));
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
        _sem.WithLock(async () => {
          try {
            await DoCloseBuffer(flushToDisk: false);
          } finally {
            _closeBuffer.Stop();
            _flushToOS.Stop();
            _flushToDisk.Stop();
            _writer.Dispose();
          }
        }).Wait();
      } finally {
        _sem.Dispose();
        _disposed = true;
      }
    }

    async Task DoCloseBuffer(bool? flushToDisk) {
      if (_buf != null) {
        if (!_buf.IsAbandoned) {
          _buf.FireOnClose();
          ArraySegment<byte> content = _buf.Compressor.GetCompressed();
          await _writer.WriteAsync(_buf.UserData, content.Array, content.Offset, content.Count);
          if (_bufDisk == null && _opt.FlushToDisk?.Age != null) {
            _flushToDisk.RunAt(DateTime.UtcNow + _opt.FlushToDisk.Age.Value);
          }
          if (_bufOS == null && _opt.FlushToOS?.Age != null) {
            _flushToOS.RunAt(DateTime.UtcNow + _opt.FlushToOS.Age.Value);
          }
          _bufOS = (_bufOS ?? 0) + _buf.BytesWritten;
          _bufDisk = (_bufDisk ?? 0) + _buf.BytesWritten;
        }
        _closeBuffer.Stop();
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
      _flushToOS.Stop();
      if (flushToDisk) {
        _bufDisk = null;
        _flushToDisk.Stop();
      }
    }

    async Task Unlock() {
      try {
        if (_buf.BytesWritten >= _buf.CloseAtSize || _buf.IsAbandoned) {
          await DoCloseBuffer(flushToDisk: null);
        } else if (_buf.CreatedAt + _buf.CloseAtAge != _closeBuffer.Time) {
          _closeBuffer.RunAt(_buf.CreatedAt + _buf.CloseAtAge);
        }
      } finally {
        _sem.Release();
      }
    }

    sealed class Buffer : IOutputBuffer, IDisposable {
      readonly BufferedWriter _writer;

      public Buffer(BufferedWriter writer) {
        _writer = writer;
        Compressor = new Compressor(writer._opt.CompressionLevel);
        CloseAtSize = writer._opt.CloseBuffer?.Size;
        CloseAtAge = writer._opt.CloseBuffer?.Age;
      }

      public event Action OnClose;

      public bool IsAbandoned { get; internal set; }
      public Compressor Compressor { get; }
      public Stream Stream => Compressor;
      public DateTime CreatedAt { get; } = DateTime.UtcNow;
      public long BytesWritten => Compressor.BytesWritten;
      public UserData UserData { get; set; }
      public object UserState { get; set; }
      public long? CloseAtSize { get; set; }
      public TimeSpan? CloseAtAge { get; set; }

      public void FireOnClose() { OnClose?.Invoke(); }
      public void Abandon() { IsAbandoned = true; }
      public void Dispose() => Compressor.Dispose();
      public Task DisposeAsync() => _writer.Unlock();
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

  static class SemaphoreSlimExtensions {
    public static async Task WithLock(this SemaphoreSlim sem, Func<Task> action) {
      sem.Wait();
      try {
        await action.Invoke();
      } finally {
        sem.Release();
      }
    }

    public static async Task<T> WithLock<T>(this SemaphoreSlim sem, Func<Task<T>> action) {
      sem.Wait();
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

    public void RunAt(DateTime? t) {
      Stop();
      if (t.HasValue) {
        Time = t;
        _cancel = new CancellationTokenSource();
        _task = Run();
      }
    }

    public void Dispose() => Stop();

    async Task Run() {
      CancellationToken token = _cancel.Token;
      try {
        await Delay(Time.Value, token);
      } catch (TaskCanceledException) {
        return;
      }
      await _sem.WithLock(() => token.IsCancellationRequested ? Task.CompletedTask : _action.Invoke());
    }

    static async Task Delay(DateTime t, CancellationToken cancel) {
      while (true) {
        DateTime now = DateTime.UtcNow;
        if (t <= now) return;
        await Task.Delay(ToDelayMs(t - now), cancel);
      }
    }

    static int ToDelayMs(TimeSpan t) => (int)Math.Ceiling(Math.Min(Math.Max(t.TotalMilliseconds, 0), int.MaxValue));
  }
}
