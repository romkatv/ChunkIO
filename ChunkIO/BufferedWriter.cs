// Copyright 2019 Roman Perepelitsa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
  // It's illegal to call any method of OutputChunk or its Stream when the chunk isn't locked.
  // Locked chunks are returned by BufferedWriter.LockChunk(). They must be unlocked with
  // OutputChunk.DisposeAsync() or OutputChunk.Dispose().
  //
  // Writes to the chunk do not block on IO but DisposeAsync() and Dispose() potentially do.
  interface IOutputChunk : IDisposable, IAsyncDisposable {
    // Called right before the chunk is closed unless it was abandoned. Afterwards the chunk no
    // longer gets used, so there is no need to unsubscribe from the event. Can fire either
    // synchronously from DisposeAsync() or Dispose(), or from another thread at any time. The chunk is
    // considered locked when the event fires, hence it's legal to write to it. If Abandon() is
    // called from OnClose, the chunk is dropped on the floor.
    event Action OnClose;

    // Flush() and FlushAsync() have no effect. Use BufferedWriter.CloseChunkAsync() and
    // BufferedWriter.FlushAsync() to close and flush the chunk.
    //
    // The user MUST NOT Dispose() or Close() the stream.
    MemoryStream Stream { get; }

    // This chunk was created by the latest call to BufferedWriter.GetChunk().
    bool IsNew { get; }
    // Time when this chunk was created.
    DateTime CreatedAt { get; }
    // User data of the chunk. Passed through to the underlying ChunkWriter. Set to default(UserData)
    // in new chunks.
    UserData UserData { get; set; }
    // Placeholder for anything the user might need to store alongside the chunk. Set to null in
    // new chunks.
    object UserState { get; set; }

    // When a chunk is created, CloseAtSize and CloseAtAge are set to
    // BufferedWriterOptions.CloseChunk.Size and BufferedWriterOptions.Age respectively.
    //
    // A chunk is automatically closed when all of the following conditions are true:
    //
    // 1. The chunk is not locked.
    // 2. Stream.Length >= CloseAtSize || DateTime.UtcNow - CreatedAt >= CloseAtAge.
    //
    // An implication of this is that the chunk won't get closed if the second condition
    // becomes true while the chunk is locked as long as it reverts to false before unlocking.
    long? CloseAtSize { get; set; }
    TimeSpan? CloseAtAge { get; }

    // Drops the chunk without writing it to disk. The user must still unlock the chunk with
    // DisposeAsync() or Dispose() after calling Abandon(). The next call to BufferedWriter.LockChunk()
    // will create a new chunk.
    //
    // It's legal to call Abandon() from OnClose. If called before OnClose is fired, the latter
    // won't fire.
    void Abandon();
  }

  public class Triggers {
    // Triggers when the data gets bigger than this. This can happen only when unlocking a chunk with
    // IOutputChunk.DisposeAsync(). Errors from the trigger are reported as exceptions and the trigger
    // isn't retried automatically.
    //
    // Requires: Size == null || Size >= 0.
    public long? Size { get; set; }

    // Triggers when the data gets older than this. This can happen only asynchronously while the user
    // isn't holding a chunk lock. If the trigger fails, it's retried after AgeRetry.
    //
    // Requires: Age == null || Age >= TimeSpan.Zero.
    // Requires: Age == null || AgeRetry != null.
    public TimeSpan? Age { get; set; }

    // When the age-based trigger fails, it's retried after this long.
    //
    // Requires: AgeRetry == null || AgeRetry >= TimeSpan.Zero.
    // Requires: Age == null || AgeRetry != null.
    public TimeSpan? AgeRetry { get; set; }

    public Triggers Clone() => (Triggers)MemberwiseClone();

    public void Validate() {
      if (Size < 0) throw new Exception($"Invalid Triggers.Size: {Size}");
      if (Age < TimeSpan.Zero) throw new Exception($"Invalid Triggers.Age: {Age}");
      if (AgeRetry < TimeSpan.Zero) throw new Exception($"Invalid Triggers.AgeRetry: {AgeRetry}");
      if (Age != null && AgeRetry == null) throw new Exception("Triggers.AgeRetry must be set");
    }
  }

  public class WriterOptions {
    public bool AllowRemoteFlush { get; set; } = true;

    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;

    // CloseChunk triggers define when a chunk should be auto-closed. See comments
    // for IOutputChunk.CloseAtSize and IOutputChunk.Age for details.
    public Triggers CloseChunk { get; set; } = new Triggers() { Size = 64 << 10 };

    // FlushToOS and FlushToDisk triggers define when chunks that have already been
    // closed are flushed to OS and disk respectively. When FlushToDisk triggers, it
    // automatically triggers FlushToOS. Neither FlushToOS nor FlushToDisk close the
    // current chunk; they only flush previously closed chunks.
    //
    // The timer for these triggers starts when a chunk is closed. Their size-based
    // trackers are incremented only when chunks are closed.
    public Triggers FlushToOS { get; set; } = new Triggers();
    public Triggers FlushToDisk { get; set; } = new Triggers();

    public WriterOptions Clone() {
      var res = (WriterOptions)MemberwiseClone();
      res.CloseChunk = res.CloseChunk?.Clone();
      res.FlushToOS = res.FlushToOS?.Clone();
      res.FlushToDisk = res.FlushToDisk?.Clone();
      return res;
    }

    public void Validate() {
      if (!Enum.IsDefined(typeof(CompressionLevel), CompressionLevel)) {
        throw new Exception($"Invalid CompressionLevel: {CompressionLevel}");
      }
      CloseChunk?.Validate();
      FlushToOS?.Validate();
      FlushToDisk?.Validate();
    }
  }

  sealed class BufferedWriter : IDisposable {
    readonly SemaphoreSlim _sem = new SemaphoreSlim(1, 1);
    readonly WriterOptions _opt;
    readonly ChunkWriter _writer;
    readonly IDisposable _listener;
    readonly Timer _closeChunk;
    readonly Timer _flushToOS;
    readonly Timer _flushToDisk;
    Chunk _buf = null;
    long? _bufOS = null;
    long? _bufDisk = null;
    volatile bool _disposed = false;

    public BufferedWriter(string fname) : this(fname, new WriterOptions()) { }

    public BufferedWriter(string fname, WriterOptions opt) {
      if (opt == null) throw new ArgumentNullException(nameof(opt));
      opt.Validate();
      _opt = opt.Clone();
      _writer = new ChunkWriter(fname);
      _closeChunk = new Timer(_sem, () => DoCloseChunk(flushToDisk: null), _opt.CloseChunk?.AgeRetry);
      _flushToOS = new Timer(_sem, () => DoFlush(flushToDisk: false), _opt.FlushToOS?.AgeRetry);
      _flushToDisk = new Timer(_sem, () => DoFlush(flushToDisk: true), _opt.FlushToDisk?.AgeRetry);
      if (_opt.AllowRemoteFlush) {
        _listener = RemoteFlush.CreateListener(fname, FlushAsync);
      }
    }

    public string Name => _writer.Name;

    // If there is a current chunk, locks and returns it. IOutputChunk.IsNew is false.
    // Otherwise creates a new chunk, locks and returns it. IOutputChunk.IsNew is true.
    public async Task<IOutputChunk> LockChunk() {
      if (_disposed) throw new ObjectDisposedException("BufferedWriter");
      await _sem.WaitAsync();
      if (_buf != null) return new LockedChunk(_buf, isNew: false);
      _buf = new Chunk(this);
      await _closeChunk.ScheduleAt(_buf.CreatedAt + _buf.CloseAtAge);
      return new LockedChunk(_buf, isNew: true);
    }

    // If there is current chunk, waits until it gets unlocked and writes its content to
    // the underlying ChunkWriter. Otherwise does nothing.
    public async Task CloseChunkAsync() {
      if (_disposed) throw new ObjectDisposedException("BufferedWriter");
      await _sem.WithLock(() => DoCloseChunk(flushToDisk: null));
    }

    // 1. If there is current chunk, waits until it gets unlocked and closes it.
    // 2. Flushes the underlying ChunkWriter.
    public async Task FlushAsync(bool flushToDisk) {
      if (_disposed) throw new ObjectDisposedException("BufferedWriter");
      await _sem.WithLock(() => DoCloseChunk(flushToDisk));
    }

    public void Dispose() {
      if (_disposed) return;
      try {
        _listener?.Dispose();
      } finally {
        try {
          _sem.WithLock(async () => {
            try {
              await DoCloseChunk(flushToDisk: false);
            } finally {
              await _closeChunk.Stop();
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

    async Task DoCloseChunk(bool? flushToDisk) {
      Debug.Assert(!_disposed);
      Debug.Assert(_sem.CurrentCount == 0);
      if (_buf != null) {
        if (!_buf.IsAbandoned) _buf.FireOnClose();
        if (!_buf.IsAbandoned) {
          ArraySegment<byte> content = Compression.Compress(
              _buf.Stream.GetBuffer(), 0, (int)_buf.Stream.Length, _opt.CompressionLevel);
          await _writer.WriteAsync(_buf.UserData, content.Array, content.Offset, content.Count);
          if (_bufDisk == null) await _flushToDisk.ScheduleAt(DateTime.UtcNow + _opt.FlushToDisk.Age);
          if (_bufOS == null) await _flushToOS.ScheduleAt(DateTime.UtcNow + _opt.FlushToOS.Age);
          _bufOS = (_bufOS ?? 0) + _buf.Stream.Length;
          _bufDisk = (_bufDisk ?? 0) + _buf.Stream.Length;
        }
        await _closeChunk.Stop();
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
      Debug.Assert(!_disposed);
      Debug.Assert(_sem.CurrentCount == 0);
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
      Debug.Assert(!_disposed);
      Debug.Assert(_sem.CurrentCount == 0);
      try {
        if (_buf.Stream.Length >= _buf.CloseAtSize || _buf.IsAbandoned) {
          await DoCloseChunk(flushToDisk: null);
        }
      } finally {
        _sem.Release();
      }
    }

    sealed class LockedChunk : IOutputChunk {
      readonly Chunk _buf;
      bool _locked = true;

      public LockedChunk(Chunk buf, bool isNew) {
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
      public TimeSpan? CloseAtAge => _buf.CloseAtAge;

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

      public void Abandon() { _buf.Abandon(); }
      public void Dispose() => DisposeAsync().Wait();
      public Task DisposeAsync() {
        if (!_locked) return Task.CompletedTask;
        _locked = false;
        return _buf.Unlock();
      }
    }

    sealed class Chunk : IDisposable {
      readonly BufferedWriter _writer;

      public Chunk(BufferedWriter writer) {
        _writer = writer;
        CloseAtSize = writer._opt.CloseChunk?.Size;
        CloseAtAge = writer._opt.CloseChunk?.Age;
      }

      public event Action OnClose;

      public MemoryStream Stream { get; } = new MemoryStream(4 << 10);
      public DateTime CreatedAt { get; } = DateTime.UtcNow;
      public UserData UserData { get; set; }
      public object UserState { get; set; }
      public long? CloseAtSize { get; set; }
      public TimeSpan? CloseAtAge { get; }
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
  sealed class Timer {
    readonly SemaphoreSlim _sem;
    readonly Func<Task> _action;
    readonly TimeSpan? _retry;
    CancellationTokenSource _cancel = null;
    DateTime? _time = null;
    Task _task = null;

    public Timer(SemaphoreSlim sem, Func<Task> action, TimeSpan? retry) {
      Debug.Assert(sem != null);
      Debug.Assert(action != null);
      Debug.Assert(retry == null || retry >= TimeSpan.Zero);
      _sem = sem;
      _action = action;
      _retry = retry;
    }

    // Requires: The semaphore is locked.
    public async Task Stop() {
      Debug.Assert((_task == null) == (_cancel == null) && (_task == null) == (_time == null));
      if (_task != null) {
        _cancel.Cancel();
        try { await _task; } catch { }
        Debug.Assert(_task == null && _cancel == null && _time == null);
      }
    }

    // Requires: The semaphore is locked.
    //
    // The returned task completes when the action is scheduled, which happens almost immediately.
    public async Task ScheduleAt(DateTime? t) {
      await Stop();
      if (t.HasValue) {
        Debug.Assert(_retry.HasValue);
        _time = t;
        _cancel = new CancellationTokenSource();
        _task = Run();
      }
    }

    async Task Run() {
      DateTime? retry = _time + _retry;
      try {
        await Delay(_time.Value, _cancel.Token);
        await _sem.WaitAsync(_cancel.Token);
      } finally {
        _cancel.Dispose();
        _task = null;
        _cancel = null;
        _time = null;
      }
      try {
        await _action.Invoke();
      } catch {
        if (_task == null) {
          DateTime now = DateTime.UtcNow;
          if (now > retry) retry = now;
          await ScheduleAt(retry);
        }
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
