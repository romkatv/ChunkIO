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
  // Writes to the chunk do not block on IO but BufferedWriter.LockChunk(), IOutputChunk.DisposeAsync()
  // and IOutputChunk.Dispose() potentially do.
  interface IOutputChunk : IDisposable, IAsyncDisposable {
    // Called right before the chunk is closed unless it was abandoned. Afterwards the chunk no
    // longer gets used, so there is no need to unsubscribe from the event. Can fire either
    // synchronously from DisposeAsync() or Dispose(), or from another thread at any time. The chunk is
    // considered locked when the event fires, hence it's legal to write to it. If Abandon() is
    // called from OnClose, the chunk is dropped on the floor. If OnClose throws, it'll be called
    // again on the next attempt to close the chunk.
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
    ref UserData UserData { get; }
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

    // BufferedWriter.Dispose() and BufferedWriter.DisposeAsync() invoke BufferedWriter.FlushAsync(arg) on
    // the first call. DisposeFlushToDisk is the argument.
    public bool DisposeFlushToDisk { get; set; } = true;

    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;

    // CloseChunk triggers define when a chunk should be auto-closed. See comments
    // for IOutputChunk.CloseAtSize and IOutputChunk.Age for details.
    public Triggers CloseChunk { get; set; } = new Triggers() { Size = 128 << 10 };

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

  // It's allowed to call all public methods of BufferedWriter concurrently, even Dispose() and DisposeAsync().
  //
  // Example: Create a writer, write two identical two-byte records and flush.
  //
  //   var opt = new WriterOptions() {
  //     // Auto-close chunks when they get big or old enough.
  //     CloseChunk = new Triggers { Size = 32 << 10, Age = TimeSpan.FromSeconds(60) },
  //     // Flush data to disk (ala fsync) when it gets old enough.
  //     FlushToDisk = new Triggers { Age = TimeSpan.FromSeconds(300) },
  //   };
  //   async using (var writer = new BufferedWriter(fname, opt)) {
  //     // Write two identical records. They may end up in two different chunks if CloseChunk,
  //     // FlushToOS or FlushToDisk is triggered after the first record is written. Or if a reader
  //     // remotely triggers flush.
  //     for (int i = 0; i != 2; ++i) {
  //       async using (IOutputChunk chunk = await writer.LockChunk()) {
  //         // If LockChunk() gave us a brand new chunk, set user data.
  //         if (chunk.IsNew) chunk.UserData = new UserData() { ULong0 = 1, ULong1 = 2 };
  //         // Write one record. The cannot be written to disk until we unlock it by
  //         // disposing the local `chunk` handle.
  //         chunk.Stream.WriteByte(42);
  //         chunk.Stream.WriteByte(69);
  //       }
  //       async writer.FlushAsync(flushToDisk: true);
  //     }
  //   }
  sealed class BufferedWriter : IDisposable, IAsyncDisposable {
    readonly AsyncMutex _mutex = new AsyncMutex();
    readonly WriterOptions _opt;
    readonly ChunkWriter _writer;
    readonly IAsyncDisposable _listener;
    readonly Timer _closeChunk;
    readonly Timer _flushToOS;
    readonly Timer _flushToDisk;
    readonly MemoryStream _strm = new MemoryStream();
    Chunk _buf = null;
    long? _bufOS = null;
    long? _bufDisk = null;
    bool _disposed = false;

    public BufferedWriter(string fname) : this(fname, new WriterOptions()) { }

    public BufferedWriter(string fname, WriterOptions opt) {
      if (opt == null) throw new ArgumentNullException(nameof(opt));
      opt.Validate();
      _opt = opt.Clone();
      _writer = new ChunkWriter(fname);
      _closeChunk = new Timer(_mutex, () => DoCloseChunk(flushToDisk: null), _opt.CloseChunk?.AgeRetry);
      _flushToOS = new Timer(_mutex, () => DoFlush(flushToDisk: false), _opt.FlushToOS?.AgeRetry);
      _flushToDisk = new Timer(_mutex, () => DoFlush(flushToDisk: true), _opt.FlushToDisk?.AgeRetry);
      if (_opt.AllowRemoteFlush) _listener = new RemoteFlush.Listener(_writer.Id, FlushAsync);
    }

    public IReadOnlyCollection<byte> Id => _writer.Id;
    public string Name => _writer.Name;

    // If there is a current chunk, locks and returns it. IOutputChunk.IsNew is false.
    // Otherwise creates a new chunk, locks and returns it. IOutputChunk.IsNew is true.
    //
    // If an error was encountered when an attempt was made to flush the last chunk,
    // LockChunk() will try to flush it and will throw if unable to do so.
    public async Task<IOutputChunk> LockChunkAsync() {
      await _mutex.LockAsync();
      try {
        CheckDispose();
        if (_buf != null) {
          if (!_buf.CompressedContent.HasValue) {
            Debug.Assert(!_buf.IsAbandoned);
            return new LockedChunk(_buf, isNew: false);
          }
          await DoCloseChunk(flushToDisk: false);
        }
        Debug.Assert(_buf == null);
        _buf = new Chunk(this);
        _closeChunk.ScheduleAt(_buf.CreatedAt + _buf.CloseAtAge);
        return new LockedChunk(_buf, isNew: true);
      } catch {
        _mutex.Unlock(runNextSynchronously: false);
        throw;
      }
    }

    // If there is current chunk, waits until it gets unlocked and writes its content to
    // the underlying ChunkWriter. Otherwise does nothing.
    public Task CloseChunkAsync() => WithLock(() => {
      CheckDispose();
      return DoCloseChunk(flushToDisk: null);
    });

    // 1. If there is current chunk, waits until it gets unlocked and closes it.
    // 2. Flushes the underlying ChunkWriter.
    public Task<long> FlushAsync(bool flushToDisk) => WithLock(async () => {
      CheckDispose();
      await DoCloseChunk(flushToDisk);
      return _writer.Length;
    });

    public async Task DisposeAsync() {
      try {
        if (_listener != null) await _listener.DisposeAsync();
      } finally {
        await WithLock(async () => {
          if (_disposed) return;
          _disposed = true;
          try {
            await DoCloseChunk(flushToDisk: _opt.DisposeFlushToDisk);
          } finally {
            _strm.Dispose();
            _closeChunk.Stop();
            _flushToOS.Stop();
            _flushToDisk.Stop();
            _writer.Dispose();
          }
        });
      }
    }

    public void Dispose() => DisposeAsync().Wait();

    async Task DoCloseChunk(bool? flushToDisk) {
      Debug.Assert(_mutex.IsLocked);
      if (_buf != null) {
        if (_buf.Close(_opt.CompressionLevel)) {
          ArraySegment<byte> compressed = _buf.CompressedContent.Value;
          await _writer.WriteAsync(_buf.UserData, compressed.Array, compressed.Offset, compressed.Count);
          if (_bufDisk == null) _flushToDisk.ScheduleAt(DateTime.UtcNow + _opt.FlushToDisk.Age);
          if (_bufOS == null) _flushToOS.ScheduleAt(DateTime.UtcNow + _opt.FlushToOS.Age);
          _bufOS = (_bufOS ?? 0) + _buf.Stream.Length;
          _bufDisk = (_bufDisk ?? 0) + _buf.Stream.Length;
        }
        _closeChunk.Stop();
        _buf = null;
      }

      if (flushToDisk == true || _bufDisk >= _opt.FlushToDisk?.Size) {
        await DoFlush(flushToDisk: true);
      } else if (flushToDisk == false || _bufOS >= _opt.FlushToOS?.Size) {
        await DoFlush(flushToDisk: false);
      }
    }

    async Task DoFlush(bool flushToDisk) {
      Debug.Assert(_mutex.IsLocked);
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
      Debug.Assert(_mutex.IsLocked);
      try {
        if (_buf.Stream.Length >= _buf.CloseAtSize || _buf.IsAbandoned) {
          await DoCloseChunk(flushToDisk: null);
        }
      } finally {
        _mutex.Unlock(runNextSynchronously: false);
      }
    }

    void CheckDispose() {
      Debug.Assert(_mutex.IsLocked);
      if (_disposed) throw new ObjectDisposedException(nameof(BufferedWriter));
    }

    async Task WithLock(Func<Task> action) {
      Debug.Assert(action != null);
      await _mutex.LockAsync();
      try {
        await action.Invoke();
      } finally {
        _mutex.Unlock(runNextSynchronously: false);
      }
    }

    async Task<T> WithLock<T>(Func<Task<T>> action) {
      Debug.Assert(action != null);
      await _mutex.LockAsync();
      try {
        return await action.Invoke();
      } finally {
        _mutex.Unlock(runNextSynchronously: false);
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

      public ref UserData UserData => ref _buf.UserData;
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
        return _buf.UnlockAsync();
      }
    }

    sealed class Chunk {
      readonly BufferedWriter _writer;
      UserData _userData;

      public Chunk(BufferedWriter writer) {
        _writer = writer;
        CloseAtSize = writer._opt.CloseChunk?.Size;
        CloseAtAge = writer._opt.CloseChunk?.Age;
        Stream.SetLength(0);
      }

      public event Action OnClose;

      public ArraySegment<byte>? CompressedContent { get; internal set; }
      public MemoryStream Stream => _writer._strm;
      public DateTime CreatedAt { get; } = DateTime.UtcNow;
      public ref UserData UserData => ref _userData;
      public object UserState { get; set; }
      public long? CloseAtSize { get; set; }
      public TimeSpan? CloseAtAge { get; }
      public bool IsAbandoned { get; internal set; }

      public void Abandon() { IsAbandoned = true; }
      public Task UnlockAsync() => _writer.Unlock();

      public bool Close(CompressionLevel lvl) {
        if (IsAbandoned) return false;
        if (CompressedContent.HasValue) return true;
        OnClose?.Invoke();
        if (IsAbandoned) return false;
        CompressedContent = Compression.Compress(Stream.GetBuffer(), 0, (int)Stream.Length, lvl);
        return true;
      }
    }
  }

  sealed class Timer {
    readonly AsyncMutex _mutex;
    readonly Func<Task> _action;
    readonly TimeSpan? _retry;
    CancellationTokenSource _cancel = null;
    DateTime? _time = null;
    Task _task = null;

    public Timer(AsyncMutex mutex, Func<Task> action, TimeSpan? retry) {
      Debug.Assert(mutex != null);
      Debug.Assert(action != null);
      Debug.Assert(retry == null || retry >= TimeSpan.Zero);
      _mutex = mutex;
      _action = action;
      _retry = retry;
    }

    // Requires: The mutex is locked.
    public void Stop() {
      Debug.Assert(_mutex.IsLocked);
      Debug.Assert((_task == null) == (_cancel == null) && (_task == null) == (_time == null));
      if (_task != null) {
        _cancel.Cancel();
        _cancel = null;
        _task = null;
        _cancel = null;
        _time = null;
      }
    }

    // Requires: The mutex is locked.
    //
    // The returned task completes when the action is scheduled, which happens almost immediately.
    public void ScheduleAt(DateTime? t) {
      Debug.Assert(_mutex.IsLocked);
      Stop();
      if (t.HasValue) {
        Debug.Assert(_retry.HasValue);
        _time = t;
        _cancel = new CancellationTokenSource();
        _task = Run(t.Value, _cancel);
      }
    }

    async Task Run(DateTime t, CancellationTokenSource cancel) {
      await Task.Yield();
      try {
        await Delay(t, cancel.Token);
        await _mutex.LockAsync(cancel.Token);
      } finally {
        cancel.Dispose();
      }
      _task = null;
      _cancel = null;
      _time = null;
      try {
        await _action.Invoke();
      } catch {
        if (_task == null) {
          DateTime? retry = t + _retry.Value;
          DateTime now = DateTime.UtcNow;
          if (now > retry) retry = now;
          ScheduleAt(retry);
        }
      } finally {
        _mutex.Unlock(runNextSynchronously: true);
      }
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
