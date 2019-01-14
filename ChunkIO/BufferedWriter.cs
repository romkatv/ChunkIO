using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  // Append-only. Neither readable nor seakable.
  //
  // It's illegal to call any method of OutputBuffer when it isn't locked. Locked instances are returned
  // by BufferedWriter.NewBuffer() and BufferedWriter.GetBuffer(). They can be unlocked with
  // OutputBuffer.Dispose().
  //
  // Writes to the buffer never block on IO. All data is stored in memory until the buffer is closed.
  // Flush() and Close() don't do anything. Dispose() unlocks the buffer.
  abstract class OutputBuffer : Stream {
    public DateTime CreatedAt { get; }

    public UserData UserData { get; set; }
    public object UserState { get; set; }

    // How many bytes have been written to the buffer via its Stream methods.
    // The meaning of BytesWritten is the same as Length, except that Length isn't available in
    // OutputBuffer because it's not seakable.
    public long BytesWritten { get; }

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

  class BufferedWriter : IDisposable {
    public BufferedWriter(string fname) : this(fname, new BufferedWriterOptions()) { }

    public BufferedWriter(string fname, BufferedWriterOptions opt) {
      throw new NotImplementedException();
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
  }
}
