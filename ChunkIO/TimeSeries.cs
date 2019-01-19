using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  // Encoder for timestamped records.
  //
  // Methods can be called from arbitrary threads but never concurrently.
  public interface ITimeSeriesEncoder<T> : IDisposable {
    // Writes the first value to an empty buffer and returns its timestamp.
    // If the timestamp is not in UTC, it is converted to UTC. This timestamp
    // is used by TimeSeriesReader.ReadAfter() to figure out which buffers
    // to skip.
    DateTime EncodePrimary(Stream strm, T val);

    // Writes a non-first value to a non-empty buffer.
    void EncodeSecondary(Stream strm, T val);
  }

  // Decoder for timestamped records.
  //
  // Methods can be called from arbitrary threads but never concurrently.
  public interface ITimeSeriesDecoder<T> : IDisposable {
    // Decodes the first value of the buffer. This value was encoded EncodePrimary() and
    // the timestmap was also produced by it (except that here it is always in UTC).
    void DecodePrimary(Stream strm, DateTime t, out T val);

    // Decodes a non-first value of the buffer. It was encoded by EncodeSecondary().
    bool DecodeSecondary(Stream strm, out T val);
  }

  // Writer for timestamped records.
  public class TimeSeriesWriter<T> : IDisposable {
    readonly BufferedWriter _writer;

    // The file must be exclusively writable. If it exists, it gets appended to.
    // You can use TimeSeriesReader to read the file. You can even do it while
    // having an active writer.
    //
    // Takes ownership of the encoder. TimeSeriesWriter.Dispose() will dispose it.
    public TimeSeriesWriter(string fname, ITimeSeriesEncoder<T> encoder) {
      if (fname == null) throw new ArgumentNullException(nameof(fname));
      if (encoder == null) throw new ArgumentNullException(nameof(encoder));

      // Guarantees:
      //
      //   * If the writer process terminates unexpectedly, we'll lose at most 1h+5m worth of data.
      //   * If the OS terminates unexpectedly, we'll lose at most 3h+1h worth of data.
      //
      // We flush data more often than necessary with a different factor for every file. This is done
      // to avoid flushing a large number of files at the same time periodically.
      //
      // It's possible to make these values configurable if this becomes necessary in the future.
      var rand = new Random((int)SipHash.ComputeHash(System.Text.Encoding.UTF8.GetBytes(fname)));
      var opt = new BufferedWriterOptions();
      opt.CloseBuffer.Size = 64 << 10;
      // Auto-close buffers older than 1h. This timer starts when a buffer is created.
      opt.CloseBuffer.Age = Jitter(TimeSpan.FromHours(1));
      // Flush all closed buffers older than 5m to OS. This timer when a buffer is closed.
      opt.FlushToOS.Age = Jitter(TimeSpan.FromMinutes(5));
      // Flush all closed buffers older than 3h to disk. This timer when a buffer is closed.
      opt.FlushToDisk.Age = Jitter(TimeSpan.FromHours(3));
      _writer = new BufferedWriter(fname, opt);
      Encoder = encoder;

      // Returns t multiplied by a random number in [0.5, 1).
      TimeSpan Jitter(TimeSpan t) => TimeSpan.FromTicks((long)((0.5 * rand.NextDouble() + 0.5) * t.Ticks));
    }

    // Methods of the encoder can be called from arbitrary threads between the calll to Write() and
    // the completion of the task it returns.
    public ITimeSeriesEncoder<T> Encoder { get; }

    // Throws on IO errors. Can block on IO.
    //
    // It's illegal to call Write() before the task returned by the previous call to Write() has completed.
    public async Task Write(T val) {
      IOutputBuffer buf = await _writer.LockBuffer();
      try {
        if (buf.IsNew) {
          buf.UserData = new UserData() { Long0 = Encoder.EncodePrimary(buf.Stream, val).ToUniversalTime().Ticks };
          // If the block is set up to automatically close after a certain number of bytes is
          // written, tell it to exclude snapshot bytes from the calculation. This is necessary
          // to avoid creating a new block on every call to Write() if snapshots happen to
          // be large.
          if (buf.CloseAtSize.HasValue) buf.CloseAtSize += buf.Stream.Length;
        } else {
          Encoder.EncodeSecondary(buf.Stream, val);
        }
      } catch {
        buf.Abandon();
        throw;
      } finally {
        await buf.DisposeAsync();
      }
    }

    // It's legal to call FlushAsync() from any thread and concurrently with Write() or another FlushAsync().
    // However, if FlushAsync() is called while there is an inflight write, it won't complete until the write
    // completes.
    public Task FlushAsync(bool flushToDisk) => _writer.FlushAsync(flushToDisk);

    // Can block on IO and throw on IO errors. Will do neither of these if you call FlushAsync() beforehand and
    // wait for its successful completion.
    public void Dispose() => Dispose(true);

    // Inheriting from TimeSeriesWriter?
    // See https://docs.microsoft.com/en-us/dotnet/standard/garbage-collection/implementing-dispose.
    protected virtual void Dispose(bool disposing) {
      if (disposing) {
        try {
          Encoder.Dispose();
        } finally {
          _writer.Dispose();
        }
      }
    }
  }

  // Reader for timestamped records.
  public class TimeSeriesReader<T> : IDisposable {
    readonly BufferedReader _reader;

    // The file must exist and be readable. There is no explicit file format check. If the file
    // wasn't created by ChunkWriter, all its content will be simply skipped without any errors.
    //
    // You can read the file even while there is an active TimeSeriesWriter writing to it. You
    // might want to use FlushRemoteWriterAsync() in this case.
    //
    // Takes ownership of the decoder. TimeSeriesReader.Dispose() will dispose it.
    public TimeSeriesReader(string fname, ITimeSeriesDecoder<T> decoder) {
      if (decoder == null) throw new ArgumentNullException(nameof(decoder));
      _reader = new BufferedReader(fname);
      Decoder = decoder;
    }

    // Methods of the decoder can be called from arbitrary threads between the calll to
    // IAsyncEnumerator<IEnumerable<T>>.MoveNextAsync() and the completion of the task it returns.
    // The enumerator is from ReadAfter().GetAsyncEnumerator().
    public ITimeSeriesDecoder<T> Decoder { get; }

    // If there a writer writing to our file and it's running on the same machine, tells it to flush,
    // waits for completion and returns true. Otherwise returns false.
    //
    // Throws if the writer is unable to flush (e.g., disk full).
    //
    // This method can be called concurrently with any other method and with itself.
    public Task<bool> FlushRemoteWriterAsync(bool flushToDisk) => _reader.FlushRemoteWriterAsync(flushToDisk);

    // Reads timestamped records from the file and returns them one buffer at a time. Each IEnumerable<T>
    // corresponds to a single buffer, the first element being "primary" and the rest "secondary".
    //
    // Buffers whose successor's primary record's timestamp is not greater than t are not read.
    // Thus, ReadAfter(DateTime.MinValue) reads all data.
    public IAsyncEnumerable<IEnumerable<T>> ReadAfter(DateTime t) {
      return new AsyncEnumerable<IEnumerable<T>>(async yield => {
        InputBuffer buf = await _reader.ReadAtPartitionAsync((UserData u) => new DateTime(u.Long0) > t);
        while (buf != null) {
          try {
            await yield.ReturnAsync(DecodeBuffer(buf, Decoder));
          } finally {
            buf.Dispose();
          }
          buf = await _reader.ReadNextAsync();
        }
      });
    }

    public void Dispose() => Dispose(true);

    // Inheriting from TimeSeriesReader?
    // See https://docs.microsoft.com/en-us/dotnet/standard/garbage-collection/implementing-dispose.
    protected virtual void Dispose(bool disposing) {
      if (disposing) {
        try {
          Decoder.Dispose();
        } finally {
          _reader.Dispose();
        }
      }
    }

    static IEnumerable<T> DecodeBuffer(InputBuffer buf, ITimeSeriesDecoder<T> decoder) {
      decoder.DecodePrimary(buf, new DateTime(buf.UserData.Long0, DateTimeKind.Utc), out T val);
      yield return val;
      while (decoder.DecodeSecondary(buf, out val)) yield return val;
    }
  }
}
