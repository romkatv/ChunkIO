using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  interface ITimeSeriesEncoder<T> : IDisposable {
    DateTime EncodePrimary(Stream strm, T val);
    void EncodeSecondary(Stream strm, T val);
  }

  interface ITimeSeriesDecoder<T> : IDisposable {
    void DecodePrimary(Stream strm, DateTime t, out T val);
    bool DecodeSecondary(Stream strm, out T val);
  }

  class TimeSeriesWriter<T> : IDisposable {
    readonly BufferedWriter _writer;

    public TimeSeriesWriter(string fname, ITimeSeriesEncoder<T> encoder) {
      if (fname == null) throw new ArgumentNullException(nameof(fname));
      if (encoder == null) throw new ArgumentNullException(nameof(encoder));

      // Guarantees:
      //
      //   * If the writer process terminates unexpectedly, we'll lose at most 1h worth of data.
      //   * If the OS terminates unexpectedly, we'll lose at most 3h worth of data.
      //
      // We flush data more often than necessary with a different factor for every file. This is done
      // to avoid flushing a large number of files at the same time periodically.
      var rand = new Random(fname.GetHashCode());
      var opt = new BufferedWriterOptions();
      opt.CloseBuffer.Size = 64 << 10;
      // Auto-close buffers older than 1h.
      opt.CloseBuffer.Age = Jitter(TimeSpan.FromHours(1));
      // As soon as a buffer is closed, flush to the OS.
      opt.FlushToOS.Age = Jitter(TimeSpan.Zero);
      // Flush all closed buffers older than 3h to disk.
      opt.FlushToDisk.Age = Jitter(TimeSpan.FromHours(3));
      _writer = new BufferedWriter(fname, opt);
      Encoder = encoder;

      // Returns t multiplied by a random number in [0.5, 1).
      TimeSpan Jitter(TimeSpan t) => TimeSpan.FromTicks((long)((0.5 * rand.NextDouble() + 0.5) * t.Ticks));
    }

    public ITimeSeriesEncoder<T> Encoder { get; }

    // Doesn't block on IO.
    public async Task Write(T val) {
      bool primary = false;
      IOutputBuffer buf = await _writer.GetBuffer();
      if (buf == null) {
        primary = true;
        buf = await _writer.NewBuffer();
      }
      try {
        if (primary) {
          buf.UserData = new UserData() { Long0 = Encoder.EncodePrimary(buf.Stream, val).ToUniversalTime().Ticks };
          // If the block is set up to automatically close after a certain number of bytes is
          // written, tell it to exclude snapshot bytes from the calculation. This is necessary
          // to avoid creating a new block on every call to Write() if snapshots happen to
          // be large.
          if (buf.CloseAtSize.HasValue) buf.CloseAtSize += buf.BytesWritten;
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

    public Task FlushAsync(bool flushToDisk) => _writer.FlushAsync(flushToDisk);

    // Can block on IO and throw. Won't do either of these if you call FlushAsync() beforehand and
    // wait for its successful completion.
    public void Dispose() => Dispose(true);

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

  class TimeSeriesReader<T> : IDisposable {
    readonly BufferedReader _reader;

    public TimeSeriesReader(string fname, ITimeSeriesDecoder<T> decoder) {
      if (decoder == null) throw new ArgumentNullException(nameof(decoder));
      _reader = new BufferedReader(fname);
      Decoder = decoder;
    }

    public ITimeSeriesDecoder<T> Decoder { get; }

    // If there a writer writing to our file, tell it to flush and wait for completion.
    // Works even if the writer is in another process, but not when it's on another machine.
    //
    // This method can be called concurrently with any other method and with itself.
    public Task FlushRemoteWriterAsync() => _reader.FlushRemoteWriterAsync();

    // Reads time series data from the file and returns it one buffer at a time. Each IEnumerable<T>
    // corresponds to a single buffer, the first element being "primary" and the rest "secondary".
    //
    // Chunks whose successor's primary element timestamp is not greater than t are not read.
    public IAsyncEnumerable<IEnumerable<T>> ReadAllAfter(DateTime t) {
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

    static IEnumerable<T> DecodeBuffer(InputBuffer buf, ITimeSeriesDecoder<T> decoder) {
      decoder.DecodePrimary(buf, new DateTime(buf.UserData.Long0, DateTimeKind.Utc), out T val);
      yield return val;
      while (decoder.DecodeSecondary(buf, out val)) yield return val;
    }

    public void Dispose() => Dispose(true);

    protected virtual void Dispose(bool disposing) {
      if (disposing) {
        try {
          Decoder.Dispose();
        } finally {
          _reader.Dispose();
        }
      }
    }
  }
}
