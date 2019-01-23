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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  // Encoder for timestamped records.
  //
  // Methods can be called from arbitrary threads but never concurrently.
  public interface ITimeSeriesEncoder<T> : IDisposable {
    // Writes the first value to an empty chunk and returns its timestamp.
    // If the timestamp is not in UTC, it is converted to UTC. This timestamp
    // is used by TimeSeriesReader.ReadAfter() to figure out which chunks
    // to skip.
    DateTime EncodePrimary(Stream strm, T val);

    // Writes a non-first value to a non-empty chunk.
    void EncodeSecondary(Stream strm, T val);
  }

  // Decoder for timestamped records.
  //
  // Methods can be called from arbitrary threads but never concurrently.
  public interface ITimeSeriesDecoder<T> : IDisposable {
    // Decodes the first value of the chunk. This value was encoded EncodePrimary() and
    // the timestmap was also produced by it (except that here it is always in UTC).
    void DecodePrimary(Stream strm, DateTime t, out T val);

    // Decodes a non-first value of the chunk. It was encoded by EncodeSecondary().
    bool DecodeSecondary(Stream strm, out T val);
  }

  // Data from a signle chunk. Not empty. The first element is "primary", the rest are "secondary".
  public interface IDecodedChunk<out T> : IEnumerable<T> {
    // Starting file position of the chunk.
    long BeginPosition { get; }
    // Past-the-end file position of the chunk.
    long EndPosition { get; }
  }

  // Writer for timestamped records.
  public class TimeSeriesWriter<T> : IDisposable {
    readonly BufferedWriter _writer;
    long _chunkRecords = 0;

    // The file must be exclusively writable. If it exists, it gets appended to.
    // You can use TimeSeriesReader to read the file. You can even do it while
    // having an active writer.
    //
    // Takes ownership of the encoder. TimeSeriesWriter.Dispose() will dispose it.
    public TimeSeriesWriter(string fname, ITimeSeriesEncoder<T> encoder, WriterOptions opt) {
      Encoder = encoder ?? throw new ArgumentNullException(nameof(encoder));
      try {
        _writer = new BufferedWriter(fname, opt);
      } catch {
        Encoder.Dispose();
        throw;
      }
    }

    public TimeSeriesWriter(string fname, ITimeSeriesEncoder<T> encoder)
        : this(fname, encoder, new WriterOptions()) { }

    public string Name => _writer.Name;

    // Methods of the encoder can be called from arbitrary threads between the calll to Write() and
    // the completion of the task it returns.
    public ITimeSeriesEncoder<T> Encoder { get; }

    // The number of records that have been written through this instance of TimeSeriesWriter.
    // Some records may not yet have been flushed to disk or even to OS. However, it's guaranteed
    // that should FlushAsync() succeed, the specified number of records will definitely have
    // appeared in the file.
    //
    // RecordsWritten can change only during Write(). It can either go up by one, or down by any
    // amount.
    //
    // It's illegal to read RecordsWritten concurrently with Write() or with the task it returns.
    //
    // This field can be used to figure out whether the same record should be passed to Write() again
    // after an exception.
    //
    //   async Task WriteReliably(TimeSeriesWriter<T> writer, T record) {
    //     while (true) {
    //       long written = writer.RecordsWritten;
    //       try {
    //         await writer.Write(record);
    //         return;
    //       } catch {
    //         if (writer.RecordsWritten > written) {
    //           // Record has been added to the chunk but the chunk couldn't be written to file.
    //           // The most common reason is full disk. We shouldn't attempt to write
    //           // the same record again. To persist it, we will call FlushAsync(). If we don't do
    //           // that, the next call to Write() will automatically call FlushAsync() and will
    //           // throw on error without writing the new record to the chunk.
    //           break;
    //         } else {
    //           Console.Error.WriteLine("Failed to write record. Will retry in 5 seconds.");
    //           await Task.Delay(TimeSpan.FromSeconds(5));
    //         }
    //       }
    //     }
    //
    //     while (true) {
    //       try {
    //         await writer.FlushAsync(flushToDisk: false);
    //         return;
    //       } catch {
    //         Console.Error.WriteLine("Failed to flush. Will retry in 5 seconds.");
    //         await Task.Delay(TimeSpan.FromSeconds(5));
    //       }
    //     }
    //   }
    public long RecordsWritten { get; internal set; }

    // Throws on IO errors. Can block on IO.
    //
    // It's illegal to call Write() before the task returned by the previous call to Write() has completed.
    public async Task Write(T val) {
      IOutputChunk buf = await _writer.LockChunk();
      try {
        if (buf.IsNew) {
          _chunkRecords = 0;
          buf.UserData = new UserData() { Long0 = Encoder.EncodePrimary(buf.Stream, val).ToUniversalTime().Ticks };
          // If the block is set up to automatically close after a certain number of bytes is
          // written, tell it to exclude snapshot bytes from the calculation. This is necessary
          // to avoid creating a new block on every call to Write() if snapshots happen to
          // be large.
          if (buf.CloseAtSize.HasValue) buf.CloseAtSize += buf.Stream.Length;
        } else {
          Encoder.EncodeSecondary(buf.Stream, val);
        }
        ++_chunkRecords;
        ++RecordsWritten;
      } catch {
        RecordsWritten -= _chunkRecords;
        _chunkRecords = 0;
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

    public string Name => _reader.Name;
    public long Length => _reader.Length;

    // Methods of the decoder can be called from arbitrary threads between the calll to
    // IAsyncEnumerator<IDecodedChunk<T>>.MoveNextAsync() and the completion of the task it returns.
    // The enumerator is from ReadAfter().GetAsyncEnumerator().
    public ITimeSeriesDecoder<T> Decoder { get; }

    // If there a writer writing to our file and it's running on the same machine, tells it to flush,
    // waits for completion and returns the size of the file immediately after flushing. Otherwise returns
    // the current file size.
    //
    // Throws if the writer is unable to flush (e.g., disk full).
    //
    // This method can be called concurrently with any other method and with itself.
    //
    // The implication is that all existing chunks with starting positions in
    // [0, FlushRemoteWriterAsync(flushToDisk).Result) are guaranteed to be final and no new chunks will
    // appear there.
    public Task<long> FlushRemoteWriterAsync(bool flushToDisk) => _reader.FlushRemoteWriterAsync(flushToDisk);

    // Reads timestamped records from the file and returns them one chunk at a time. Each IDecodedChunk<T>
    // corresponds to a single chunk, the first element being "primary" and the rest "secondary".
    //
    // Chunks whose successor's primary record's timestamp is not greater than t are not read.
    // Thus, ReadAfter(DateTime.MinValue) reads all data while ReadAfter(DateTime.MaxValue) reads
    // just the last chunk.
    //
    // In addition, chunks whose start file position is outside of [from, to) are also ignored.
    // There are no constraints on the values of the boundaries. Even long.MinValue and long.MaxValue
    // are legal. If from >= to, the result is empty.
    //
    // The caller doesn't have to iterate over all chunks (that is, over the whole IAsyncEnumerable) or
    // over all records in a chunk (over IDecodedChunk<T>). It's OK to stop iterating half-way.
    public IAsyncEnumerable<IDecodedChunk<T>> ReadAfter(DateTime t, long from, long to) =>
        new Chunks(this, t, from, to);

    public IAsyncEnumerable<IDecodedChunk<T>> ReadAfter(DateTime t) => ReadAfter(t, 0, long.MaxValue);

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

    class TimeSeriesChunk : IDecodedChunk<T> {
      readonly IEnumerable<T> _data;

      public TimeSeriesChunk(InputChunk chunk, ITimeSeriesDecoder<T> decoder) {
        BeginPosition = chunk.BeginPosition;
        EndPosition = chunk.EndPosition;
        _data = Decode();

        IEnumerable<T> Decode() {
          decoder.DecodePrimary(chunk, new DateTime(chunk.UserData.Long0, DateTimeKind.Utc), out T val);
          yield return val;
          while (decoder.DecodeSecondary(chunk, out val)) yield return val;
        }
      }

      public long BeginPosition { get; }
      public long EndPosition { get; }
      public IEnumerator<T> GetEnumerator() => _data.GetEnumerator();
      IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    class ChunkEnumerator : IAsyncEnumerator<IDecodedChunk<T>> {
      readonly TimeSeriesReader<T> _reader;
      readonly DateTime _after;
      readonly long _from;
      readonly long _to;
      InputChunk _chunk = null;
      bool _done = false;
      long _next = 0;

      public ChunkEnumerator(TimeSeriesReader<T> reader, DateTime after, long from, long to) {
        _reader = reader;
        _after = after;
        _from = from;
        _to = to;
      }

      public IDecodedChunk<T> Current { get; internal set; }

      public async Task<bool> MoveNextAsync(CancellationToken cancel) {
        if (_done) return false;

        if (_chunk == null) {
          _chunk = await _reader._reader.ReadAtPartitionAsync(_from, _to, u => new DateTime(u.Long0) > _after);
        } else {
          _chunk.Dispose();
          _chunk = await _reader._reader.ReadNextAsync(_next, _to);
        }

        if (_chunk == null) {
          _done = true;
          return false;
        } else {
          Current = new TimeSeriesChunk(_chunk, _reader.Decoder);
          _next = _chunk.EndPosition;
          return true;
        }
      }

      public void Reset() {
        _chunk?.Dispose();
        _chunk = null;
        _next = 0;
        _done = false;
      }

      public void Dispose() => Reset();
    }

    class Chunks : IAsyncEnumerable<IDecodedChunk<T>> {
      readonly TimeSeriesReader<T> _reader;
      readonly DateTime _after;
      readonly long _from;
      readonly long _to;

      public Chunks(TimeSeriesReader<T> reader, DateTime after, long from, long to) {
        _reader = reader;
        _after = after;
        _from = from;
        _to = to;
      }

      public IAsyncEnumerator<IDecodedChunk<T>> GetAsyncEnumerator() =>
          new ChunkEnumerator(_reader, _after, _from, _to);
    }
  }
}
