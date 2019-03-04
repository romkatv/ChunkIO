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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  // Encoder for timestamped records.
  //
  // Methods can be called from arbitrary threads but never concurrently.
  public interface ITimeSeriesEncoder<T> {
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
  public interface ITimeSeriesDecoder<T> {
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

  public enum TimeSeriesWriteResult {
    // Record(s) successfully added to the buffer (current chunk). There is no point in trying to
    // write the same record. Doing so can result in duplicates.
    RecordsBuffered,
    // Record(s) dropped on the floor. If you need this record persisted, you need to write it again.
    RecordsDropped,
    // Record(s) dropped on the floor, together with all other records that were previously added to
    // the buffer (current chunk). This happens when ITimeSeriesEncoder throws when attempting to
    // encode the record(s).
    ChunkDropped,
  }

  public class TimeSeriesWriteException : Exception {
    public TimeSeriesWriteException(Exception inner, TimeSeriesWriteResult result) : base(result.ToString(), inner) {
      Result = result;
    }

    // If Result is RecordsBuffered, it means there was an IO error when flushing data to disk after
    // adding the record to the buffer (current chunk). The record is not lost. The next flush will
    // attempt to write it again.
    public TimeSeriesWriteResult Result { get; }
  }

  // Writer for timestamped records.
  //
  // It's allowed to call all public methods of TimeSeriesWriter concurrently, even Dispose() and DisposeAsync().
  public class TimeSeriesWriter<T> : IDisposable, IAsyncDisposable {
    readonly BufferedWriter _writer;

    // The file must be exclusively writable. If it exists, it gets appended to.
    // You can use TimeSeriesReader to read the file. You can even do it while
    // having an active writer.
    //
    // Takes ownership of the encoder. TimeSeriesWriter.Dispose() will dispose it.
    public TimeSeriesWriter(string fname, ITimeSeriesEncoder<T> encoder, WriterOptions opt) {
      Encoder = encoder ?? throw new ArgumentNullException(nameof(encoder));
      _writer = new BufferedWriter(fname, opt);
    }

    public TimeSeriesWriter(string fname, ITimeSeriesEncoder<T> encoder)
        : this(fname, encoder, new WriterOptions()) { }

    public IReadOnlyCollection<byte> Id => _writer.Id;
    public string Name => _writer.Name;

    // Methods of the encoder can be called from arbitrary threads between the calll to Write() and
    // the completion of the task it returns.
    public ITimeSeriesEncoder<T> Encoder { get; }

    // Writes all records in order into the same chunk. They can either be appended to the current
    // chunk or written into a brand new chunk.
    //
    // Throws on IO errors. Can block on IO.
    //
    // The only exception it throws is TimeSeriesWriteException. You can examine its Result field
    // to figure out what happened to the record(s) you were trying to write. InnerException is the
    // culprit. It's never null.
    public async Task WriteBatchAsync(IEnumerable<T> records) {
      TimeSeriesWriteResult res = TimeSeriesWriteResult.RecordsDropped;
      try {
        if (records == null) throw new ArgumentNullException(nameof(records));
        IOutputChunk buf = await _writer.LockChunkAsync();
        try {
          IEnumerator<T> e = records.GetEnumerator();
          if (!e.MoveNext()) {
            buf.Abandon();
            return;
          }
          if (buf.IsNew) {
            buf.UserData.Long0 = Encoder.EncodePrimary(buf.Stream, e.Current).ToUniversalTime().Ticks;
            // If the block is set up to automatically close after a certain number of bytes is
            // written, tell it to exclude snapshot bytes from the calculation. This is necessary
            // to avoid creating a new block on every call to Write() if snapshots happen to
            // be large.
            if (buf.CloseAtSize.HasValue) buf.CloseAtSize += buf.Stream.Length;
          } else {
            Encoder.EncodeSecondary(buf.Stream, e.Current);
          }
          while (e.MoveNext()) Encoder.EncodeSecondary(buf.Stream, e.Current);
          res = TimeSeriesWriteResult.RecordsBuffered;
        } catch {
          res = TimeSeriesWriteResult.ChunkDropped;
          buf.Abandon();
          throw;
        } finally {
          await buf.DisposeAsync();
        }
      } catch (Exception e) {
        throw new TimeSeriesWriteException(e, res);
      }
    }

    public Task WriteAsync(T val) => WriteBatchAsync(new[] { val });

    public Task FlushAsync(bool flushToDisk) => _writer.FlushAsync(flushToDisk);

    public Task DisposeAsync() => DisposeAsync(true);

    // Inheriting from TimeSeriesWriter?
    // See https://docs.microsoft.com/en-us/dotnet/standard/garbage-collection/implementing-dispose.
    protected async Task DisposeAsync(bool disposing) {
      if (disposing) await _writer.DisposeAsync();
    }

    // Can block on IO and throw on IO errors. Use DisposeAsync() instead if you can.
    public void Dispose() => DisposeAsync().Wait();
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
      Decoder = decoder ?? throw new ArgumentNullException(nameof(decoder));
      _reader = new BufferedReader(fname);
    }

    public IReadOnlyCollection<byte> Id => _reader.Id;
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
      if (disposing) _reader.Dispose();
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
          _chunk = await _reader._reader.ReadFirstAsync(_next, _to);
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

  // Transcoder for files written with TimeSeriesWriter.
  public static class TimeSeriesTranscoder {
    // Reads all chunks from the ChunkIO file named `input`, decodes their content with `decoder`,
    // transforms records with `transform`, encodes them with `encoder` and appends to the ChunkIO
    // file named `output`.
    //
    // The output ChunkIO file is written with default WriterOptions. Some input chunks may get
    // merged but no chunks get split.
    public static async Task TranscodeAsync<T, U>(
        string input,
        string output,
        ITimeSeriesDecoder<T> decoder,
        ITimeSeriesEncoder<U> encoder,
        Func<T, U> transform) {
      using (var reader = new TimeSeriesReader<T>(input, decoder)) {
        var writer = new TimeSeriesWriter<U>(output, encoder);
        try {
          long len = await reader.FlushRemoteWriterAsync(flushToDisk: false);
          await reader.ReadAfter(DateTime.MinValue, 0, len).ForEachAsync(async (IDecodedChunk<T> c) => {
            await writer.WriteBatchAsync(c.Select(transform));
          });
        } finally {
          await writer.DisposeAsync();
        }
      }
    }

    public static Task TranscodeAsync<T>(
        string input,
        string output,
        ITimeSeriesDecoder<T> decoder,
        ITimeSeriesEncoder<T> encoder,
        Func<T, T> transform = null) {
      return TranscodeAsync<T, T>(input, output, decoder, encoder, transform ?? (x => x));
    }
  }
}
