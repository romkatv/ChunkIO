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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO.Benchmark {
  struct Empty { }

  class EmptyEncoder : EventEncoder<Empty> {
    protected override void Encode(BinaryWriter writer, Empty rec, bool isPrimary) { }
  }

  class EmptyDecoder : EventDecoder<Empty> {
    protected override Empty Decode(BinaryReader reader, bool isPrimary) => new Empty();
  }

  class EmptyWriter : TimeSeriesWriter<Event<Empty>> {
    public EmptyWriter(string fname) : base(fname, new EmptyEncoder(), Opt()) { }

    static WriterOptions Opt() => new WriterOptions() {
      // Set all time-based triggers because this is what production code normally does.
      // Use large values that won't actually cause any flushes.
      CloseChunk = new Triggers() {
        Size = 32 << 10,
        Age = TimeSpan.FromDays(1),
        AgeRetry = TimeSpan.FromDays(1),
      },
      FlushToOS = new Triggers() {
        Age = TimeSpan.FromDays(1),
        AgeRetry = TimeSpan.FromDays(1),
      },
      FlushToDisk = new Triggers() {
        Age = TimeSpan.FromDays(1),
        AgeRetry = TimeSpan.FromDays(1),
      },
    };
  }

  class EmptyReader : TimeSeriesReader<Event<Empty>> {
    public EmptyReader(string fname) : base(fname, new EmptyDecoder()) { }
  }

  class Benchmark {
    // Writes individual Empty records to the file. Records have timestamps with consecutive ticks starting from 1.
    static async Task Write(string fname, long records) {
      Debug.Assert(records >= 0);
      using (var writer = new EmptyWriter(fname)) {
        Stopwatch stopwatch = Stopwatch.StartNew();
        for (long i = 0; i != records; ++i) {
          await writer.Write(new Event<Empty>(new DateTime(i + 1, DateTimeKind.Utc), new Empty()));
        }
        await writer.FlushAsync(flushToDisk: false);
        double seconds = stopwatch.Elapsed.TotalSeconds;
        long bytes = new FileInfo(fname).Length;
        Console.WriteLine("  Write(records: {0:N0}): {1:N0} bytes, {2:N0} records/sec, {3:N0} bytes/sec.",
                          records, bytes, records / seconds, bytes / seconds);
      }
    }

    // Writes batches of Empty records to the file. Records have timestamps with consecutive ticks starting from 1.
    static async Task WriteBatch(string fname, long batches, long recsPerBatch) {
      Debug.Assert(batches >= 0);
      Debug.Assert(recsPerBatch > 0);
      using (var writer = new EmptyWriter(fname)) {
        Stopwatch stopwatch = Stopwatch.StartNew();
        for (long i = 0; i != batches; ++i) {
          await writer.WriteBatch(Batch());
          IEnumerable<Event<Empty>> Batch() {
            for (long j = 0; j != recsPerBatch; ++j) {
              yield return new Event<Empty>(new DateTime(i * recsPerBatch + j + 1, DateTimeKind.Utc), new Empty());
            }
          }
        }
        await writer.FlushAsync(flushToDisk: false);
        double seconds = stopwatch.Elapsed.TotalSeconds;
        long bytes = new FileInfo(fname).Length;
        long records = batches * recsPerBatch;
        Console.WriteLine(
            "  WriteBatch(batches: {0:N0}, recsPerBatch: {1:N0}): " +
                "{2:N0} records, {3:N0} bytes, {4:N0} records/sec, {5:N0} bytes/sec.",
            batches, recsPerBatch, records, bytes, records / seconds, bytes / seconds);
      }
    }

    // Reads all Empty records from the specified file. Returns the number of records read.
    static async Task<long> ReadAll(string fname) {
      using (var reader = new EmptyReader(fname)) {
        long records = 0;
        Stopwatch stopwatch = Stopwatch.StartNew();
        await reader.ReadAfter(DateTime.MinValue).ForEachAsync((IEnumerable<Event<Empty>> buf) => {
          records += buf.Count();
          return Task.CompletedTask;
        });
        double seconds = stopwatch.Elapsed.TotalSeconds;
        long bytes = new FileInfo(fname).Length;
        Console.WriteLine("  ReadAll: {0:N0} records, {1:N0} bytes, {2:N0} records/sec, {3:N0} bytes/sec.",
                          records, bytes, records / seconds, bytes / seconds);
        return records;
      }
    }

    // Seeks to random timestamps in the file for the specified amount of time. The timestamps
    // to seek are uniformly distributed in [0, maxTicks].
    static async Task SeekMany(string fname, long maxTicks, double seconds) {
      if (maxTicks >= int.MaxValue) throw new Exception("Sorry, not implemented");
        var rng = new Random();
        long seeks = 0;
      Stopwatch stopwatch = Stopwatch.StartNew();
      do {
          ++seeks;
          var t = new DateTime(rng.Next((int)maxTicks + 1), DateTimeKind.Utc);
          // Create a new reader for every seek to avoid the possibility of caching in the reader.
          using (var reader = new EmptyReader(fname)) {
            // Note that this not only seeks but also reads and decompresses the content of the
            // first chunk.
            await reader.ReadAfter(t).GetAsyncEnumerator().MoveNextAsync(CancellationToken.None);
          }
        } while (stopwatch.Elapsed < TimeSpan.FromSeconds(seconds));
        seconds = stopwatch.Elapsed.TotalSeconds;
        Console.WriteLine("  SeekMany: {0:N0} seeks, {1:N1} seeks/sec.", seeks, seeks / seconds);
    }

    static async Task WithFile(Func<string, Task> action) {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        await action.Invoke(fname);
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
      }
    }

    // Sample run on AWS c4.xlarge, Windows Server 2016, .NET Framework 4.7.2:
    //
    //   Write(records: 8,390,656): 12,828,714 bytes, 612,319 records/sec, 936,192 bytes/sec.
    //   WriteBatch(batches: 34,816, recsPerBatch: 241): 8,390,656 records, 12,828,714 bytes, 864,088 records/sec, 1,321,128 bytes/sec.
    //   ReadAll: 8,390,656 records, 12,828,714 bytes, 7,258,004 records/sec, 11,096,971 bytes/sec.
    //   SeekMany: 1,392 seeks, 278.3 seeks/sec.
    static async Task RunBenchmarks() {
      await Run("Warmup", chunks: 16, seconds: 0.1);
      await Run("Benchmark", chunks: 2 << 10, seconds: 5);

      async Task Run(string label, long chunks, double seconds) {
        Console.WriteLine("{0}:", label);
        // The number 241 is chosen to ensure that WriteBatch() and Write() generate identical files
        // with the same number of chunks. Our chunks are auto-closed when they are at least 32KiB, which
        // means we can fit (32 << 10) / 8 + 1 = 4097 records in a chunk (+1 because the first record is
        // encoded in user data). 4097 is 17 * 241.
        const long RecsPerBatch = 241;
        const long BatchesPerChunk = 17;
        long batches = chunks * BatchesPerChunk;
        long records = batches * RecsPerBatch;
        await WithFile((string fname) => Write(fname, records));
        await WithFile(async (string fname) => {
          await WriteBatch(fname, batches, RecsPerBatch);
          long read = await ReadAll(fname);
          if (read != records) throw new Exception($"Written {records} but read back {read}");
          await SeekMany(fname, records, seconds);
        });
      }
    } 

    static int Main(string[] args) {
      try {
        RunBenchmarks().Wait();
      } catch (Exception e) {
        Console.Error.WriteLine("Error: {0}", e);
        return 1;
      }
      return 0;
    }
  }
}
