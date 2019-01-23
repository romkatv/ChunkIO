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
    public EmptyWriter(string fname, int chunkSizeBytes) : base(fname, new EmptyEncoder(), Opt(chunkSizeBytes)) { }

    static WriterOptions Opt(int chunkSizeBytes) => new WriterOptions() {
      // Set all time-based triggers because this is what production code normally does.
      // Use large values that won't actually cause any flushes.
      CloseChunk = new Triggers() {
        Size = chunkSizeBytes,
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
    // Writes Empty records to the specified file for the specified amount of time.
    // Returns the number of written records. Records have timestamps with consecutive
    // ticks starting from 1.
    static async Task<long> WriteMany(string fname, int chunkSizeBytes, double seconds) {
      using (var writer = new EmptyWriter(fname, chunkSizeBytes)) {
        long records = 0;
        DateTime start = DateTime.UtcNow;
        do {
          for (int i = 0; i != 256; ++i) {
            ++records;
            await writer.Write(new Event<Empty>(new DateTime(records, DateTimeKind.Utc), new Empty()));
          }
        } while (DateTime.UtcNow < start + TimeSpan.FromSeconds(seconds));
        await writer.FlushAsync(flushToDisk: false);
        seconds = (DateTime.UtcNow - start).TotalSeconds;
        long bytes = new FileInfo(fname).Length;
        Console.WriteLine("  WriteMany: {0:N0} records, {1:N0} bytes, {2:N0} records/sec, {3:N0} bytes/sec.",
                          records, bytes, records / seconds, bytes / seconds);
        return records;
      }
    }

    // Reads all Empty records from the specified file. Returns the number of records read.
    static async Task<long> ReadAll(string fname) {
      using (var reader = new EmptyReader(fname)) {
        long records = 0;
        DateTime start = DateTime.UtcNow;
        await reader.ReadAfter(DateTime.MinValue).ForEachAsync((IEnumerable<Event<Empty>> buf) => {
          records += buf.Count();
          return Task.CompletedTask;
        });
        double seconds = (DateTime.UtcNow - start).TotalSeconds;
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
        DateTime start = DateTime.UtcNow;
        do {
          ++seeks;
          var t = new DateTime(rng.Next((int)maxTicks + 1), DateTimeKind.Utc);
          using (var reader = new EmptyReader(fname)) {
            await reader.ReadAfter(t).GetAsyncEnumerator().MoveNextAsync(CancellationToken.None);
          }
        } while (DateTime.UtcNow < start + TimeSpan.FromSeconds(seconds));
        seconds = (DateTime.UtcNow - start).TotalSeconds;
        Console.WriteLine("  SeekMany: {0:N0} seeks, {1:N1} seeks/sec.", seeks, seeks / seconds);
    }

    // Sample run on Intel Core i9-7900X with M.2 SSD:
    //
    //   ===[ Chunk Size 0KiB ]===
    //     WriteMany: 450,816 records, 18,037,056 bytes, 15,023 records/sec, 601,048 bytes/sec.
    //     ReadAll: 450,816 records, 18,037,056 bytes, 22,139 records/sec, 885,785 bytes/sec.
    //     SeekMany: 60 seeks, 6.0 seeks/sec.
    //   ===[ Chunk Size 32KiB ]===
    //     WriteMany: 24,065,792 records, 36,795,999 bytes, 802,140 records/sec, 1,226,452 bytes/sec.
    //     ReadAll: 24,065,792 records, 36,795,999 bytes, 7,616,344 records/sec, 11,645,201 bytes/sec.
    //     SeekMany: 4,573 seeks, 457.2 seeks/sec.
    //   ===[ Chunk Size 64KiB ]===
    //     WriteMany: 19,598,848 records, 29,809,972 bytes, 653,238 records/sec, 993,579 bytes/sec.
    //     ReadAll: 19,598,848 records, 29,809,972 bytes, 7,982,399 records/sec, 12,141,279 bytes/sec.
    //     SeekMany: 4,539 seeks, 453.9 seeks/sec.
    //   ===[ Chunk Size 128KiB ]===
    //     WriteMany: 25,313,792 records, 38,413,841 bytes, 843,494 records/sec, 1,280,008 bytes/sec.
    //     ReadAll: 25,313,792 records, 38,413,841 bytes, 10,255,839 records/sec, 15,563,301 bytes/sec.
    //     SeekMany: 4,006 seeks, 400.6 seeks/sec.
    //   ===[ Chunk Size 4096KiB ]===
    //     WriteMany: 28,659,456 records, 43,398,456 bytes, 948,322 records/sec, 1,436,025 bytes/sec.
    //     ReadAll: 28,659,456 records, 43,398,456 bytes, 11,226,253 records/sec, 16,999,697 bytes/sec.
    //     SeekMany: 407 seeks, 40.6 seeks/sec.
    static async Task RunBenchmarks() {
      foreach (int chunkSizeKiB in new[] { 0, 32, 64, 128, 4 << 10 }) {
        Console.WriteLine("===[ Chunk Size {0}KiB ]===", chunkSizeKiB);
        string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
          long written = await WriteMany(fname, chunkSizeBytes: chunkSizeKiB << 10, seconds: 30);
          long read = await ReadAll(fname);
          if (written != read) throw new Exception($"Written {written} but read back {read}");
          // Note that performance of SeekMany() depends on the file size. The latter, in turn, depends
          // on the performance of WriteMany(). If you want to compare the performance of two
          // implementations of seek, you need to bechmark both against the same file.
          await SeekMany(fname, written, seconds: 10);
        } finally {
          if (File.Exists(fname)) File.Delete(fname);
        }
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
