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
      using (var reader = new EmptyReader(fname)) {
        var rng = new Random();
        long seeks = 0;
        DateTime start = DateTime.UtcNow;
        do {
          ++seeks;
          var t = new DateTime(rng.Next((int)maxTicks + 1), DateTimeKind.Utc);
          await reader.ReadAfter(t).GetAsyncEnumerator().MoveNextAsync(CancellationToken.None);
        } while (DateTime.UtcNow < start + TimeSpan.FromSeconds(seconds));
        seconds = (DateTime.UtcNow - start).TotalSeconds;
        Console.WriteLine("  SeekMany: {0:N0} seeks, {1:N1} seeks/sec.", seeks, seeks / seconds);
      }
    }

    // Sample run on Intel Core i9-7900X with M.2 SSD:
    //
    //   ===[ Chunk Size 0KiB ]===
    //     WriteMany: 291,840 records, 11,676,464 bytes, 9,723 records/sec, 389,035 bytes/sec.
    //     ReadAll: 291,840 records, 11,676,464 bytes, 12,700 records/sec, 508,143 bytes/sec.
    //     SeekMany: 18 seeks, 1.7 seeks/sec.
    //   ===[ Chunk Size 32KiB ]===
    //     WriteMany: 20,182,784 records, 30,858,569 bytes, 672,736 records/sec, 1,028,583 bytes/sec.
    //     ReadAll: 20,182,784 records, 30,858,569 bytes, 7,696,455 records/sec, 11,767,533 bytes/sec.
    //     SeekMany: 1,232 seeks, 123.1 seeks/sec.
    //   ===[ Chunk Size 64KiB ]===
    //     WriteMany: 17,516,800 records, 26,642,917 bytes, 583,680 records/sec, 887,773 bytes/sec.
    //     ReadAll: 17,516,800 records, 26,642,917 bytes, 7,972,044 records/sec, 12,125,418 bytes/sec.
    //     SeekMany: 1,821 seeks, 182.1 seeks/sec.
    //   ===[ Chunk Size 128KiB ]===
    //     WriteMany: 24,147,712 records, 36,644,234 bytes, 804,683 records/sec, 1,221,109 bytes/sec.
    //     ReadAll: 24,147,712 records, 36,644,234 bytes, 5,146,398 records/sec, 7,809,676 bytes/sec.
    //     SeekMany: 1,938 seeks, 193.7 seeks/sec.
    static async Task RunBenchmarks() {
      foreach (int chunkSizeKiB in new[] { 0, 32, 64, 128 }) {
        Console.WriteLine("===[ Chunk Size {0}KiB ]===", chunkSizeKiB);
        string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
          long written = await WriteMany(fname, chunkSizeBytes: chunkSizeKiB << 10, seconds: 30);
          long read = await ReadAll(fname);
          if (written != read) throw new Exception($"Written {written} but read back {read}");
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
