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
    //     WriteMany: 367,104 records, 14,687,760 bytes, 12,235 records/sec, 489,515 bytes/sec.
    //     ReadAll: 367,104 records, 14,687,760 bytes, 19,749 records/sec, 790,162 bytes/sec.
    //     SeekMany: 20 seeks, 2.0 seeks/sec.
    //   ===[ Chunk Size 32KiB ]===
    //     WriteMany: 21,974,016 records, 33,597,499 bytes, 732,351 records/sec, 1,119,739 bytes/sec.
    //     ReadAll: 21,974,016 records, 33,597,499 bytes, 3,507,819 records/sec, 5,363,332 bytes/sec.
    //     SeekMany: 1,226 seeks, 122.5 seeks/sec.
    //   ===[ Chunk Size 64KiB ]===
    //     WriteMany: 17,809,152 records, 27,087,560 bytes, 593,543 records/sec, 902,774 bytes/sec.
    //     ReadAll: 17,809,152 records, 27,087,560 bytes, 4,029,928 records/sec, 6,129,485 bytes/sec.
    //     SeekMany: 1,965 seeks, 196.4 seeks/sec.
    //   ===[ Chunk Size 128KiB ]===
    //     WriteMany: 23,022,848 records, 34,937,240 bytes, 767,179 records/sec, 1,164,197 bytes/sec.
    //     ReadAll: 23,022,848 records, 34,937,240 bytes, 3,844,840 records/sec, 5,834,556 bytes/sec.
    //     SeekMany: 1,681 seeks, 168.0 seeks/sec.
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
