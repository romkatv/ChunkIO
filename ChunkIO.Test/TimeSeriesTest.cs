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
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ChunkIO.Test {
  public static class EnumerableExtension {
    public static IEnumerable<T> RandomShuffled<T>(this IEnumerable<T> seq, int seed) {
      T[] res = seq.ToArray();
      var rng = new Random(seed);
      int n = res.Length;
      while (n > 1) {
        n--;
        int k = rng.Next(n + 1);
        T value = res[k];
        res[k] = res[n];
        res[n] = value;
      }
      return res;
    }
  }

  [TestClass]
  public class TimeSeriesTest {
    class Encoder : EventEncoder<long> {
      protected override void Encode(BinaryWriter writer, long rec, bool isPrimary) {
        writer.Write(isPrimary);
        writer.Write(rec);
      }
    }

    class Decoder : EventDecoder<long> {
      protected override long Decode(BinaryReader reader, bool isPrimary) {
        Assert.AreEqual(isPrimary, reader.ReadBoolean());
        return reader.ReadInt64();
      }
    }

    class Writer : TimeSeriesWriter<Event<long>> {
      public Writer(string fname) : base(fname, new Encoder(), new WriterOptions() { CloseChunk = null }) { }
    }

    class Reader : TimeSeriesReader<Event<long>> {
      public Reader(string fname) : base(fname, new Decoder()) { }
    }

    static async Task Write(string fname, long start, long count, long bufRecs) {
      Debug.Assert(start > 0);
      Debug.Assert(count >= 0);
      Debug.Assert(bufRecs >= 0);
      using (var writer = new Writer(fname)) {
        long recs = 0;
        for (long i = start; i != start + count; ++i) {
          await writer.Write(new Event<long>(new DateTime(i, DateTimeKind.Utc), i));
          if (++recs == bufRecs) {
            await writer.FlushAsync(flushToDisk: false);
            recs = 0;
          }
        }
      }
    }

    struct ReadStats {
      public long Total { get; set; }
      public long First { get; set; }
      public long Last { get; set; }
    }

    [Flags]
    enum FileState {
      Pristine = 0,
      Corrupted = 1 << 0,
      Truncated = 1 << 1,
      Expanding = Corrupted | 1 << 2,
    }

    static async Task<ReadStats> ReadAllAfter(string fname, long after, long bufRecs, FileState state) {
      using (var reader = new Reader(fname)) {
        var stats = new ReadStats();
        long lastLen = -1;
        await reader.ReadAfter(new DateTime(after, DateTimeKind.Utc)).ForEachAsync((IEnumerable<Event<long>> buf) => {
          Event<long>[] events = buf.ToArray();
          for (int i = 0; i != events.Length; ++i) {
            Assert.IsTrue(events[i].Value > 0);
            Assert.AreEqual(events[i].Timestamp.Ticks, events[i].Value);
            if (i != 0) Assert.AreEqual(events[i - 1].Value + 1, events[i].Value);
          }
          Assert.AreNotEqual(0, events.Length);
          Assert.IsTrue(events.Length <= bufRecs);
          Assert.IsTrue(events[0].Value > stats.Last);
          Assert.IsTrue((events[0].Value - 1) % bufRecs == 0);
          if (stats.First == 0) {
            stats.First = events.First().Value;
          } else {
            Assert.AreEqual(bufRecs, lastLen);
            if (!state.HasFlag(FileState.Expanding)) Assert.IsTrue(events.First().Value > after);
          }
          stats.Last = events.Last().Value;
          stats.Total += events.Length;
          lastLen = events.Length;
          return Task.CompletedTask;
        });
        return stats;
      }
    }

    async Task VerifyFile(string fname, long n, long bufRecs, FileState state) {
      Debug.Assert(n >= 0);
      Debug.Assert(bufRecs > 0);
      var pos = new[] {
          0, 1, 2, 3, 4, 5,
          bufRecs - 2, bufRecs - 1, bufRecs, bufRecs + 1, bufRecs + 2,
          2 * bufRecs - 2, 2 * bufRecs - 1, 2 * bufRecs, 2 * bufRecs + 1, 2 * bufRecs + 2,
          n - bufRecs - 2, n - bufRecs - 1, n - bufRecs, n - bufRecs + 1, n - bufRecs + 2,
          n / 2 - 2, n / 2 - 1, n / 2, n / 2 + 1, n / 2 + 2,
          n - 2, n - 1, n, n + 1, n + 2
        };
      foreach (long after in pos.Where(p => p >= 0).Distinct()) {
        ReadStats stats = await ReadAllAfter(fname, after, bufRecs, state);
        Assert.IsTrue(stats.Last <= n);
        if (!state.HasFlag(FileState.Corrupted) && stats.Total > 0) {
          long start = Math.Max(0, Math.Min(n, after) - 1) / bufRecs * bufRecs + 1;
          Assert.IsTrue(stats.First <= start);
          if (!state.HasFlag(FileState.Truncated)) Assert.AreEqual(start, stats.First);
          Assert.AreEqual(stats.Total, stats.Last - stats.First + 1);
        }
        if (state == FileState.Pristine) {
          Assert.AreEqual(n, stats.Last);
          if (n > 0 && after <= 1) {
            Assert.AreEqual(n, stats.Total);
            Assert.AreEqual(1, stats.First);
          }
        }
      }
    }

    async Task CorruptVerifyFile(string fname, long n, long bufRecs) {
      using (var file = new FileStream(fname, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 1, useAsync: true)) {
        long size = file.Length;
        var pos = new[] {
          0, 8, 16, 48, 56, 64,
          (64 << 10), (64 << 10) + 8, (64 << 10) + 16, (64 << 10) + 48, (64 << 10) + 56, (64 << 10) + 64,
          size - 1 - 64, size - 1 - 56, size - 1 - 48, size - 1 - 16, size - 1 - 8, size - 1
        };
        int seed = (int)Stopwatch.GetTimestamp();
        foreach (long p in pos.Where(x => x >= 0 && x < size).Distinct().RandomShuffled(seed)) {
          var buf = new byte[1];
          file.Seek(p, SeekOrigin.Begin);
          Assert.AreEqual(1, await file.ReadAsync(buf, 0, 1));
          ++buf[0];
          file.Seek(p, SeekOrigin.Begin);
          await file.WriteAsync(buf, 0, 1);
          await file.FlushAsync();
          await VerifyFile(fname, n, bufRecs, FileState.Corrupted);
        }
      }
    }

    async Task TruncateVerifyFile(string fname, long n, long bufRecs) {
      using (var file = new FileStream(fname, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 1, useAsync: true)) {
        long size = file.Length;
        var pos = new[] {
          0, 8, 16, 48, 56, 64,
          (64 << 10), (64 << 10) + 8, (64 << 10) + 16, (64 << 10) + 48, (64 << 10) + 56, (64 << 10) + 64,
          size - 1 - 64, size - 1 - 56, size - 1 - 48, size - 1 - 16, size - 1 - 8, size - 1
        };
        foreach (long p in pos.Where(x => x >= 0 && x < size).Distinct().OrderByDescending(x => x)) {
          file.SetLength(p);
          Debug.Assert(new FileInfo(fname).Length == p);
          await VerifyFile(fname, n, bufRecs, FileState.Truncated);
        }
      }
    }

    [TestMethod]
    public void WriteAndReadTest() {
      Task.WaitAll(Tests().ToArray());

      IEnumerable<Task> Tests() {
        for (long bufRecs = 1; bufRecs != 4; ++bufRecs) {
          for (long n = 0; n != 16; ++n) {
            yield return Test(n, bufRecs);
          }
        }
        foreach (long bufRecs in new[] { 1 << 10, 4 << 10, 8 << 10, 16 << 10 }) {
          foreach (long n in new[] { 1 << 10, 4 << 10, 8 << 10, 16 << 10 }) {
            yield return Test(n, bufRecs);
          }
        }
      }

      async Task Test(long n, long bufRecs) {
        Debug.Assert(n >= 0);
        Debug.Assert(bufRecs >= 1);

        await WithFile(TestPristine);
        await WithFile(TestCorruption);
        await WithFile(TestTruncation);
        await WithFile(TestAppend);
        await WithFile(TestConcurrent);

        async Task TestPristine(string fname) {
          await Write(fname, 1, n, bufRecs);
          await VerifyFile(fname, n, bufRecs, FileState.Pristine);
        }

        async Task TestCorruption(string fname) {
          await Write(fname, 1, n, bufRecs);
          await CorruptVerifyFile(fname, n, bufRecs);
        }

        async Task TestTruncation(string fname) {
          await Write(fname, 1, n, bufRecs);
          await TruncateVerifyFile(fname, n, bufRecs);
        }

        async Task TestAppend(string fname) {
          foreach (int junk in new[] { 0, 1, 128 << 10 }) {
            long half = (n / 2) / bufRecs * bufRecs;
            File.AppendAllText(fname, new string(' ', junk));
            await Write(fname, 1, half, bufRecs);
            File.AppendAllText(fname, new string(' ', junk));
            await Write(fname, 1 + half, n - half, bufRecs);
            File.AppendAllText(fname, new string(' ', junk));
            await VerifyFile(fname, n, bufRecs, FileState.Pristine);
            File.Delete(fname);
          }
        }

        async Task TestConcurrent(string fname) {
          Task w = Write(fname, 1, n, bufRecs);
          while (!w.IsCompleted) {
            await VerifyFile(fname, n, bufRecs, FileState.Expanding);
          }
        }
      }

      async Task WithFile(Func<string, Task> action) {
        string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        try {
          await action.Invoke(fname);
        } finally {
          if (File.Exists(fname)) File.Delete(fname);
        }
      }
    }
  }
}
