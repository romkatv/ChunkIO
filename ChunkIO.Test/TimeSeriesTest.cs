using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ChunkIO.Test {
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
      public Writer(string fname) : base(fname, new Encoder(), new ChunkIO.WriterOptions() { CloseChunk = null }) { }
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

    static async Task<ReadStats> ReadAllAfter(string fname, long after, long bufRecs, bool pristine) {
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
          if (stats.First == 0) {
            stats.First = events.First().Value;
          } else {
            Assert.AreEqual(bufRecs, lastLen);
            if (pristine) Assert.IsTrue(events.First().Value > after);
          }
          stats.Last = events.Last().Value;
          stats.Total += events.Length;
          lastLen = events.Length;
          return Task.CompletedTask;
        });
        return stats;
      }
    }

    async Task VerifyFile(string fname, long n, long bufRecs, bool pristine) {
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
        ReadStats stats = await ReadAllAfter(fname, after, bufRecs, pristine);
        Assert.IsTrue(stats.Last <= n);
        if (pristine) {
          Assert.AreEqual(n, stats.Last);
          Assert.IsTrue(stats.First <= Math.Max(1, after));
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
        foreach (long p in pos.Where(x => x >= 0 && x < size).Distinct()) {
          var buf = new byte[1];
          file.Seek(p, SeekOrigin.Begin);
          Assert.AreEqual(1, await file.ReadAsync(buf, 0, 1));
          ++buf[0];
          file.Seek(p, SeekOrigin.Begin);
          await file.WriteAsync(buf, 0, 1);
          await file.FlushAsync();
          await VerifyFile(fname, n, bufRecs, pristine: false);
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
          await VerifyFile(fname, n, bufRecs, pristine: false);
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
          await VerifyFile(fname, n, bufRecs, pristine: true);
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
          long half = (n / 2) / bufRecs * bufRecs;
          await Write(fname, 1, half, bufRecs);
          await Write(fname, 1 + half, n - half, bufRecs);
          await VerifyFile(fname, n, bufRecs, pristine: true);
        }

        async Task TestConcurrent(string fname) {
          Task w = Write(fname, 1, n, bufRecs);
          while (!w.IsCompleted) {
            await VerifyFile(fname, n, bufRecs, pristine: false);
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
