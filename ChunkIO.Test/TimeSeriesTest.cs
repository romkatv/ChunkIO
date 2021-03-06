﻿// Copyright 2019 Roman Perepelitsa
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
      public Writer(string fname, WriterOptions opt) : base(fname, new Encoder(), opt) { }
    }

    class Reader : TimeSeriesReader<Event<long>> {
      public Reader(string fname) : base(fname, new Decoder()) { }
    }

    static async Task Write(string fname, long start, long count, long bufRecs) {
      Debug.Assert(start > 0);
      Debug.Assert(count >= 0);
      Debug.Assert(bufRecs > 0);
      using (var writer = new Writer(fname, new WriterOptions() { CloseChunk = null, DisposeFlushToDisk = false })) {
        for (long i = start; i != start + count; ++i) {
          await writer.WriteAsync(new Event<long>(new DateTime(i, DateTimeKind.Utc), i));
          if ((i - start + 1) % bufRecs == 0) await Flush();
        }
        await Flush();

        async Task Flush() {
          while (true) {
            try {
              await writer.FlushAsync(flushToDisk: false);
              break;
            } catch (InjectedWriteException) {
            }
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
        IAsyncEnumerable<IDecodedChunk<Event<long>>> chunks =
            reader.ReadAfter(new DateTime(after, DateTimeKind.Utc));
        using (IAsyncEnumerator<IDecodedChunk<Event<long>>> iter = chunks.GetAsyncEnumerator()) {
          while (await Do(() => iter.MoveNextAsync(CancellationToken.None))) {
            Event<long>[] events = iter.Current.ToArray();
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
          }
        }
        return stats;
      }

      async Task<T> Do<T>(Func<Task<T>> action) {
        while (true) {
          try {
            return await action.Invoke();
          } catch (InjectedReadException) {
          }
        }
      }
    }

    static async Task VerifyFile(string fname, long n, long bufRecs, FileState state) {
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

    static async Task CorruptVerifyFile(string fname, long n, long bufRecs) {
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
          file.Flush();
          await VerifyFile(fname, n, bufRecs, FileState.Corrupted);
        }
      }
    }

    static async Task TruncateVerifyFile(string fname, long n, long bufRecs) {
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

    static async Task WithFile(Func<string, Task> action) {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        await action.Invoke(fname);
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
      }
    }

    static async Task WithErrorInjection(Func<Task> action) {
      Debug.Assert(ByteWriter.ErrorInjector == null);
      Debug.Assert(ByteReader.ErrorInjector == null);
      ByteWriter.ErrorInjector = new WriteErrorInjector();
      ByteReader.ErrorInjector = new ReadErrorInjector();
      try {
        await action.Invoke();
      } finally {
        ByteWriter.ErrorInjector = null;
        ByteReader.ErrorInjector = null;
      }
    }

    static async Task RunTest(Func<string, Task> action) {
      await WithFile(action);
      await WithErrorInjection(() => WithFile(action));
    }

    static async Task RunTest(Func<string, long, long, Task> action) {
      await Task.WhenAll(Tests());
      await WithErrorInjection(() => Task.WhenAll(Tests()));

      IEnumerable<Task> Tests() {
        for (long bufRecs = 1; bufRecs != 4; ++bufRecs) {
          for (long n = 0; n != 16; ++n) {
            yield return WithFile(fname => action.Invoke(fname, n, bufRecs));
          }
        }
        foreach (long bufRecs in new[] { 1 << 10, 4 << 10, 8 << 10, 16 << 10 }) {
          foreach (long n in new[] { 1 << 10, 4 << 10, 8 << 10, 16 << 10 }) {
            yield return WithFile(fname => action.Invoke(fname, n, bufRecs));
          }
        }
      }
    }

    [TestMethod]
    public void SizeTriggersTest() {
      RunTest(TestCloseChunk).Wait();
      async Task TestCloseChunk(string fname) {
        var opt = new WriterOptions() {
          CloseChunk = new Triggers() { Size = 0 },
          FlushToOS = new Triggers() { Size = 0 },
          DisposeFlushToDisk = false,
        };
        using (var writer = new Writer(fname, opt)) {
          long written = 0;
          while (true) {
            try {
              await writer.WriteAsync(new Event<long>(new DateTime(written + 1, DateTimeKind.Utc), written + 1));
              ++written;
              break;
            } catch (TimeSeriesWriteException e) {
              Assert.IsInstanceOfType(e.InnerException, typeof(InjectedWriteException));
              if (e.Result == TimeSeriesWriteResult.RecordsBuffered) {
                ++written;
              } else {
                Assert.AreEqual(TimeSeriesWriteResult.RecordsDropped, e.Result);
              }
            }
          }
          Assert.IsTrue(written > 0);
          while (true) {
            ReadStats stats = await ReadAllAfter(fname, 0, 1, FileState.Expanding);
            if (stats.Total >= written) {
              Assert.AreEqual(1, stats.First);
              Assert.AreEqual(written, stats.Last);
              Assert.AreEqual(written, stats.Total);
              break;
            }
            await Task.Delay(TimeSpan.FromMilliseconds(1));
          }
        }
      }
    }

    [TestMethod]
    public void TimeTriggersTest() {
      RunTest((string fname) => Test(fname, new WriterOptions() {
        CloseChunk = new Triggers() {
          Age = TimeSpan.Zero,
          AgeRetry = TimeSpan.Zero,
        },
        FlushToOS = new Triggers() {
          Size = 0,
        },
        DisposeFlushToDisk = false,
      })).Wait();
      RunTest((string fname) => Test(fname, new WriterOptions() {
        CloseChunk = new Triggers() {
          Age = TimeSpan.Zero,
          AgeRetry = TimeSpan.Zero,
        },
        FlushToOS = new Triggers() {
          Age = TimeSpan.Zero,
          AgeRetry = TimeSpan.Zero,
        },
        DisposeFlushToDisk = false,
      })).Wait();
      RunTest((string fname) => Test(fname, new WriterOptions() {
        CloseChunk = new Triggers() {
          Age = TimeSpan.Zero,
          AgeRetry = TimeSpan.Zero,
        },
        FlushToDisk = new Triggers() {
          Age = TimeSpan.Zero,
          AgeRetry = TimeSpan.Zero,
        },
        DisposeFlushToDisk = false,
      })).Wait();

      async Task Test(string fname, WriterOptions opt) {
        using (var writer = new Writer(fname, opt)) {
          await writer.WriteAsync(new Event<long>(new DateTime(1, DateTimeKind.Utc), 1));
          while (true) {
            ReadStats stats = await ReadAllAfter(fname, 0, 1, FileState.Expanding);
            if (stats.Total > 0) {
              Assert.AreEqual(1, stats.Total);
              Assert.AreEqual(1, stats.First);
              Assert.AreEqual(1, stats.Last);
              break;
            }
            await Task.Delay(TimeSpan.FromMilliseconds(1));
          }
        }
      }
    }

    [TestMethod]
    public void PristineTest() {
      RunTest(async (string fname, long n, long bufRecs) => {
        await Write(fname, 1, n, bufRecs);
        await VerifyFile(fname, n, bufRecs, FileState.Pristine);
      }).Wait();
    }

    [TestMethod]
    public void CorruptionTest() {
      RunTest(async (string fname, long n, long bufRecs) => {
        await Write(fname, 1, n, bufRecs);
        await CorruptVerifyFile(fname, n, bufRecs);
      }).Wait();
    }

    [TestMethod]
    public void TruncationTest() {
      RunTest(async (string fname, long n, long bufRecs) => {
        await Write(fname, 1, n, bufRecs);
        await TruncateVerifyFile(fname, n, bufRecs);
      }).Wait();
    }

    [TestMethod]
    public void AppendTest() {
      RunTest(async (string fname, long n, long bufRecs) => {
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
      }).Wait();
    }

    [TestMethod]
    public void NoMetersTest() {
      RunTest(async (string fname, long n, long bufRecs) => {
        await Write(fname, 1, n, bufRecs);
        using (var file = new FileStream(fname, FileMode.Open, FileAccess.ReadWrite,
                                         FileShare.Read, 1, useAsync: true)) {
          for (long pos = 0; pos < file.Length; pos += Format.MeterInterval) {
            var buf = new byte[1];
            file.Seek(pos, SeekOrigin.Begin);
            Assert.AreEqual(1, await file.ReadAsync(buf, 0, 1));
            ++buf[0];
            file.Seek(pos, SeekOrigin.Begin);
            await file.WriteAsync(buf, 0, 1);
          }
        }
        await VerifyFile(
          fname, n, bufRecs, ByteWriter.ErrorInjector == null ? FileState.Pristine : FileState.Truncated);
        await CorruptVerifyFile(fname, n, bufRecs);
      }).Wait();
    }

    [TestMethod]
    public void ConcurrentReadWriteTest() {
      RunTest(async (string fname, long n, long bufRecs) => {
        Task w = Write(fname, 1, n, bufRecs);
        while (!w.IsCompleted) {
          await VerifyFile(fname, n, bufRecs, FileState.Expanding);
        }
      }).Wait();
    }

    [TestMethod]
    public void ConcurrentWriteDisposeTest() {
      RunTest(async (string fname) => {
        const int N = 8 << 10;
        const int M = 4;
        int written = 0;

        using (var writer = new Writer(fname, new WriterOptions())) {
          async Task Do(Func<Task> action) {
            try {
              while (true) {
                try {
                  await action.Invoke();
                  break;
                } catch (TimeSeriesWriteException e) {
                  Assert.IsInstanceOfType(e.InnerException, typeof(InjectedWriteException));
                } catch (InjectedWriteException) {
                } catch (IOException e) {
                  Assert.AreEqual("Remote writer failed to flush", e.Message);
                }
              }
            } catch (ObjectDisposedException) {
            }
          }

          async Task Write(int x) {
            for (int i = 0; i != M; ++i) {
              int val = x * M + i + 1;
              await Task.Yield();
              await Do(() => writer.WriteAsync(new Event<long>(new DateTime(val, DateTimeKind.Utc), val)));
            }
            await Do(() => writer.FlushAsync(flushToDisk: false));
            await Do(() => RemoteFlush.FlushAsync(writer.Id, flushToDisk: false));
            if (Interlocked.Increment(ref written) >= N) {
              await Do(() => writer.DisposeAsync());
            }
          }

          await Task.WhenAll(Enumerable.Range(0, 2 * N).Select(Write));
        }

        using (var reader = new Reader(fname)) {
          async Task<T> Do<T>(Func<Task<T>> action) {
            while (true) {
              try {
                return await action.Invoke();
              } catch (InjectedReadException) {
              }
            }
          }

          var read = new HashSet<long>();
          using (var enumerator = reader.ReadAfter(DateTime.MinValue).GetAsyncEnumerator()) {
            while (await Do(() => enumerator.MoveNextAsync(CancellationToken.None))) {
              IDecodedChunk<Event<long>> chunk = enumerator.Current;
              foreach (Event<long> e in chunk) {
                Assert.IsTrue(e.Value > 0);
                Assert.IsTrue(e.Value <= 2 * N * M);
                Assert.AreEqual(e.Value, e.Timestamp.Ticks);
                Assert.IsTrue(read.Add(e.Value) || ByteWriter.ErrorInjector != null);
              }
            }
          }
          Assert.IsTrue(read.Count >= N * M);
        }
      }).Wait();
    }
  }
}
