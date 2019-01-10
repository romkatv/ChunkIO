using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO.Test
{
    struct PriceLevel
    {
        // If Price < 0, it's an ask (limit sell order) with price equal to -Price.
        // Otherwise it's a bid (limit buy order).
        public decimal Price { get; set; }
        public decimal Size { get; set; }
    }

    static class Serialization
    {
        public static void Write(BinaryWriter writer, DateTime t) => writer.Write(t.Ticks);
        public static DateTime ReadDateTime(BinaryReader reader) => new DateTime(reader.ReadInt64());

        public static void Write(BinaryWriter writer, PriceLevel[] levels)
        {
            writer.Write(levels.Length);
            foreach (PriceLevel lvl in levels)
            {
                writer.Write(lvl.Price);
                writer.Write(lvl.Size);
            }
        }

        public static PriceLevel[] ReadPriceLevels(BinaryReader reader)
        {
            var levels = new PriceLevel[reader.ReadInt32()];
            for (int i = 0; i != levels.Length; ++i)
            {
                levels[i] = new PriceLevel() { Price = reader.ReadDecimal(), Size = reader.ReadDecimal() };
            }
            return levels;
        }
    }

    class OrderBook
    {
        Dictionary<decimal, decimal> _levels = new Dictionary<decimal, decimal>();

        public PriceLevel[] GetSnapshot() =>
            _levels.Select(kv => new PriceLevel() { Price = kv.Key, Size = kv.Value }).ToArray();

        public void ApplyPatch(PriceLevel[] patch)
        {
            foreach (PriceLevel lvl in patch) _levels[lvl.Price] = lvl.Size;
            foreach (PriceLevel lvl in patch)
            {
                if (_levels.TryGetValue(lvl.Price, out decimal size) && size == 0) _levels.Remove(lvl.Price);
            }
        }

        public void Clear() => _levels.Clear();
    }

    class MarketDataWriter : IDisposable
    {
        readonly OrderBook _book = new OrderBook();
        readonly BufferedWriter _writer;

        public MarketDataWriter(string fname)
        {
            // Guarantees:
            //
            //   * If the writer process terminates unexpectedly, we'll lose at most 1h worth of data.
            //   * If the OS terminates unexpectedly, we'll lose at most 3h worth of data.
            var opt = new BufferedWriterOptions();
            opt.CloseBuffer.Size = 64 << 10;
            // Auto-close buffers older than 1h.
            opt.CloseBuffer.Age = TimeSpan.FromHours(1);
            // As soon as a buffer is closed, flush to the OS.
            opt.FlushToOS.Age = TimeSpan.Zero;
            // Flush all closed buffers older than 3h to disk.
            opt.FlushToDisk.Age = TimeSpan.FromHours(3);
            _writer = new BufferedWriter(fname, opt);
        }

        // Doesn't block on IO.
        public void WritePatch(DateTime t, PriceLevel[] patch)
        {
            if (t.Kind != DateTimeKind.Utc) throw new Exception("DateTime.Kind must be Utc");
            _book.ApplyPatch(patch);
            using (OutputBuffer buf = _writer.GetBuffer() ?? _writer.NewBuffer())
            using (var w = new BinaryWriter(buf))
            {
                if (buf.UserData == null)
                {
                    // This is a new buffer produced by _writer.NewBuffer(). Write a snapshot into it.
                    buf.UserData = new UserData() { Long0 = t.Ticks };
                    Serialization.Write(w, _book.GetSnapshot());
                    // If the block is set up to automatically close after a certain number of bytes is
                    // written, tell it to exclude snapshot bytes from the calculation. This is necessary
                    // to avoid creating a new block on every call to WritePatch() if snapshots happen to
                    // be large.
                    if (buf.CloseAtSize.HasValue)
                    {
                        w.Flush();
                        buf.CloseAtSize += buf.BytesWritten;
                    }
                }
                else
                {
                    // This is an existing non-empty buffer returned by GetBuffer(). We have already
                    // written a full snapshot into it. Write a patch.
                    Serialization.Write(w, t);
                    Serialization.Write(w, patch);
                }
            }
        }

        public Task Flush(bool flushToDisk) => _writer.Flush(flushToDisk);

        // Can block on IO and throw. Won't do either of these if you call Flush() beforehand and
        // wait for its successful completion.
        public void Dispose() => _writer.Dispose();
    }

    class MarketDataReader : IDisposable
    {
        readonly BufferedReader _reader;

        public MarketDataReader(string fname)
        {
            _reader = new BufferedReader(fname);
        }

        // If there a writer writing to our file, tell it to flush and wait for completion.
        // Works even if the writer is in another process, but not when it's on another machine.
        public Task FlushRemoteWriter() => _reader.FlushRemoteWriter();

        // Reads order books from the file and invokes `onOrderBook` for each of them. The boolean parameter of
        // the callback is true for snapshots.
        //
        // If `onOrderBook` is called at all, the first call is guaranteed to be a snapshot with the largest
        // available timestamp that is not greater than `start`.
        public async Task ReadAllAfter(DateTime start, Action<DateTime, PriceLevel[], bool> onOrderBook)
        {
            InputBuffer buf = await _reader.ReadAtPartition((UserData u) => new DateTime(u.Long0) > start);
            while (buf != null)
            {
                using (var r = new BinaryReader(buf))
                {
                    onOrderBook.Invoke(new DateTime(buf.UserData.Long0), Serialization.ReadPriceLevels(r), true);
                    while (r.PeekChar() != -1)
                    {
                        onOrderBook.Invoke(Serialization.ReadDateTime(r), Serialization.ReadPriceLevels(r), false);
                    }
                }
                buf = await _reader.ReadNext();
            }
        }

        public void Dispose() => _reader.Dispose();
    }

    [TestClass]
    public class IntegrationTest
    {
        [TestMethod]
        public void ReadWriteMarketData()
        {
            string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try
            {
                using (var writer = new MarketDataWriter(fname))
                {
                    writer.WritePatch(new DateTime(1), new[] { new PriceLevel() { Price = 10, Size = 100 } });
                    writer.WritePatch(new DateTime(2), new[] { new PriceLevel() { Price = 20, Size = 200 } });
                    using (var reader = new MarketDataReader(fname))
                    {
                        int read = 0;
                        reader.FlushRemoteWriter().Wait();
                        reader.ReadAllAfter(new DateTime(0), (DateTime t, PriceLevel[] levels, bool isSnapshot) =>
                        {
                            Assert.AreNotEqual(2, read);
                            ++read;
                            Assert.AreEqual(new DateTime(read), t);
                            Assert.AreEqual(1, levels.Length);
                            Assert.AreEqual(10 * read, levels[0].Price);
                            Assert.AreEqual(100 * read, levels[0].Size);
                            Assert.AreEqual(read == 1, isSnapshot);
                        }).Wait();
                        Assert.AreEqual(2, read);
                    }
                }
            }
            finally
            {
                if (File.Exists(fname)) File.Delete(fname);
            }
        }
    }
}
