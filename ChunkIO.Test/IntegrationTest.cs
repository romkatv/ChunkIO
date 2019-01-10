using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO.Test
{
    struct Tick<T>
    {
        public DateTime Timestamp { get; set; }
        public T Value { get; set; }
    }

    abstract class TickEncoder<T> : ITimeSeriesEncoder<Tick<T>>
    {
        public DateTime EncodePrimary(Stream strm, Tick<T> tick)
        {
            using (var writer = new BinaryWriter(strm)) Encode(writer, tick.Value, isPrimary: true);
            return tick.Timestamp;
        }

        public void EncodeSecondary(Stream strm, Tick<T> tick)
        {
            using (var writer = new BinaryWriter(strm))
            {
                writer.Write(tick.Timestamp.ToUniversalTime().Ticks);
                Encode(writer, tick.Value, isPrimary: false);
            }
        }

        protected abstract void Encode(BinaryWriter writer, T val, bool isPrimary);
    }

    abstract class TickDecoder<T> : ITimeSeriesDecoder<Tick<T>>
    {
        public void DecodePrimary(Stream strm, DateTime t, out Tick<T> val)
        {
            using (var reader = new BinaryReader(strm))
            {
                val = new Tick<T>()
                {
                    Timestamp = t,
                    Value = Decode(reader, isPrimary: true),
                };
            }
        }

        public bool DecodeSecondary(Stream strm, out Tick<T> val)
        {
            if (strm.Position == strm.Length)
            {
                val = default(Tick<T>);
                return false;
            }
            using (var reader = new BinaryReader(strm))
            {
                val = new Tick<T>()
                {
                    Timestamp = new DateTime(reader.ReadInt64(), DateTimeKind.Utc),
                    Value = Decode(reader, isPrimary: false),
                };
            }
            return true;
        }

        protected abstract T Decode(BinaryReader reader, bool isPrimary);
    }

    struct PriceLevel
    {
        // If Price < 0, it's an ask (limit sell order) with price equal to -Price.
        // Otherwise it's a bid (limit buy order).
        public decimal Price { get; set; }
        public decimal Size { get; set; }
    }

    struct OrderBookUpdate
    {
        public bool IsSnapshot { get; set; }
        public PriceLevel[] PriceLevels { get; set; }
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
    }

    class OrderBookPatchEncoder : TickEncoder<PriceLevel[]>
    {
        readonly OrderBook _book = new OrderBook();

        protected override void Encode(BinaryWriter writer, PriceLevel[] levels, bool isPrimary)
        {
            _book.ApplyPatch(levels);
            if (isPrimary) levels = _book.GetSnapshot();
            writer.Write(levels.Length);
            foreach (PriceLevel lvl in levels)
            {
                writer.Write(lvl.Price);
                writer.Write(lvl.Size);
            }
        }
    }

    class OrderBookUpdateDecoder : TickDecoder<OrderBookUpdate>
    {
        protected override OrderBookUpdate Decode(BinaryReader reader, bool isPrimary)
        {
            var res = new OrderBookUpdate()
            {
                IsSnapshot = isPrimary,
                PriceLevels = new PriceLevel[reader.ReadInt32()]
            };
            for (int i = 0; i != res.PriceLevels.Length; ++i)
            {
                res.PriceLevels[i] = new PriceLevel() { Price = reader.ReadDecimal(), Size = reader.ReadDecimal() };
            }
            return res;
        }
    }

    class OrderBookWriter : TimeSeriesWriter<Tick<PriceLevel[]>>
    {
        public OrderBookWriter(string fname) : base(fname, new OrderBookPatchEncoder()) { }
    }

    class OrderBookReader : TimeSeriesReader<Tick<OrderBookUpdate>>
    {
        public OrderBookReader(string fname) : base(fname, new OrderBookUpdateDecoder()) { }
    }

    struct Trade
    {
        public decimal Price { get; set; }
        // If Price < 0, taker bought with price equal to -Price. Otherwise taker sold.
        public decimal Size { get; set; }
    }

    class TradeEncoder : TickEncoder<Trade>
    {
        protected override void Encode(BinaryWriter writer, Trade trade, bool isPrimary)
        {
            writer.Write(trade.Price);
            writer.Write(trade.Size);
        }
    }

    class TradeDecoder : TickDecoder<Trade>
    {
        protected override Trade Decode(BinaryReader reader, bool isPrimary)
        {
            return new Trade() { Price = reader.ReadDecimal(), Size = reader.ReadDecimal() };
        }
    }

    class TradeWriter : TimeSeriesWriter<Tick<Trade>>
    {
        public TradeWriter(string fname) : base(fname, new TradeEncoder()) { }
    }

    class TradeReader : TimeSeriesReader<Tick<Trade>>
    {
        public TradeReader(string fname) : base(fname, new TradeDecoder()) { }
    }

    [TestClass]
    public class IntegrationTest
    {
        [TestMethod]
        public void ReadWriteOrderBooks()
        {
            string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try
            {
                using (var writer = new OrderBookWriter(fname))
                {
                    writer.Write(new Tick<PriceLevel[]>()
                    {
                        Timestamp = new DateTime(1),
                        Value = new[] { new PriceLevel() { Price = 10, Size = 100 } }
                    });
                    writer.Write(new Tick<PriceLevel[]>()
                    {
                        Timestamp = new DateTime(2),
                        Value = new[] { new PriceLevel() { Price = 20, Size = 200 } }
                    });
                    using (var reader = new OrderBookReader(fname))
                    {
                        reader.FlushRemoteWriter().Wait();
                        // Production code should probably avoid materializing all data like we do here.
                        // Instead, it should either iterate over the result of Sync() or even use
                        // ForEachAsync() instead of Sync().
                        Tick<OrderBookUpdate>[][] chunks = reader.ReadAllAfter(new DateTime(0))
                            .Sync()
                            .Select(c => c.ToArray())
                            .ToArray();
                        Assert.AreEqual(1, chunks.Length);
                        Tick<OrderBookUpdate>[] ticks = chunks[0].ToArray();
                        Assert.AreEqual(2, ticks.Length);
                        for (int i = 0; i != ticks.Length; ++i)
                        {
                            Tick<OrderBookUpdate> tick = ticks[i];
                            Assert.AreEqual(new DateTime(i), tick.Timestamp);
                            Assert.AreEqual(i == 0, tick.Value.IsSnapshot);
                            Assert.AreEqual(1, tick.Value.PriceLevels.Length);
                            Assert.AreEqual(10 * i, tick.Value.PriceLevels[0].Price);
                            Assert.AreEqual(100 * i, tick.Value.PriceLevels[0].Size);
                        }
                    }
                }
            }
            finally
            {
                if (File.Exists(fname)) File.Delete(fname);
            }
        }

        // This test is the same as ReadWriteOrderBooks() above but for trades.
        [TestMethod]
        public void ReadWriteTrades()
        {
            string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try
            {
                using (var writer = new TradeWriter(fname))
                {
                    writer.Write(new Tick<Trade>()
                    {
                        Timestamp = new DateTime(1),
                        Value = new Trade() { Price = 10, Size = 100 }
                    });
                    writer.Write(new Tick<Trade>()
                    {
                        Timestamp = new DateTime(2),
                        Value = new Trade() { Price = 20, Size = 200 }
                    });
                    using (var reader = new TradeReader(fname))
                    {
                        reader.FlushRemoteWriter().Wait();
                        Tick<Trade>[][] chunks = reader.ReadAllAfter(new DateTime(0))
                            .Sync()
                            .Select(c => c.ToArray())
                            .ToArray();
                        Assert.AreEqual(1, chunks.Length);
                        Tick<Trade>[] ticks = chunks[0].ToArray();
                        Assert.AreEqual(2, ticks.Length);
                        for (int i = 0; i != ticks.Length; ++i)
                        {
                            Tick<Trade> tick = ticks[i];
                            Assert.AreEqual(new DateTime(i), tick.Timestamp);
                            Assert.AreEqual(10 * i, tick.Value.Price);
                            Assert.AreEqual(100 * i, tick.Value.Size);
                        }
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
