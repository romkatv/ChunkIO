using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO.Test {
  [TestClass]
  public class TradeSerializationExample {
    struct Trade {
      public Trade(decimal price, decimal size) {
        Price = price;
        Size = size;
      }

      public decimal Price { get; }
      // If Price < 0, taker bought with price equal to -Price. Otherwise taker sold.
      public decimal Size { get; }
    }

    class TradeEncoder : TickEncoder<Trade> {
      protected override void Encode(BinaryWriter writer, Trade trade, bool isPrimary) {
        writer.Write(trade.Price);
        writer.Write(trade.Size);
      }
    }

    class TradeDecoder : TickDecoder<Trade> {
      protected override Trade Decode(BinaryReader reader, bool isPrimary) {
        return new Trade(price: reader.ReadDecimal(), size: reader.ReadDecimal());
      }
    }

    class TradeWriter : TimeSeriesWriter<Tick<Trade>> {
      public TradeWriter(string fname) : base(fname, new TradeEncoder()) { }
    }

    class TradeReader : TimeSeriesReader<Tick<Trade>> {
      public TradeReader(string fname) : base(fname, new TradeDecoder()) { }
    }

    [TestMethod]
    public void ReadWriteTrades() {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        using (var writer = new TradeWriter(fname)) {
          for (int i = 0; i != 2; ++i) {
            var t = new DateTime(i);
            var trade = new Trade(price: 10 * i, size: 100 * i);
            writer.Write(new Tick<Trade>(t, trade)).Wait();
          }
          using (var reader = new TradeReader(fname)) {
            reader.FlushRemoteWriterAsync(flushToDisk: false).Wait();
            // Production code should probably avoid materializing all data like we do here.
            // Instead, it should either iterate over the result of Sync() or -- even better --
            // use ForEachAsync() instead of Sync().
            Tick<Trade>[][] chunks = reader.ReadAllAfter(new DateTime(0))
                .Sync()
                .Select(c => c.ToArray())
                .ToArray();
            Assert.AreEqual(1, chunks.Length);
            Tick<Trade>[] ticks = chunks[0].ToArray();
            Assert.AreEqual(2, ticks.Length);
            for (int i = 0; i != ticks.Length; ++i) {
              Tick<Trade> tick = ticks[i];
              Assert.AreEqual(new DateTime(i), tick.Timestamp);
              Assert.AreEqual(10 * i, tick.Value.Price);
              Assert.AreEqual(100 * i, tick.Value.Size);
            }
          }
        }
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
      }
    }
  }

  // The same as TradeSerializationExample above but for order books.
  [TestClass]
  public class OrderBookSerializationExample {
    struct PriceLevel {
      public PriceLevel(decimal price, decimal size) {
        Price = price;
        Size = size;
      }

      // If Price < 0, it's an ask (limit sell order) with price equal to -Price.
      // Otherwise it's a bid (limit buy order).
      public decimal Price { get; }
      public decimal Size { get; }
    }

    struct OrderBookUpdate {
      public OrderBookUpdate(bool isSnapshot, PriceLevel[] lvls) {
        IsSnapshot = isSnapshot;
        PriceLevels = lvls;
      }

      public bool IsSnapshot { get; }
      public PriceLevel[] PriceLevels { get; }
    }

    class OrderBook {
      Dictionary<decimal, decimal> _levels = new Dictionary<decimal, decimal>();

      public PriceLevel[] GetSnapshot() =>
          _levels.Select(kv => new PriceLevel(price: kv.Key, size: kv.Value)).ToArray();

      public void ApplyPatch(PriceLevel[] patch) {
        foreach (PriceLevel lvl in patch) _levels[lvl.Price] = lvl.Size;
        foreach (PriceLevel lvl in patch) {
          if (_levels.TryGetValue(lvl.Price, out decimal size) && size == 0) _levels.Remove(lvl.Price);
        }
      }
    }

    class OrderBookPatchEncoder : TickEncoder<PriceLevel[]> {
      readonly OrderBook _book = new OrderBook();

      protected override void Encode(BinaryWriter writer, PriceLevel[] levels, bool isPrimary) {
        _book.ApplyPatch(levels);
        if (isPrimary) levels = _book.GetSnapshot();
        writer.Write(levels.Length);
        foreach (PriceLevel lvl in levels) {
          writer.Write(lvl.Price);
          writer.Write(lvl.Size);
        }
      }
    }

    class OrderBookUpdateDecoder : TickDecoder<OrderBookUpdate> {
      protected override OrderBookUpdate Decode(BinaryReader reader, bool isPrimary) {
        var res = new OrderBookUpdate(isSnapshot: isPrimary, lvls: new PriceLevel[reader.ReadInt32()]);
        for (int i = 0; i != res.PriceLevels.Length; ++i) {
          res.PriceLevels[i] = new PriceLevel(price: reader.ReadDecimal(), size: reader.ReadDecimal());
        }
        return res;
      }
    }

    class OrderBookWriter : TimeSeriesWriter<Tick<PriceLevel[]>> {
      public OrderBookWriter(string fname) : base(fname, new OrderBookPatchEncoder()) { }
    }

    class OrderBookReader : TimeSeriesReader<Tick<OrderBookUpdate>> {
      public OrderBookReader(string fname) : base(fname, new OrderBookUpdateDecoder()) { }
    }

    [TestMethod]
    public void ReadWriteOrderBooks() {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        using (var writer = new OrderBookWriter(fname)) {
          for (int i = 0; i != 2; ++i) {
            var t = new DateTime(i);
            var lvl = new PriceLevel(price: 10 * i, size: 100 * i);
            writer.Write(new Tick<PriceLevel[]>(t, new[] { lvl })).Wait();
          }
          using (var reader = new OrderBookReader(fname)) {
            reader.FlushRemoteWriterAsync(flushToDisk: false).Wait();
            Tick<OrderBookUpdate>[][] chunks = reader.ReadAllAfter(new DateTime(0))
                .Sync()
                .Select(c => c.ToArray())
                .ToArray();
            Assert.AreEqual(1, chunks.Length);
            Tick<OrderBookUpdate>[] ticks = chunks[0].ToArray();
            Assert.AreEqual(2, ticks.Length);
            for (int i = 0; i != ticks.Length; ++i) {
              Tick<OrderBookUpdate> tick = ticks[i];
              Assert.AreEqual(new DateTime(i), tick.Timestamp);
              Assert.AreEqual(i == 0, tick.Value.IsSnapshot);
              Assert.AreEqual(1, tick.Value.PriceLevels.Length);
              Assert.AreEqual(10 * i, tick.Value.PriceLevels[0].Price);
              Assert.AreEqual(100 * i, tick.Value.PriceLevels[0].Size);
            }
          }
        }
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
      }
    }
  }
}
