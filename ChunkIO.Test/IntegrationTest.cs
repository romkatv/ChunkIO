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

    class TradeEncoder : EventEncoder<Trade> {
      protected override void Encode(BinaryWriter writer, Trade trade, bool isPrimary) {
        writer.Write(trade.Price);
        writer.Write(trade.Size);
      }
    }

    class TradeDecoder : EventDecoder<Trade> {
      protected override Trade Decode(BinaryReader reader, bool isPrimary) {
        return new Trade(price: reader.ReadDecimal(), size: reader.ReadDecimal());
      }
    }

    class TradeWriter : TimeSeriesWriter<Event<Trade>> {
      public TradeWriter(string fname) : base(fname, new TradeEncoder()) { }
    }

    class TradeReader : TimeSeriesReader<Event<Trade>> {
      public TradeReader(string fname) : base(fname, new TradeDecoder()) { }
    }

    static async Task WriteManyAsync(string fname, double seconds) {
      using (var writer = new TradeWriter(fname)) {
        long records = 0;
        DateTime start = DateTime.UtcNow;
        while (true) {
          DateTime now = DateTime.UtcNow;
          await writer.Write(new Event<Trade>(now, new Trade(1, 1)));
          ++records;
          if (now >= start + TimeSpan.FromSeconds(seconds)) break;
        }
        await writer.FlushAsync(flushToDisk: false);
        seconds = (DateTime.UtcNow - start).TotalSeconds;
        long bytes = new FileInfo(fname).Length;
        Console.WriteLine("{0:N2} records/sec, {1:N2} bytes/sec", records / seconds, bytes / seconds);
      }
    }

    [TestMethod]
    public void Benchmark() {
      const double Seconds = 10;
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        WriteManyAsync(fname, Seconds).Wait();
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
      }
    }

    [TestMethod]
    public void ReadWriteTrades() {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        using (var writer = new TradeWriter(fname)) {
          for (int i = 0; i != 2; ++i) {
            var t = new DateTime(i + 1, DateTimeKind.Utc);
            var trade = new Trade(price: 10 * (i + 1), size: 100 * (i + 1));
            writer.Write(new Event<Trade>(t, trade)).Wait();
          }
          using (var reader = new TradeReader(fname)) {
            Assert.IsTrue(reader.FlushRemoteWriterAsync(flushToDisk: false).Result);
            // Production code should probably avoid materializing all data like we do here.
            // Instead, it should either iterate over the result of Sync() or -- even better --
            // use ForEachAsync() instead of Sync().
            Event<Trade>[][] chunks = reader.ReadAllAfter(new DateTime(0))
                .Sync()
                .Select(c => c.ToArray())
                .ToArray();
            Assert.AreEqual(1, chunks.Length);
            Event<Trade>[] events = chunks[0].ToArray();
            Assert.AreEqual(2, events.Length);
            for (int i = 0; i != events.Length; ++i) {
              Event<Trade> e = events[i];
              Assert.AreEqual(new DateTime(i + 1, DateTimeKind.Utc), e.Timestamp);
              Assert.AreEqual(10 * (i + 1), e.Value.Price);
              Assert.AreEqual(100 * (i + 1), e.Value.Size);
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

    class OrderBookPatchEncoder : EventEncoder<PriceLevel[]> {
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

    class OrderBookUpdateDecoder : EventDecoder<OrderBookUpdate> {
      protected override OrderBookUpdate Decode(BinaryReader reader, bool isPrimary) {
        var res = new OrderBookUpdate(isSnapshot: isPrimary, lvls: new PriceLevel[reader.ReadInt32()]);
        for (int i = 0; i != res.PriceLevels.Length; ++i) {
          res.PriceLevels[i] = new PriceLevel(price: reader.ReadDecimal(), size: reader.ReadDecimal());
        }
        return res;
      }
    }

    class OrderBookWriter : TimeSeriesWriter<Event<PriceLevel[]>> {
      public OrderBookWriter(string fname) : base(fname, new OrderBookPatchEncoder()) { }
    }

    class OrderBookReader : TimeSeriesReader<Event<OrderBookUpdate>> {
      public OrderBookReader(string fname) : base(fname, new OrderBookUpdateDecoder()) { }
    }

    [TestMethod]
    public void ReadWriteOrderBooks() {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        using (var writer = new OrderBookWriter(fname)) {
          for (int i = 0; i != 2; ++i) {
            var t = new DateTime(i + 1, DateTimeKind.Utc);
            var lvl = new PriceLevel(price: 10 * (i + 1), size: 100 * (i + 1));
            writer.Write(new Event<PriceLevel[]>(t, new[] { lvl })).Wait();
          }
          using (var reader = new OrderBookReader(fname)) {
            reader.FlushRemoteWriterAsync(flushToDisk: false).Wait();
            Event<OrderBookUpdate>[][] chunks = reader.ReadAllAfter(new DateTime(0))
                .Sync()
                .Select(c => c.ToArray())
                .ToArray();
            Assert.AreEqual(1, chunks.Length);
            Event<OrderBookUpdate>[] events = chunks[0].ToArray();
            Assert.AreEqual(2, events.Length);
            for (int i = 0; i != events.Length; ++i) {
              Event<OrderBookUpdate> e = events[i];
              Assert.AreEqual(new DateTime(i + 1, DateTimeKind.Utc), e.Timestamp);
              Assert.AreEqual(i == 0, e.Value.IsSnapshot);
              Assert.AreEqual(1, e.Value.PriceLevels.Length);
              Assert.AreEqual(10 * (i + 1), e.Value.PriceLevels[0].Price);
              Assert.AreEqual(100 * (i + 1), e.Value.PriceLevels[0].Size);
            }
          }
        }
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
      }
    }
  }
}
