using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO.Example {
  struct PriceLevel {
    public PriceLevel(decimal price, decimal size) {
      Debug.Assert(price != 0);
      Debug.Assert(size >= 0);
      Price = price;
      Size = size;
    }

    // If Price < 0, it's an ask (limit sell order) with price equal to -Price.
    // Otherwise it's a bid (limit buy order).
    public decimal Price { get; }
    public decimal Size { get; }

    public override string ToString() => $"({(Price < 0 ? "sell" : "buy")} {Size} @ {Math.Abs(Price)})";
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
      // Here we are encoding data differently depending on isPrimary but this isn't a requirement.
      // For example, it makes sense to encode trades the same way regardless of isPrimary.
      _book.ApplyPatch(levels);
      if (isPrimary) levels = _book.GetSnapshot();
      writer.Write(levels.Length);
      foreach (PriceLevel lvl in levels) {
        writer.Write(lvl.Price);
        writer.Write(lvl.Size);
      }
    }
  }

  class OrderBookUpdateDecoder : EventDecoder<PriceLevel[]> {
    protected override PriceLevel[] Decode(BinaryReader reader, bool isPrimary) {
      // It so happens that decoding in our examle doesn't depend on isPrimary, but it could.
      // There is a guarantee that the decoder always sees the same value of isPrimary as what
      // the encoder saw when it produced the record.
      var res = new PriceLevel[reader.ReadInt32()];
      for (int i = 0; i != res.Length; ++i) {
        res[i] = new PriceLevel(price: reader.ReadDecimal(), size: reader.ReadDecimal());
      }
      return res;
    }
  }

  class OrderBookWriter : TimeSeriesWriter<Event<PriceLevel[]>> {
    public OrderBookWriter(string fname) : base(fname, new OrderBookPatchEncoder(), Opt(fname)) { }

    public static WriterOptions Opt(string fname) {
      // Guarantees:
      //
      //   * If the writer process terminates unexpectedly, we'll lose at most 1h+5m worth of data.
      //   * If the OS terminates unexpectedly, we'll lose at most 3h+1h worth of data.
      //
      // We flush data more often than necessary with a different factor for every file. This is done
      // to avoid flushing a large number of files at the same time periodically.
      var rand = new Random(fname.GetHashCode());
      var opt = new WriterOptions();
      // Auto-close chunks older than 1h. This timer starts when a chunk is created.
      opt.CloseChunk.Age = Jitter(TimeSpan.FromHours(1));
      // Flush all closed chunks older than 5m to OS. This timer when a chunk is closed.
      opt.FlushToOS.Age = Jitter(TimeSpan.FromMinutes(5));
      // Flush all closed chunks older than 3h to disk. This timer when a chunk is closed.
      opt.FlushToDisk.Age = Jitter(TimeSpan.FromHours(3));
      return opt;

      // Returns t multiplied by a random number in [0.5, 1).
      TimeSpan Jitter(TimeSpan t) => TimeSpan.FromTicks((long)((0.5 * rand.NextDouble() + 0.5) * t.Ticks));
    }
  }

  class OrderBookReader : TimeSeriesReader<Event<PriceLevel[]>> {
    public OrderBookReader(string fname) : base(fname, new OrderBookUpdateDecoder()) { }
  }

  static class Printer {
    public static string Print(PriceLevel[] lvl) => "[" + string.Join(", ", lvl) + "]";
    public static string Print(Event<PriceLevel[]> e)
        => $"{e.Timestamp.ToString("yyyy-MM-dd HH:mm:ss")} => {Print(e.Value)}";
  }

  class Example {
    static DateTime T0 { get; } = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    static async Task WriteOrderBooks(string fname) {
      Console.WriteLine("Writing order books...");
      using (var writer = new OrderBookWriter(fname)) {
        // Initial snapshot.
        await Write(new Event<PriceLevel[]>(T0 + TimeSpan.FromSeconds(0), new[] {
          new PriceLevel(2, 3),
          new PriceLevel(3, 1),
          new PriceLevel(-5, 2),
        }));
        // A couple of patches.
        await Write(new Event<PriceLevel[]>(T0 + TimeSpan.FromSeconds(1), new[] {
          new PriceLevel(3, 0),
          new PriceLevel(4, 1),
        }));
        await Write(new Event<PriceLevel[]>(T0 + TimeSpan.FromSeconds(2), new[] {
          new PriceLevel(-6, 4),
        }));

        // Flush to close the current chunk to demonstrate how it affects the data
        // we'll read from the file later.
        Console.WriteLine("Flushing and writing some more...");
        await writer.FlushAsync(flushToDisk: false);
        // Write one more patch.
        await Write(new Event<PriceLevel[]>(T0 + TimeSpan.FromSeconds(3), new[] {
          new PriceLevel(2, 0),
          new PriceLevel(-6, 2),
        }));

        Task Write(Event<PriceLevel[]> e) {
          // We are always sending patches to the writer. The writer then decides whether to
          // encode them as snapshots or patches on disk.
          Console.WriteLine("  {0}", Printer.Print(e));
          return writer.Write(e);
        }
      }
    }

    static async Task ReadOrderBooks(string fname, DateTime from) {
      Console.WriteLine("Reading order books starting from {1:yyyy-MM-dd HH:mm:ss}", fname, from);
      using (var reader = new OrderBookReader(fname)) {
        // Read all orders books with timestamps after `from`.
        await reader.ReadAfter(from).ForEachAsync(ProcessChunks);
      }

      Task ProcessChunks(IEnumerable<Event<PriceLevel[]>> chunks) {
        // Our encoder and decoder guarantee that every chunk has at least one event, the first is
        // always a snapshot and the rest are always patches.
        bool isPrimary = true;
        foreach (Event<PriceLevel[]> e in chunks) {
          Console.WriteLine("  {0}: {1}", isPrimary ? "Snapshot" : "Patch   ", Printer.Print(e));
          isPrimary = false;
        }
        return Task.CompletedTask;
      }
    }

    // Sample output:
    //
    //   Writing order books...
    //     2000-01-01 00:00:00 => [(buy 3 @ 2), (buy 1 @ 3), (sell 2 @ 5)]
    //     2000-01-01 00:00:01 => [(buy 0 @ 3), (buy 1 @ 4)]
    //     2000-01-01 00:00:02 => [(sell 4 @ 6)]
    //   Flushing and writing some more...
    //     2000-01-01 00:00:03 => [(buy 0 @ 2), (sell 2 @ 6)]
    //
    //   Reading order books starting from 0001-01-01 00:00:00
    //     Snapshot: 2000-01-01 00:00:00 => [(buy 3 @ 2), (buy 1 @ 3), (sell 2 @ 5)]
    //     Patch   : 2000-01-01 00:00:01 => [(buy 0 @ 3), (buy 1 @ 4)]
    //     Patch   : 2000-01-01 00:00:02 => [(sell 4 @ 6)]
    //     Snapshot: 2000-01-01 00:00:03 => [(sell 2 @ 6), (sell 2 @ 5), (buy 1 @ 4)]
    //
    //   Reading order books starting from 2000-01-01 00:00:02
    //     Snapshot: 2000-01-01 00:00:00 => [(buy 3 @ 2), (buy 1 @ 3), (sell 2 @ 5)]
    //     Patch   : 2000-01-01 00:00:01 => [(buy 0 @ 3), (buy 1 @ 4)]
    //     Patch   : 2000-01-01 00:00:02 => [(sell 4 @ 6)]
    //     Snapshot: 2000-01-01 00:00:03 => [(sell 2 @ 6), (sell 2 @ 5), (buy 1 @ 4)]
    //
    //   Reading order books starting from 2000-01-01 00:00:03
    //     Snapshot: 2000-01-01 00:00:03 => [(sell 2 @ 6), (sell 2 @ 5), (buy 1 @ 4)]
    //
    //   Reading order books starting from 9999-12-31 23:59:59
    //     Snapshot: 2000-01-01 00:00:03 => [(sell 2 @ 6), (sell 2 @ 5), (buy 1 @ 4)]
    static int Main(string[] args) {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        WriteOrderBooks(fname).Wait();
        Console.WriteLine();
        // Reading from DateTime.MinValue always gives all data.
        ReadOrderBooks(fname, DateTime.MinValue).Wait();
        Console.WriteLine();
        // This will also give us all data because the first chunk with timestamp >= T0 + 2s is
        // the very first one.
        ReadOrderBooks(fname, T0 + TimeSpan.FromSeconds(2)).Wait();
        Console.WriteLine();
        // This will give us only the second chunk.
        ReadOrderBooks(fname, T0 + TimeSpan.FromSeconds(3)).Wait();
        Console.WriteLine();
        // Reading from DateTime.MaxValue always gives the very last chunk.
        ReadOrderBooks(fname, DateTime.MaxValue).Wait();
        return 0;
      } catch (Exception e) {
        Console.Error.WriteLine("Error: {0}", e);
        return 1;
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
      }
    }
  }
}
