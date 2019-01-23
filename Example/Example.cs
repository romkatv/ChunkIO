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
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO.Example {
  // One price level of a limit order book.
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

    public override string ToString() => $"{(Price < 0 ? "sell" : "buy ")} {Size} @ {Math.Abs(Price)}";
  }

  // Helper class for building order books out of patches.
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

  // Encoder for PriceLevel[]. It allows us to write Event<PriceLevel[]> to ChunkIO.
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

  // Decoder for PriceLevel[]. It allows us to read Event<PriceLevel[]> from ChunkIO.
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

    // Returns options for the writer.
    //
    // The defaults WriterOptions doesn't define any time-based triggers, so it's important to set
    // them manually. These options are recommended for production use.
    static WriterOptions Opt(string fname) {
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
      // Whenever time-based triggers fail, retry them after an hour.
      opt.CloseChunk.AgeRetry = opt.FlushToOS.AgeRetry = opt.FlushToDisk.AgeRetry = Jitter(TimeSpan.FromHours(1));
      return opt;

      // Returns t multiplied by a random number in [0.5, 1).
      TimeSpan Jitter(TimeSpan t) => TimeSpan.FromTicks((long)((0.5 * rand.NextDouble() + 0.5) * t.Ticks));
    }
  }

  class OrderBookReader : TimeSeriesReader<Event<PriceLevel[]>> {
    public OrderBookReader(string fname) : base(fname, new OrderBookUpdateDecoder()) { }
  }

  class Example {
    static  DateTime T0 { get; } = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    static string Print(Event<PriceLevel[]> e) => $"{e.Timestamp:HH:mm} => {string.Join("; ", e.Value)}";

    // Writes a bunch of order books to the specified file.
    static async Task WriteOrderBooks(string fname) {
      Console.WriteLine("Writing order books into chunk #1");
      using (var writer = new OrderBookWriter(fname)) {
        // Helper function that prints to console everything we are writing.
        Task Write(Event<PriceLevel[]> e) {
          // We are always sending patches to the writer. The writer then decides whether to
          // encode them as snapshots or patches on disk.
          Console.WriteLine("  {0}", Print(e));
          return writer.Write(e);
        }

        // Write the initial snapshot. It'll go into the first chunk.
        await Write(new Event<PriceLevel[]>(T0 + TimeSpan.FromMinutes(0), new[] {
          new PriceLevel(2, 3),
          new PriceLevel(3, 1),
          new PriceLevel(-5, 2),
        }));
        // Write a couple of patches. They'll go into the same chunk as the initial snapshot.
        await Write(new Event<PriceLevel[]>(T0 + TimeSpan.FromMinutes(1), new[] {
          new PriceLevel(3, 0),
          new PriceLevel(4, 1),
        }));
        await Write(new Event<PriceLevel[]>(T0 + TimeSpan.FromMinutes(2), new[] {
          new PriceLevel(-6, 4),
        }));

        // Flush to close the current chunk to demonstrate how it affects the data
        // we'll read from the file later.
        Console.WriteLine("Writing order books into chunk #2");
        await writer.FlushAsync(flushToDisk: false);
        // Write one more patch. It will be encoded as a snapshot in the second chunk.
        await Write(new Event<PriceLevel[]>(T0 + TimeSpan.FromMinutes(3), new[] {
          new PriceLevel(2, 0),
          new PriceLevel(-6, 2),
        }));

        // In asynchronous code it's a good idea to manually flush the writer before disposing it.
        // Otherwise Dispose() will block as it'll have to flush any unwritten data.
        await writer.FlushAsync(flushToDisk: false);
      }
    }

    static void PrintChunk(IDecodedChunk<Event<PriceLevel[]>> chunk) {
      // Our encoder and decoder guarantee that every chunk has at least one event, the first is
      // always a snapshot and the rest are always patches.
      bool isPrimary = true;
      foreach (Event<PriceLevel[]> e in chunk) {
        Console.WriteLine("  {0}: {1}", isPrimary ? "Snapshot" : "Patch   ", Print(e));
        isPrimary = false;
      }
    }

    // Reads order books from the specified file. Skips chunks whose successors have timestamps not
    // smaller than the specified time. In other words, it reads all order books after `from` and
    // perhaps a little bit extra before that.
    static async Task ReadOrderBooks(string fname, DateTime from) {
      Console.WriteLine("Reading order books starting from {0:yyyy-MM-dd HH:mm}", from);
      using (var reader = new OrderBookReader(fname)) {
        // It's a good idea to ask potential writers that might be writing to our file right now to
        // flush their buffers. There are no writers in this example at this point but this call
        // doesn't hurt.
        await reader.FlushRemoteWriterAsync(flushToDisk: false);
        // Now read order books one chunk at a time.
        await reader.ReadAfter(from).ForEachAsync((chunk) => {
          PrintChunk(chunk);
          return Task.CompletedTask;
        });
      }
    }

    // Advanced method of incrementally reading ChunkIO files while another process is writing to them.
    // It ensures that all data we are interested in is read exactly once and no data is ever missed.
    static async Task ReadOrderBooksIncremental(string fname, DateTime from) {
      using (var reader = new OrderBookReader(fname)) {
        long startPosition = 0;
        // The first iteration of this loop reads all available data in file (subject to time constraints).
        // The second iteration reads all extra data that might have been written in the meantime. In this
        // example no data gets written in the meantime, so the second iteration reads nothing.
        for (int i = 0; i != 2; ++i) {
          long endPosition = await reader.FlushRemoteWriterAsync(flushToDisk: false);
          Console.WriteLine("Reading order books in [{0}, {1}) starting from {2:yyyy-MM-dd HH:mm}",
                            startPosition, endPosition, from);
          // Scan the data we are interested in but ignore all chunks that start outside of
          // [startPosition, endPosition).It's guaranteed that new data cannot appear in the
          // [0, startPosition) range, which we are skipping here.
          await reader.ReadAfter(from, startPosition, endPosition).ForEachAsync((chunk) => {
            PrintChunk(chunk);
            startPosition = chunk.EndPosition;
            return Task.CompletedTask;
          });
        }
      }
    }

    // Sample output:
    //
    //   Writing order books into chunk #1
    //     00:00 => buy  3 @ 2; buy  1 @ 3; sell 2 @ 5
    //     00:01 => buy  0 @ 3; buy  1 @ 4
    //     00:02 => sell 4 @ 6
    //   Writing order books into chunk #2
    //     00:03 => buy  0 @ 2; sell 2 @ 6
    //   
    //   Reading order books starting from 0001-01-01 00:00
    //     Snapshot: 00:00 => buy  3 @ 2; buy  1 @ 3; sell 2 @ 5
    //     Patch   : 00:01 => buy  0 @ 3; buy  1 @ 4
    //     Patch   : 00:02 => sell 4 @ 6
    //     Snapshot: 00:03 => sell 2 @ 6; sell 2 @ 5; buy  1 @ 4
    //   
    //   Reading order books starting from 2000-01-01 00:02
    //     Snapshot: 00:00 => buy  3 @ 2; buy  1 @ 3; sell 2 @ 5
    //     Patch   : 00:01 => buy  0 @ 3; buy  1 @ 4
    //     Patch   : 00:02 => sell 4 @ 6
    //     Snapshot: 00:03 => sell 2 @ 6; sell 2 @ 5; buy  1 @ 4
    //   
    //   Reading order books starting from 2000-01-01 00:03
    //     Snapshot: 00:03 => sell 2 @ 6; sell 2 @ 5; buy  1 @ 4
    //   
    //   Reading order books starting from 9999-12-31 23:59
    //     Snapshot: 00:03 => sell 2 @ 6; sell 2 @ 5; buy  1 @ 4
    //   
    //   Reading order books in two steps starting from 2000-01-01 00:03
    //     Snapshot: 00:03 => sell 2 @ 6; sell 2 @ 5; buy  1 @ 4
    static int Main(string[] args) {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        WriteOrderBooks(fname).Wait();
        Console.WriteLine();
        // Reading from DateTime.MinValue always gives all data.
        ReadOrderBooks(fname, DateTime.MinValue).Wait();
        Console.WriteLine();
        // This will also give us all data because the first chunk with timestamp >= T0 + 2m is
        // the very first one.
        ReadOrderBooks(fname, T0 + TimeSpan.FromMinutes(2)).Wait();
        Console.WriteLine();
        // This will give us only the second chunk.
        ReadOrderBooks(fname, T0 + TimeSpan.FromMinutes(3)).Wait();
        Console.WriteLine();
        // Reading from DateTime.MaxValue always gives the very last chunk.
        ReadOrderBooks(fname, DateTime.MaxValue).Wait();
        Console.WriteLine();
        // ReadOrderBooksTwoStep() will read the same data as ReadOrderBooks() because there are no writers.
        ReadOrderBooksIncremental(fname, T0 + TimeSpan.FromMinutes(3)).Wait();
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
