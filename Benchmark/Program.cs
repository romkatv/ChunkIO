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
    protected override void Encode(BinaryWriter writer, Empty trade, bool isPrimary) { }
  }

  class EmptyDecoder : EventDecoder<Empty> {
    protected override Empty Decode(BinaryReader reader, bool isPrimary) => new Empty();
  }

  class EmptyWriter : TimeSeriesWriter<Event<Empty>> {
    public EmptyWriter(string fname) : base(fname, new EmptyEncoder()) { }
  }

  class EmptyReader : TimeSeriesReader<Event<Empty>> {
    public EmptyReader(string fname) : base(fname, new EmptyDecoder()) { }
  }

  class Program {
    // Writes Empty records to the specified file for the specified amount of time.
    // Returns the number of written records. Records have timestamps with consecutive
    // ticks starting from 1.
    static async Task<long> WriteMany(string fname, double seconds) {
      using (var writer = new EmptyWriter(fname)) {
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
        Console.WriteLine("WriteMany: {0:N} records, {1:N} bytes, {2:N} records/sec, {3:N} bytes/sec.",
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
        Console.WriteLine("ReadAll: {0:N} records, {1:N} bytes, {2:N} records/sec, {3:N} bytes/sec.",
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
        Console.WriteLine("SeekMany: {0:N} seeks, {1:N1} seeks/sec.", seeks, seeks / seconds);
      }
    }

    // Sample run on Intel Core i9-7900X with M.2 SSD:
    //
    //   WriteMany: 17,090,816.00 records, 25,994,933.00 bytes, 569,402.99 records/sec, 866,055.34 bytes/sec.
    //   ReadAll: 17,090,816.00 records, 25,994,933.00 bytes, 3,804,831.91 records/sec, 5,787,105.22 bytes/sec.
    //   SeekMany: 2,103.00 seeks, 210.3 seeks/sec.
    static async Task RunBenchmarks() {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        long written = await WriteMany(fname, seconds: 30);
        long read = await ReadAll(fname);
        if (written != read) throw new Exception($"Written {written} but read back {read}");
        await SeekMany(fname, written, seconds: 10);
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
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
