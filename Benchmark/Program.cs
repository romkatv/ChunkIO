using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO.Benchmark {
  struct Empty { }

  class EmptyEncoder : TickEncoder<Empty> {
    protected override void Encode(BinaryWriter writer, Empty trade, bool isPrimary) { }
  }

  class EmptyDecoder : TickDecoder<Empty> {
    protected override Empty Decode(BinaryReader reader, bool isPrimary) => new Empty();
  }

  class EmptyWriter : TimeSeriesWriter<Tick<Empty>> {
    public EmptyWriter(string fname) : base(fname, new EmptyEncoder()) { }
  }

  class EmptyReader : TimeSeriesReader<Tick<Empty>> {
    public EmptyReader(string fname) : base(fname, new EmptyDecoder()) { }
  }

  class Program {
    static async Task WriteMany(string fname, double seconds) {
      using (var writer = new EmptyWriter(fname)) {
        long records = 0;
        DateTime start = DateTime.UtcNow;
        do {
          for (int i = 0; i != 256; ++i) {
            ++records;
            await writer.Write(new Tick<Empty>(new DateTime(records, DateTimeKind.Utc), new Empty()));
          }
        } while (DateTime.UtcNow < start + TimeSpan.FromSeconds(seconds));
        await writer.FlushAsync(flushToDisk: false);
        seconds = (DateTime.UtcNow - start).TotalSeconds;
        long bytes = new FileInfo(fname).Length;
        Console.WriteLine("WriteMany: {0:N2} records/sec, {1:N2} bytes/sec", records / seconds, bytes / seconds);
      }
    }

    static void Benchmark() {
      const double Seconds = 10;
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        WriteMany(fname, Seconds).Wait();
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
      }
    }

    static void Main(string[] args) {
      Benchmark();
    }
  }
}
