using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  sealed class ChunkWriter : IDisposable {
    readonly ByteWriter _writer;
    readonly byte[] _header = new byte[ChunkHeader.Size];
    readonly byte[] _meter = new byte[Meter.Size];
    bool _torn = true;

    public ChunkWriter(string fname) {
      _writer = new ByteWriter(fname);
    }

    public string Name => _writer.Name;

    public async Task WriteAsync(UserData userData, byte[] array, int offset, int count) {
      if (array == null) throw new ArgumentNullException(nameof(array));
      if (offset < 0 || count < 0 || array.Length - offset < count) {
        throw new Exception($"Invalid range for array of length {array.Length}: [{offset}, {offset} + {count})");
      }
      if (count > Chunk.MaxContentLength) throw new Exception($"Record too big: {count}");
      if (_torn) {
        await WritePadding();
        _torn = false;
      }
      var meter = new Meter() { ChunkBeginPosition = _writer.Position };
      var header = new ChunkHeader() {
        UserData = userData,
        ContentLength = count,
        ContentHash = SipHash.ComputeHash(array, offset, count),
      };
      if (!header.EndPosition(meter.ChunkBeginPosition).HasValue) {
        throw new Exception($"File too big: {meter.ChunkBeginPosition}");
      }
      meter.WriteTo(_meter);
      header.WriteTo(_header);
      try {
        await WriteMetered(_header, 0, _header.Length);
        await WriteMetered(array, offset, count);
      }
      catch {
        _torn = true;
        throw;
      }
    }

    public Task FlushAsync(bool flushToDisk) => _writer.FlushAsync(flushToDisk);

    public void Dispose() => _writer.Dispose();

    async Task WritePadding() {
      int p = (int)(_writer.Position % Chunk.MeterInterval);
      if (p == 0) return;
      var padding = new byte[Chunk.MeterInterval - p];
      await _writer.WriteAsync(padding, 0, padding.Length);
    }

    async Task WriteMetered(byte[] array, int offset, int count) {
      while (count > 0) {
        int p = (int)(_writer.Position % Chunk.MeterInterval);
        if (p == 0) {
          await _writer.WriteAsync(_meter, 0, _meter.Length);
          p += _meter.Length;
        }
        int n = Math.Min(count, Chunk.MeterInterval - p);
        await _writer.WriteAsync(array, offset, n);
        offset += n;
        count -= n;
      }
    }
  }
}
