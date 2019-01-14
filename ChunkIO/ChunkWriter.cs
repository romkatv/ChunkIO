using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  class ChunkWriter : IDisposable {
    readonly ByteWriter _writer;
    readonly byte[] _header = new byte[ChunkHeader.Size];
    readonly byte[] _meter = new byte[Meter.Size];

    public ChunkWriter(string fname) {
      _writer = new ByteWriter(fname);
    }

    public string Name => _writer.Name;

    public async Task WriteAsync(UserData userData, byte[] array, int offset, int count) {
      if (array == null) throw new ArgumentNullException(nameof(array));
      if (offset < 0 || count < 0 || array.Length - count <= offset) {
        throw new Exception($"Invalid range for array of length {array.Length}: [{offset}, {offset} + {count})");
      }
      new Meter() {
        ChunkBeginPosition = (ulong)_writer.Position,
        ContentLength = (ulong)count,
      }.WriteTo(_meter);
      new ChunkHeader() {
        UserData = userData,
        ContentLength = (ulong)count,
        ContentHash = SipHash.ComputeHash(array, offset, count),
      }.WriteTo(_header);
      await WriteMetered(_header, 0, _header.Length);
      await WriteMetered(array, offset, count);
    }

    public Task FlushAsync(bool flushToDisk) => _writer.FlushAsync(flushToDisk);

    public void Dispose() => _writer.Dispose();

    async Task WriteMetered(byte[] array, int offset, int count) {
      while (count > 0) {
        long p = _writer.Position % Chunk.MeterInterval;
        if (p == 0) {
          await _writer.WriteAsync(_meter, 0, _meter.Length);
          p += _meter.Length;
        }
        int n = (int)Math.Min(count, Chunk.MeterInterval - p);
        await _writer.WriteAsync(array, offset, n);
        offset += n;
        count -= n;
      }
    }
  }
}
