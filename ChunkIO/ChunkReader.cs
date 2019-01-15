using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using static ChunkIO.Chunk;

namespace ChunkIO {
  interface IChunk {
    long BeginPosition { get; }
    long EndPosition { get; }
    int ContentLength { get; }
    UserData UserData { get; }

    Task<bool> ReadContentAsync(byte[] array, int offset);
  }

  class ChunkReader : IDisposable {
    readonly ByteReader _reader;
    readonly byte[] _meter = new byte[Meter.Size];
    readonly byte[] _header = new byte[ChunkHeader.Size];

    public ChunkReader(string fname) {
      _reader = new ByteReader(fname);
    }

    public string Name => _reader.Name;
    public long Length => _reader.Length;

    public async Task<IChunk> ReadBeforeAsync(long position) {
      position = Math.Min(position, _reader.Length);
      while (true) {
        if (position < 0) return null;
        Meter? meter = await ReadMeter(MeterBefore(position));
        if (!meter.HasValue) {
          position -= MeterInterval;
          continue;
        }
        ChunkHeader? res = null;
        long h = meter.Value.ChunkBeginPosition;
        while (true) {
          ChunkHeader? header = await ReadChunkHeader(h);
          if (!header.HasValue) break;
          res = header;
          long? next = MeteredPosition(h, ChunkHeader.Size + header.Value.ContentLength);
          if (!next.HasValue || next > position) break;
          h = next.Value;
        }
        if (res.HasValue) return new Chunk(this, h, res.Value);
        position = meter.Value.ChunkBeginPosition - 1;
      }
    }

    public async Task<IChunk> ReadAfterAsync(long position) {
      long m = Math.Max(position, 0);
      while (true) {
        if (m >= _reader.Length) return null;
        Meter? meter = await ReadMeter(MeterBefore(m));
        if (!meter.HasValue) {
          m += MeterInterval;
          continue;
        }
        long h = meter.Value.ChunkBeginPosition;
        while (true) {
          ChunkHeader? header = await ReadChunkHeader(h);
          if (!header.HasValue) break;
          if (h >= position) return new Chunk(this, h, header.Value);
          long? next = MeteredPosition(h, ChunkHeader.Size + header.Value.ContentLength);
          if (!next.HasValue) break;
          h = next.Value;
        }
        m = Math.Max(
            m + MeterInterval,
            MeteredPosition(meter.Value.ChunkBeginPosition, ChunkHeader.Size + meter.Value.ContentLength).Value);
      }
    }

    public void Dispose() => _reader.Dispose();

    static long MeterBefore(long pos) => pos / MeterInterval * MeterInterval;

    async Task<Meter?> ReadMeter(long pos) {
      Debug.Assert(pos % MeterInterval == 0);
      _reader.Seek(pos);
      if (await _reader.ReadAsync(_meter, 0, _meter.Length) != _meter.Length) return null;
      var res = new Meter();
      if (!res.ReadFrom(_meter)) return null;
      if (res.ChunkBeginPosition >= pos) return null;
      if (MeteredPosition(res.ChunkBeginPosition, res.ContentLength) <= pos + Meter.Size) return null;
      return res;
    }

    async Task<ChunkHeader?> ReadChunkHeader(long pos) {
      if (!await ReadMetered(pos, _header, 0, _header.Length)) return null;
      var res = new ChunkHeader();
      if (!res.ReadFrom(_header)) return null;
      return res;
    }

    async Task<bool> ReadMetered(long pos, byte[] array, int offset, int count) {
      Debug.Assert(MeteredPosition(pos, count).HasValue);
      if (MeteredPosition(pos, count) > _reader.Length) return false;
      pos = MeteredPosition(pos, 0).Value;
      while (count > 0) {
        _reader.Seek(pos);
        int n = Math.Min(count, (int)(MeterInterval - pos % MeterInterval));
        if (await _reader.ReadAsync(array, offset, n) != n) return false;
        offset += n;
        count -= n;
        pos = MeteredPosition(pos, n).Value;
      }
      return true;
    }

    class Chunk : IChunk {
      readonly ChunkReader _reader;
      readonly ChunkHeader _header;

      public Chunk(ChunkReader reader, long pos, ChunkHeader header) {
        _reader = reader;
        _header = header;
        BeginPosition = pos;
      }

      public long BeginPosition { get; }
      public long EndPosition => MeteredPosition(BeginPosition, ChunkHeader.Size + ContentLength).Value;
      public int ContentLength => _header.ContentLength;
      public UserData UserData => _header.UserData;

      public async Task<bool> ReadContentAsync(byte[] array, int offset) {
        if (array == null) throw new ArgumentNullException(nameof(array));
        if (offset < 0) throw new ArgumentException($"Negative offset: {offset}");
        if (array.Length - ContentLength <= offset)  throw new ArgumentException($"Array too short: {array.Length}");
        long pos = MeteredPosition(BeginPosition, ChunkHeader.Size).Value;
        return await _reader.ReadMetered(pos, array, offset, ContentLength) &&
               SipHash.ComputeHash(array, offset, ContentLength) == _header.ContentHash;
      }
    }
  }
}
