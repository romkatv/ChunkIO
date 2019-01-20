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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using static ChunkIO.Format;

namespace ChunkIO {
  interface IChunk {
    long BeginPosition { get; }
    long EndPosition { get; }
    int ContentLength { get; }
    UserData UserData { get; }

    Task<bool> ReadContentAsync(byte[] array, int offset);
  }

  sealed class ChunkReader : IDisposable {
    readonly ByteReader _reader;
    readonly byte[] _meter = new byte[Meter.Size];
    readonly byte[] _header = new byte[ChunkHeader.Size];
    long _last = 0;

    public ChunkReader(string fname) {
      _reader = new ByteReader(fname);
    }

    public string Name => _reader.Name;
    public long Length => _reader.Length;

    // Returns the last chunk whose ChunkBeginPosition is in [from, to) or null.
    // No requirements on the arguments. If `from >= to` or `to <= 0`, the result is null.
    public async Task<IChunk> ReadLastAsync(long from, long to) {
      if (to <= 0) return null;
      for (long m = MeterBefore(Math.Min(to - 1, _reader.Length)); m >= 0 && from < to; m -= MeterInterval) {
        Debug.Assert(m < to);
        Meter? meter = await ReadMeter(m);
        if (!meter.HasValue) continue;
        IChunk res = await Scan(meter.Value.ChunkBeginPosition);
        if (res != null) return res;
        Debug.Assert(MeterBefore(meter.Value.ChunkBeginPosition) <= m);
        m = MeterBefore(meter.Value.ChunkBeginPosition);
        to = meter.Value.ChunkBeginPosition;
      }
      // This last scan is for the case where all meters are corrupted but we still can get to the
      // desired file position by iterating over chunks.
      return from < to ? await Scan(_last <= from ? _last : 0) : null;

      async Task<IChunk> Scan(long h) {
        ChunkHeader? res = null;
        while (true) {
          Debug.Assert(h < to);
          ChunkHeader? header = await ReadChunkHeader(h);
          if (!header.HasValue) break;
          if (h >= from) res = header;
          long next = header.Value.EndPosition(h).Value;
          if (next >= to) break;
          h = next;
        }
        return res.HasValue ? new Chunk(this, h, res.Value) : null;
      }
    }

    // Returns the first chunk whose ChunkBeginPosition is in [from, to) or null.
    // No requirements on the arguments. If `from >= to` or `to <= 0`, the result is null.
    public async Task<IChunk> ReadFirstAsync(long from, long to) {
      if (from >= _reader.Length) return null;
      if (from == _last) {
        IChunk res = await Scan(from);
        if (res != null) return res;
      }
      for (long m = MeterBefore(Math.Max(0, from)); m < Math.Min(_reader.Length, to); m += MeterInterval) {
        Meter? meter = await ReadMeter(m);
        if (!meter.HasValue) continue;
        IChunk res = await Scan(meter.Value.ChunkBeginPosition);
        if (res != null) return res;
      }
      return null;

      async Task<IChunk> Scan(long h) {
        while (h < to) {
          ChunkHeader? header = await ReadChunkHeader(h);
          if (!header.HasValue) return null;
          long next = header.Value.EndPosition(h).Value;
          if (h >= from) {
            _last = next;
            return new Chunk(this, h, header.Value);
          }
          h = next;
        }
        return null;
      }
    }

    public void Dispose() => _reader.Dispose();

    static long MeterBefore(long pos) => pos / MeterInterval * MeterInterval;

    async Task<Meter?> ReadMeter(long pos) {
      Debug.Assert(pos >= 0 && pos % MeterInterval == 0);
      _reader.Seek(pos);
      if (await _reader.ReadAsync(_meter, 0, _meter.Length) != _meter.Length) return null;
      var res = new Meter();
      if (!res.ReadFrom(_meter)) return null;
      if (res.ChunkBeginPosition > pos) return null;
      return res;
    }

    async Task<ChunkHeader?> ReadChunkHeader(long pos) {
      if (!await ReadMetered(pos, _header, 0, _header.Length)) return null;
      var res = new ChunkHeader();
      if (!res.ReadFrom(_header)) return null;
      if (!res.EndPosition(pos).HasValue) return null;
      return res;
    }

    async Task<bool> ReadMetered(long pos, byte[] array, int offset, int count) {
      Debug.Assert(array != null);
      Debug.Assert(offset >= 0);
      Debug.Assert(array.Length - offset >= count);
      if (!(MeteredPosition(pos, count) <= _reader.Length)) return false;
      while (count > 0) {
        Debug.Assert(IsValidPosition(pos));
        if (pos % MeterInterval == 0) pos += Meter.Size;
        _reader.Seek(pos);
        int n = Math.Min(count, MeterInterval - (int)(pos % MeterInterval));
        if (await _reader.ReadAsync(array, offset, n) != n) return false;
        pos += n;
        offset += n;
        count -= n;
      }
      return true;
    }

    sealed class Chunk : IChunk {
      readonly ChunkReader _reader;
      readonly ChunkHeader _header;

      public Chunk(ChunkReader reader, long pos, ChunkHeader header) {
        _reader = reader;
        _header = header;
        BeginPosition = pos;
      }

      public long BeginPosition { get; }
      public long EndPosition => _header.EndPosition(BeginPosition).Value;
      public int ContentLength => _header.ContentLength;
      public UserData UserData => _header.UserData;

      public async Task<bool> ReadContentAsync(byte[] array, int offset) {
        if (array == null) throw new ArgumentNullException(nameof(array));
        if (offset < 0) throw new ArgumentException($"Negative offset: {offset}");
        if (array.Length - offset < ContentLength)  throw new ArgumentException($"Array too short: {array.Length}");
        long pos = MeteredPosition(BeginPosition, ChunkHeader.Size).Value;
        return await _reader.ReadMetered(pos, array, offset, ContentLength) &&
               SipHash.ComputeHash(array, offset, ContentLength) == _header.ContentHash;
      }
    }
  }
}
