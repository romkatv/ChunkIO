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
    // The last chunk read by ReadFirstAsync(). It's used to speed up sequential reads.
    Chunk _last = null;

    public ChunkReader(string fname) {
      _reader = new ByteReader(fname);
    }

    public string Name => _reader.Name;
    public long Length => _reader.Length;

    // Returns the first chunk whose ChunkBeginPosition is in [from, to) or null.
    // No requirements on the arguments. If `from >= to` or `to <= 0`, the result is null.
    public async Task<IChunk> ReadFirstAsync(long from, long to) {
      long len = Length;
      if (to <= 0 || from >= len || from >= to) return null;
      if (from > _last?.BeginPosition && from <= _last?.EndPosition && await _last.IsSkippable()) {
        IChunk res = await Scan(_last.EndPosition);
        if (res != null) return res;
      }
      bool meters = false;
      for (long m = MeterBefore(Math.Max(0, from)); m < Math.Min(len, to); m += MeterInterval) {
        Meter? meter = await ReadMeter(m);
        if (!meter.HasValue) continue;
        meters = true;
        IChunk res = await Scan(meter.Value.ChunkBeginPosition);
        if (res != null) return res;
      }
      if (meters) return null;
      // If there are valid chunks in [from, to) but all meters embedded in them are corrupted, we might still
      // be able to get to them by scanning from the last chunk in [0, from).
      Chunk prev = (Chunk)await ReadLastAsync(0, from);
      return prev == null || !await prev.IsSkippable() ? null : await Scan(prev.EndPosition);

      async Task<IChunk> Scan(long h) {
        while (h < to) {
          ChunkHeader? header = await ReadChunkHeader(h);
          if (!header.HasValue) break;
          if (h >= from) {
            _last = new Chunk(this, h, header.Value);
            return _last;
          }
          if (!await IsSkippable(h, header.Value)) break;
          h = header.Value.EndPosition(h).Value;
        }
        return null;
      }
    }

    // Returns the last chunk whose ChunkBeginPosition is in [from, to) or null.
    // No requirements on the arguments. If `from >= to` or `to <= 0`, the result is null.
    public async Task<IChunk> ReadLastAsync(long from, long to) {
      long len = Length;
      if (to <= 0 || from >= len || from >= to) return null;
      bool meters = false;
      for (long m = MeterBefore(Math.Min(to - 1, len)); m >= 0 && from < to; m -= MeterInterval) {
        Debug.Assert(m < to);
        Meter? meter = await ReadMeter(m);
        if (!meter.HasValue) continue;
        meters = true;
        IChunk res = await Scan(meter.Value.ChunkBeginPosition);
        if (res != null) return res;
        Debug.Assert(MeterBefore(meter.Value.ChunkBeginPosition) <= m);
        m = MeterBefore(meter.Value.ChunkBeginPosition);
        to = meter.Value.ChunkBeginPosition;
      }
      // If there are valid chunks in [from, to) but all meters embedded in them are corrupted, we might still
      // be able to get to them by scanning from the last known valid chunk in [0, from).
      if (meters || from >= to) return null;
      if (from == 0) await Scan(_last?.BeginPosition < to ? _last.BeginPosition : 0);
      Chunk prev = (Chunk)await ReadLastAsync(0, from);
      return prev?.EndPosition < to && await prev.IsSkippable() ? await Scan(prev.EndPosition) : null;

      async Task<IChunk> Scan(long h) {
        ChunkHeader? res = null;
        while (true) {
          Debug.Assert(h < to);
          ChunkHeader? header = await ReadChunkHeader(h);
          if (!header.HasValue) break;
          if (h >= from) res = header;
          long next = header.Value.EndPosition(h).Value;
          if (next >= to) break;
          if (!await IsSkippable(h, header.Value)) break;
          h = next;
        }
        return res.HasValue ? new Chunk(this, h, res.Value) : null;
      }
    }

    // Returns any chunk whose ChunkBeginPosition is in [from, to) or null. Prefers to pick the
    // chunk close to the middle of the range.
    //
    // No requirements on the arguments. If `from >= to` or `to <= 0`, the result is null.
    public async Task<IChunk> ReadMiddleAsync(long from, long to) {
      if (to <= 0 || from >= to) return null;
      if (to / MeterInterval > from / MeterInterval) {
        long mid = from + (to - from) / 2;
        mid = Math.Min(MeterBefore(to), MeterAfter(mid));
        Debug.Assert(mid >= from && mid <= to);
        Meter? meter = await ReadMeter(mid);
        if (meter.HasValue) {
          long pos = meter.Value.ChunkBeginPosition;
          if (pos >= from && pos < to) {
            ChunkHeader? header = await ReadChunkHeader(pos);
            if (header.HasValue) return new Chunk(this, pos, header.Value);
          }
        }
      }
      return await ReadFirstAsync(from, to);
    }

    public void Dispose() => _reader.Dispose();

    static long MeterBefore(long pos) => pos / MeterInterval * MeterInterval;
    static long MeterAfter(long pos) => MeterBefore(pos + MeterInterval - 1);

    // Returns true if it's safe to read the chunk after the specified chunk. There are
    // two cases where blindly reading the next chunk can backfire:
    //
    //   1. By reading the next chunk you'll actually skip valid chunks. So the chunk you'll read
    //      won't really be the next.
    //   2. Even if the next chunk decodes correctly (both its header and content hashes match),
    //      it may be not a real chunk but a part of some larger chunk's content.
    //
    // To see these horrors in action, replace the implementation of this method with `return true`
    // and run tests. TrickyTruncateTest() and TrickyEmbedTest() should fail. They correspond to the
    // two cases described above. Note that there is no malicious action in these tests. The chunkio
    // files are produced by ChunkWriter. There are also file truncations, but they can naturally
    // happen when a processing with ChunkWriter crashes.
    async Task<bool> IsSkippable(long begin, ChunkHeader header) {
      long end = header.EndPosition(begin).Value;
      if (begin / MeterInterval == (end - 1) / MeterInterval) return true;
      Meter? meter = await ReadMeter(MeterBefore(end - 1));
      if (meter.HasValue) return meter.Value.ChunkBeginPosition == begin;
      var content = new byte[header.ContentLength];
      long pos = MeteredPosition(begin, ChunkHeader.Size).Value;
      return await ReadMetered(pos, content, 0, content.Length) && SipHash.ComputeHash(content) == header.ContentHash;
    }

    async Task<Meter?> ReadMeter(long pos) {
      Debug.Assert(pos >= 0 && pos % MeterInterval == 0);
      if (pos == 0) return new Meter() { ChunkBeginPosition = 0 };
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
      if (!(MeteredPosition(pos, count) <= Length)) return false;
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
      bool? _skippable = null;

      public Chunk(ChunkReader reader, long pos, ChunkHeader header) {
        _reader = reader;
        _header = header;
        BeginPosition = pos;
      }

      public long BeginPosition { get; }
      public long EndPosition => _header.EndPosition(BeginPosition).Value;
      public int ContentLength => _header.ContentLength;
      public UserData UserData => _header.UserData;

      public async Task<bool> IsSkippable() {
        if (_skippable == null) _skippable = await _reader.IsSkippable(BeginPosition, _header);
        return _skippable.Value;
      }

      public async Task<bool> ReadContentAsync(byte[] array, int offset) {
        if (array == null) throw new ArgumentNullException(nameof(array));
        if (offset < 0) throw new ArgumentException($"Negative offset: {offset}");
        if (array.Length - offset < ContentLength)  throw new ArgumentException($"Array too short: {array.Length}");
        long pos = MeteredPosition(BeginPosition, ChunkHeader.Size).Value;
        _skippable = await _reader.ReadMetered(pos, array, offset, ContentLength) &&
                     SipHash.ComputeHash(array, offset, ContentLength) == _header.ContentHash;
        return _skippable.Value;
      }
    }
  }
}
