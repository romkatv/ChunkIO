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
  // Essentially a wrapper around a file position and chunk header. It's guaranteed to
  // correspond to a real chunk that was written by the user. E.g., the user always
  // writes chunks of length 42 with user data containing 1337, ContentLength is
  // guaranteed to be equal to 42 and user data to contain 1337 no matter how corrupted
  // or truncated the file.
  interface IChunk {
    // File position where this chunk starts. To read the chunk right before this one,
    // call ChunkReader.ReadLastChunk(0, BeginPosition). That is, ask for the last chunk
    // whose start position is less than this chunk's start position.
    long BeginPosition { get; }

    // Past-the-end file position where this chunk ends.
    // Unless you've successfully read the chunk's content with ReadContentAsync and thus
    // implicitly verified that the chunk isn't corrupted, there is no guarantee that the
    // chunk is indeed this long. If this chunk is corrupted, there may be valid chunks
    // before this chunk allegedly ends.
    //
    // If you want to find the next chunk after this, the safe way to do that is to call
    // ChunkReader.ReadFirstAsync(BeginPosition + 1, long.MaxValue). In other words, look
    // for the first chunk whose start position is greater than this chunks start position.
    long EndPosition { get; }

    // Length of the user-supplied content. It corresponds to the count argument of
    // ChunkWriter.Write().
    int ContentLength { get; }

    // Chunk's user data. It corresponds to the userData argument of ChunkWriter.Write().
    UserData UserData { get; }

    // Reads this chunk's content and writes it into the specified array starting at the
    // specified offset. The array must be long enough to accomodate all ContentLength
    // bytes of content.
    //
    // Returns true on success, false on content hash mismatch (corrupted chunk content).
    // Throws on invalid arguments and IO errors.
    Task<bool> ReadContentAsync(byte[] array, int offset);
  }

  // Thread-compatible but not thread-safe. Think FileStream.
  //
  // Example: Print all chunks in a file.
  //
  //   async Task PrintAllChunks(string fname) {
  //     using (var reader = new ChunkReader(fname)) {
  //       long pos = 0;
  //       while (true) {
  //         IChunk chunk = await reader.ReadFirstAsync(pos, long.MaxValue);
  //         if (chunk == null) break;
  //         Console.WriteLine("User data: {0}, {1}.", chunk.UserData.ULong0, chunk.UserData.ULong1);
  //         byte[] content = new byte[chunk.ContentLength];
  //         if (await chunk.ReadContentAsync(content, 0)) {
  //           Console.WriteLine("Content: [{0}].", string.Join(", ", content));
  //         } else {
  //           Console.WriteLine("Content: corrupted.");
  //         }
  //         pos = chunk.BeginPosition + 1;
  //       }
  //     }
  //   }
  sealed class ChunkReader : IDisposable {
    readonly ByteReader _reader;
    readonly byte[] _meter = new byte[Meter.Size];
    readonly byte[] _header = new byte[ChunkHeader.Size];
    // The last chunk read by ReadFirstAsync(). It's used to speed up sequential reads.
    Chunk _last = null;

    public ChunkReader(string fname) {
      _reader = new ByteReader(fname);
    }

    public IReadOnlyCollection<byte> Id => _reader.Id;
    public string Name => _reader.Name;
    public long Length => _reader.Length;

    // Returns the first chunk whose ChunkBeginPosition is in [from, to) or null.
    // No requirements on the arguments. If `from >= to` or `to <= 0`, the result is null.
    public async Task<IChunk> ReadFirstAsync(long from, long to) {
      long len = Length;
      if (!Clamp(ref from, ref to, len)) return null;

      IChunk res = null;
      if (!await ReadNext(_last) && !await ReadNext((Chunk)await ReadLastAsync(0, from))) await Scan();
      return res;

      async Task Scan() {
        Debug.Assert(res == null);
        for (long m = MeterAfter(from); m < len; m += MeterInterval) {
          Debug.Assert(m >= from);
          Meter? meter = await ReadMeter(m);
          if (!meter.HasValue) continue;
          // This happens if ReadLastAsync(0, from) is not skippable.
          if (meter.Value.ChunkBeginPosition < from) continue;
          if (meter.Value.ChunkBeginPosition >= to) break;
          res = await Read(meter.Value.ChunkBeginPosition);
          if (res != null) break;
        }
      }

      async Task<bool> ReadNext(Chunk prev) {
        Debug.Assert(res == null);
        if (prev != null && prev.BeginPosition < from && prev.EndPosition >= from &&
            await prev.IsSkippableAsync()) {
          from = prev.EndPosition;
          if (!Clamp(ref from, ref to, len)) return true;
          res = await Read(from);
        }
        return res != null;
      }

      async Task<IChunk> Read(long pos) {
        Debug.Assert(pos >= from && pos < to);
        ChunkHeader? header = await ReadChunkHeader(pos);
        if (!header.HasValue) return null;
        _last = new Chunk(this, pos, header.Value);
        return _last;
      }
    }

    // Returns the last chunk whose ChunkBeginPosition is in [from, to) or null.
    // No requirements on the arguments. If `from >= to` or `to <= 0`, the result is null.
    public async Task<IChunk> ReadLastAsync(long from, long to) {
      long len = Length;
      if (!Clamp(ref from, ref to, len)) return null;

      for (long m = MeterAfter(to); m < len; m += MeterInterval) {
        Debug.Assert(m >= to);
        Meter? meter = await ReadMeter(m);
        if (!meter.HasValue) continue;
        if (meter.Value.ChunkBeginPosition < to) {
          IChunk res = await Scan(meter.Value.ChunkBeginPosition);
          if (res != null) return res;
        }
        break;
      }

      for (long m = MeterBefore(to - 1); m >= 0 && from < to; m -= MeterInterval) {
        Debug.Assert(m < to);
        Meter? meter = await ReadMeter(m);
        if (!meter.HasValue) continue;
        IChunk res = await Scan(meter.Value.ChunkBeginPosition);
        if (res != null) return res;
        Debug.Assert(MeterBefore(meter.Value.ChunkBeginPosition) <= m);
        m = MeterBefore(meter.Value.ChunkBeginPosition);
        to = meter.Value.ChunkBeginPosition;
      }
      return null;

      async Task<IChunk> Scan(long pos) {
        ChunkHeader? res = null;
        while (true) {
          Debug.Assert(pos < to);
          ChunkHeader? header = await ReadChunkHeader(pos);
          if (!header.HasValue) break;
          if (pos >= from) res = header;
          long end = header.Value.EndPosition(pos).Value;
          if (end >= to) break;
          if (!await IsSkippable(pos, header.Value)) break;
          pos = end;
        }
        return res.HasValue ? new Chunk(this, pos, res.Value) : null;
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

    static bool Clamp(ref long from, ref long to, long len) {
      from = Math.Max(from, 0);
      to = Math.Min(to, len);
      return from < to;
    }

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

      public async Task<bool> IsSkippableAsync() {
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
