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
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  using static Format;

  class InputChunk : MemoryStream {
    public InputChunk(long beginPosition, long endPosition, UserData userData) {
      BeginPosition = beginPosition;
      EndPosition = endPosition;
      UserData = userData;
    }
    public long BeginPosition { get; }
    public long EndPosition { get; }
    public UserData UserData { get; }
  }

  // Example: Create a reader, seek to a chunk based on user data, print all records in the following chunks.
  //
  //   using (var reader = new BufferedReader(fname)) {
  //     // If there is a writer writing to our file, tell it to flush to OS.
  //     // We now have a guarantee that all chunks with start positions in
  //     // [0, len) are final. They cannot change.
  //     long len = await reader.FlushRemoteWriterAsync(flushToDisk: false);
  //     // Assuming that chunks are partitioned with respect to the predicate
  //     // such that falsy chunks cannot follow truthy chunks, find the last
  //     // falsy chunk or the very first chunk if none are falsy.
  //     InputChunk chunk = await reader.ReadAtPartitionAsync(0, len, (UserData d) => d.ULong1 > 1);
  //     while (chunk != null) {
  //       Console.WriteLine("User data: {0}, {1}.", chunk.UserData.ULong0, chunk.UserData.ULong1);
  //       // Assume that all records in the chunk are two bytes.
  //       Debug.Assert(chunk.Length % 2 == 0);
  //       while (chunk.Position != chunk.Length) {
  //         Console.WriteLine("Record: [{0}, {1}]", chunk.ReadByte(), chunk.ReadByte());
  //       }
  //       // Read the first chunk whose start position is in [chunk.EndPosition, len).
  //       // A.K.A. the next chunk.
  //       chunk = await reader.ReadFirstAsync(chunk.EndPosition, len);
  //     }
  //   }
  sealed class BufferedReader : IDisposable {
    readonly ChunkReader _reader;

    public BufferedReader(string fname) {
      _reader = new ChunkReader(fname);
    }

    public IReadOnlyCollection<byte> Id => _reader.Id;
    public string Name => _reader.Name;
    public long Length => _reader.Length;

    // In order:
    //
    //   * If there are no chunks with the starting file position in [from, to), returns null.
    //   * Else if the predicate evaluates to true on the first chunk, returns the first chunk.
    //   * Else returns a chunk for which the predicate evaluates to false and either it's the
    //     last chunk or the predicate evaluates to true on the next chunk. If there are multiple
    //     chunks satisfying these requirements, it's unspecified which one is returned.
    //
    // Implication: If false chunks cannot follow true chunks, returns the last false chunk if
    // there are any or the very first chunk otherwise.
    //
    // There are no constraints on the values of position boundaries. Even long.MinValue and
    // long.MaxValue are legal. If from >= to, the result is null.
    public async Task<InputChunk> ReadAtPartitionAsync(long from, long to, Func<UserData, bool> pred) {
      if (pred == null) throw new ArgumentNullException(nameof(pred));
      IChunk left = await _reader.ReadFirstAsync(from, to);
      if (left == null || pred.Invoke(left.UserData)) return await MakeChunk(left, Scan.Forward, from, to);
      IChunk right = await _reader.ReadLastAsync(left.BeginPosition + 1, to);
      if (right == null) return await MakeChunk(left, Scan.None, from, to);
      if (!pred.Invoke(right.UserData)) return await MakeChunk(right, Scan.Backward, from, to);
      while (true) {
        IChunk mid = await _reader.ReadMiddleAsync(left.BeginPosition + 1, right.BeginPosition);
        if (mid == null) {
          return await MakeChunk(left, Scan.Backward, from, to) ?? await MakeChunk(left, Scan.Forward, from, to);
        }
        if (pred.Invoke(mid.UserData)) {
          right = mid;
        } else {
          left = mid;
        }
      }
    }

    // Returns the first chunk whose ChunkBeginPosition is in [from, to) or null.
    //
    // You can iterate over chunks by repeatedly calling ReadFirstAsync(chunk.EndPosition, long.MaxValue)
    // until it returns null. This is efficient.
    //
    // There are no constraints on the values of position boundaries. Even long.MinValue and
    // long.MaxValue are legal. If from >= to, the result is null.
    public async Task<InputChunk> ReadFirstAsync(long from, long to) =>
        await MakeChunk(await _reader.ReadFirstAsync(from, to), Scan.Forward, from, to);

    // Returns the last chunk whose ChunkBeginPosition is in [from, to) or null.
    //
    // You can iterate over chunks backwards by repeatedly calling ReadLastAsync(0, chunk.BeginPosition)
    // until it returns null. However, this is less efficient than iterating forward with ReadFirstAsync().
    //
    // There are no constraints on the values of position boundaries. Even long.MinValue and
    // long.MaxValue are legal. If from >= to, the result is null.
    public async Task<InputChunk> ReadLastAsync(long from, long to) =>
        await MakeChunk(await _reader.ReadLastAsync(from, to), Scan.Backward, from, to);

    // If there a writer writing to our file and it's running on the same machine, tells it to flush,
    // waits for completion and returns the size of the file immediately after flushing. Otherwise returns
    // the current file size.
    //
    // Throws if the writer is unable to flush (e.g., disk full).
    //
    // This method can be called concurrently with any other method and with itself.
    //
    // The implication is that all existing chunks with starting positions in
    // [0, FlushRemoteWriterAsync(flushToDisk).Result) are guaranteed to be final and no new chunks will
    // appear there.
    public async Task<long> FlushRemoteWriterAsync(bool flushToDisk) {
      long len = Length;
      long? res = await RemoteFlush.FlushAsync(Id, flushToDisk);
      return res ?? len;
    }

    public void Dispose() => _reader.Dispose();

    enum Scan {
      None,
      Forward,
      Backward
    }

    async Task<InputChunk> MakeChunk(IChunk chunk, Scan scan, long from, long to) {
      while (true) {
        if (chunk == null) return null;
        InputChunk res = await Decompress(chunk);
        if (res != null) return res;
        switch (scan) {
          case Scan.None:
            return null;
          case Scan.Forward:
            chunk = await _reader.ReadFirstAsync(chunk.BeginPosition + 1, to);
            break;
          case Scan.Backward:
            chunk = await _reader.ReadLastAsync(from, chunk.BeginPosition);
            break;
          default:
            Debug.Fail("Invalid scan");
            break;
        }
      }
    }

    public static async Task<InputChunk> Decompress(IChunk chunk) {
      var content = new byte[chunk.ContentLength];
      if (!await chunk.ReadContentAsync(content, 0)) return null;
      var res = new InputChunk(chunk.BeginPosition, chunk.EndPosition, chunk.UserData);
      try {
        Compression.DecompressTo(content, 0, content.Length, res);
      } catch {
        res.Dispose();
        // This translation of decompression errors into missing chunks is the only reason
        // why ReadAtPartitionAsync is implemented in BufferedReader rather than ChunkReader.
        return null;
      }
      return res;
    }
  }
}
