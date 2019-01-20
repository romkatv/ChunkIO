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
  class InputChunk : MemoryStream {
    public InputChunk(UserData userData) { UserData = userData; }
    public UserData UserData { get; }
  }

  sealed class BufferedReader : IDisposable {
    readonly ChunkReader _reader;
    long _last = 0;

    public BufferedReader(string fname) {
      _reader = new ChunkReader(fname);
    }

    public string Name => _reader.Name;

    // In order:
    //
    //   * If the data source is empty, returns null.
    //   * Else if the predicate evaluates to true on the first chunk, returns the first chunk.
    //   * Else returns a chunk for which the predicate evaluates to false and either it's the
    //     last chunk or the predicate evaluates to true on the next chunk. If there are multiple
    //     chunks satisfying these requirements, it's unspecified which one is returned.
    //
    // Implication: If false chunks cannot follow true chunks, returns the last false chunk if
    // there are any or the very first chunk otherwise.
    public async Task<InputChunk> ReadAtPartitionAsync(Func<UserData, bool> pred) {
      if (pred == null) throw new ArgumentNullException(nameof(pred));
      IChunk left = await _reader.ReadFirstAsync(0, long.MaxValue);
      if (left == null || pred.Invoke(left.UserData)) return await MakeChunk(left, Scan.Forward);
      IChunk right = await _reader.ReadLastAsync(left.EndPosition, long.MaxValue);
      if (right == null) return await MakeChunk(left, Scan.None);
      if (!pred.Invoke(right.UserData)) return await MakeChunk(right, Scan.Backward);
      while (true) {
        IChunk mid = await ReadMiddle(left.EndPosition, right.BeginPosition);
        if (mid == null) return await MakeChunk(left, Scan.Backward) ?? await MakeChunk(left, Scan.Forward);
        if (pred.Invoke(mid.UserData)) {
          right = mid;
        } else {
          left = mid;
        }
      }
    }

    // Returns the chunk that follows the last chunk returned by ReadAtPartitionAsync() or ReadNextAsync(),
    // or null if there aren't any.
    public async Task<InputChunk> ReadNextAsync() =>
      await MakeChunk(await _reader.ReadFirstAsync(_last, long.MaxValue), Scan.Forward);

    public Task<bool> FlushRemoteWriterAsync(bool flushToDisk) => RemoteFlush.FlushAsync(Name, flushToDisk);

    public void Dispose() => _reader.Dispose();

    async Task<IChunk> ReadMiddle(long from, long to) {
      if (to <= from) return null;
      if (to == from + 1) return await _reader.ReadFirstAsync(from, to);
      long mid = from + (to - from) / 2;
      return await _reader.ReadFirstAsync(mid, to) ?? await _reader.ReadLastAsync(from, mid);
    }

    enum Scan {
      None,
      Forward,
      Backward
    }

    async Task<InputChunk> MakeChunk(IChunk chunk, Scan scan) {
      while (true) {
        if (chunk == null) return null;
        InputChunk res = await Decompress(chunk);
        if (res != null) {
          _last = chunk.EndPosition;
          return res;
        }
        switch (scan) {
          case Scan.None:
            return null;
          case Scan.Forward:
            chunk = await _reader.ReadFirstAsync(chunk.EndPosition, long.MaxValue);
            break;
          case Scan.Backward:
            chunk = await _reader.ReadLastAsync(0, chunk.BeginPosition);
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
      var res = new InputChunk(chunk.UserData);
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
