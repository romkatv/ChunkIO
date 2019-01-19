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
  class InputBuffer : MemoryStream {
    public InputBuffer(UserData userData) { UserData = userData; }
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
    //   * Else if the predicate evaluates to true on the first buffer, returns the first buffer.
    //   * Else returns a buffer for which the predicate evaluates to false and either it's the
    //     last buffer or the predicate evaluates to true on the next buffer. If there are multiple
    //     buffers satisfying these requirements, it's unspecified which one is returned.
    //
    // Implication: If false buffers cannot follow true buffers, returns the last false buffer if
    // there are any or the very first buffer otherwise.
    public async Task<InputBuffer> ReadAtPartitionAsync(Func<UserData, bool> pred) {
      if (pred == null) throw new ArgumentNullException(nameof(pred));
      IChunk left = await _reader.ReadFirstAsync(0, long.MaxValue);
      if (left == null || pred.Invoke(left.UserData)) return await MakeBuffer(left, Scan.Forward);
      IChunk right = await _reader.ReadLastAsync(left.EndPosition, long.MaxValue);
      if (right == null) return await Decompress(left);
      if (!pred.Invoke(right.UserData)) return await MakeBuffer(right, Scan.Backward);
      while (true) {
        IChunk mid = await ReadMiddle(left.EndPosition, right.BeginPosition);
        if (mid == null) return await MakeBuffer(left, Scan.Backward) ?? await MakeBuffer(left, Scan.Forward);
        if (pred.Invoke(mid.UserData)) {
          right = mid;
        } else {
          left = mid;
        }
      }
    }

    // Returns the buffer that follows the last buffer returned by ReadAtPartitionAsync() or ReadNextAsync(),
    // or null if there aren't any.
    public async Task<InputBuffer> ReadNextAsync() =>
      await MakeBuffer(await _reader.ReadFirstAsync(_last, long.MaxValue), Scan.Forward);

    public Task<bool> FlushRemoteWriterAsync(bool flushToDisk) => RemoteFlush.FlushAsync(Name, flushToDisk);

    public void Dispose() => _reader.Dispose();

    async Task<IChunk> ReadMiddle(long from, long to) {
      if (to <= from) return null;
      if (to == from + 1) return await _reader.ReadFirstAsync(from, to);
      long mid = from + (to - from) / 2;
      return await _reader.ReadFirstAsync(mid, to) ?? await _reader.ReadLastAsync(from, mid);
    }

    enum Scan {
      Forward,
      Backward
    }

    async Task<InputBuffer> MakeBuffer(IChunk chunk, Scan scan) {
      while (true) {
        if (chunk == null) return null;
        InputBuffer res = await Decompress(chunk);
        if (res != null) {
          _last = chunk.EndPosition;
          return res;
        }
        chunk = scan == Scan.Forward
            ? await _reader.ReadFirstAsync(chunk.EndPosition, long.MaxValue)
            : await _reader.ReadLastAsync(0, chunk.BeginPosition);
      }
    }

    public static async Task<InputBuffer> Decompress(IChunk chunk) {
      var content = new byte[chunk.ContentLength];
      if (!await chunk.ReadContentAsync(content, 0)) return null;
      var res = new InputBuffer(chunk.UserData);
      try {
        Compression.DecompressTo(content, 0, content.Length, res);
      } catch {
        res.Dispose();
        return null;
      }
      return res;
    }
  }
}
