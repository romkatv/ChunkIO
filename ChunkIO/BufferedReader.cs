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
  // Readable and seakable but not writable.
  abstract class InputBuffer : Stream {
    protected InputBuffer(UserData userData) { UserData = userData; }
    public UserData UserData { get; }
  }

  class BufferedReader : IDisposable {
    readonly ChunkReader _reader;

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
      IChunk right = await _reader.ReadLastAsync(0, long.MaxValue);
      if (right == null || !pred.Invoke(right.UserData)) return await MakeBuffer(right, Scan.Backward);
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

    public Task<InputBuffer> ReadNextAsync() {
      throw new NotImplementedException();
    }

    public Task FlushRemoteWriterAsync() {
      throw new NotImplementedException();
    }

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
        InputBuffer res = await Buffer.New(chunk);
        if (res != null) return res;
        chunk = scan == Scan.Forward
            ? await _reader.ReadFirstAsync(chunk.EndPosition, long.MaxValue)
            : await _reader.ReadLastAsync(0, chunk.BeginPosition);
      }
    }

    class Buffer : InputBuffer {
      readonly MemoryStream _strm;

      public Buffer(MemoryStream strm, UserData userData) : base(userData) {
        _strm = strm;
      }

      public static async Task<InputBuffer> New(IChunk chunk) {
        var content = new byte[chunk.ContentLength];
        if (!await chunk.ReadContentAsync(content, 0)) return null;
        using (var input = new MemoryStream(chunk.ContentLength)) {
          input.Write(content, 0, content.Length);
          input.Seek(0, SeekOrigin.Begin);
          using (var deflate = new DeflateStream(input, CompressionMode.Decompress, leaveOpen: true)) {
            var output = new MemoryStream(4 * chunk.ContentLength);
            var block = new byte[1024];
            try {
              while (true) {
                int n = deflate.Read(block, 0, block.Length);
                if (n <= 0) break;
                output.Write(block, 0, n);
              }
            } catch {
              output.Dispose();
              return null;
            }
            output.Seek(0, SeekOrigin.Begin);
            return new Buffer(output, chunk.UserData);
          }
        }
      }

      // There is an override for every public virtual/abstract method. These four are the only overrides
      // that don't simply delegate to the underlying stream.
      public override void Flush() { }
      public override bool CanWrite => false;
      public override void SetLength(long value) => throw new NotSupportedException();
      public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

      // The remaining overrides simply delegate to the underlying stream.
      public override bool CanRead => _strm.CanRead;
      public override bool CanSeek => _strm.CanSeek;
      public override long Length => _strm.Length;
      public override bool CanTimeout => _strm.CanTimeout;
      public override int ReadTimeout => _strm.ReadTimeout;
      public override int WriteTimeout => _strm.WriteTimeout;
      public override long Position {
        get => _strm.Position;
        set => _strm.Position = value;
      }

      public override int Read(byte[] buffer, int offset, int count) => _strm.Read(buffer, offset, count);
      public override long Seek(long offset, SeekOrigin origin) => _strm.Seek(offset, origin);
      public override void Close() => _strm.Close();
      public override int EndRead(IAsyncResult asyncResult) => _strm.EndRead(asyncResult);
      public override void EndWrite(IAsyncResult asyncResult) => _strm.EndWrite(asyncResult);
      public override Task FlushAsync(CancellationToken cancellationToken) => _strm.FlushAsync(cancellationToken);
      public override int ReadByte() => _strm.ReadByte();
      public override void WriteByte(byte value) => _strm.WriteByte(value);
      public override IAsyncResult BeginRead(byte[] buffer, int offset, int count,
                                             AsyncCallback callback, object state) {
        return _strm.BeginRead(buffer, offset, count, callback, state);
      }
      public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count,
                                              AsyncCallback callback, object state) {
        return _strm.BeginWrite(buffer, offset, count, callback, state);
      }
      public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken) {
        return _strm.CopyToAsync(destination, bufferSize, cancellationToken);
      }
      public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) {
        return _strm.ReadAsync(buffer, offset, count, cancellationToken);
      }
      public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) {
        return _strm.WriteAsync(buffer, offset, count, cancellationToken);
      }
    }
  }
}
