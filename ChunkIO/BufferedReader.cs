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

    // If false chunks cannot follow true chunks, seeks to the last false chunk if there are any or
    // to the very first chunk otherwise.
    //
    // TODO: Document what it does if there is no ordering guarantee.
    public async Task<InputBuffer> ReadAtPartitionAsync(Func<UserData, bool> pred) {
      if (pred is null) throw new ArgumentNullException(nameof(pred));
      IChunk left = await _reader.ReadAfterAsync(0);
      while (true) {
        if (left is null) return null;
        if (!pred.Invoke(left.UserData)) break;
        InputBuffer res = await MakeBuffer(left);
        if (res != null) return res;
        left = await _reader.ReadAfterAsync(left.EndPosition);
      }
      IChunk right = await _reader.ReadBeforeAsync(long.MaxValue);
      while (true) {
        if (right is null) return null;
        if (right.BeginPosition < left.BeginPosition) return null;
        if (pred.Invoke(right.UserData)) break;
        InputBuffer res = await MakeBuffer(right);
        if (res != null) return res;
        right = await _reader.ReadBeforeAsync(right.BeginPosition - 1);
      }
      Debug.Assert(left != null && right != null && left.BeginPosition <= right.BeginPosition);
      if (left.EndPosition > right.BeginPosition) return null;
      if (left.EndPosition == right.BeginPosition) return await MakeBuffer(left) ?? await MakeBuffer(right);
      long p = left.EndPosition + (right.BeginPosition - left.EndPosition) / 2;
      IChunk mid = await _reader.ReadAfterAsync(p);
      // TODO: Implement me.
      throw new NotImplementedException();
    }

    public Task<InputBuffer> ReadNextAsync() {
      throw new NotImplementedException();
    }

    public Task FlushRemoteWriterAsync() {
      throw new NotImplementedException();
    }

    public void Dispose() => _reader.Dispose();

    static async Task<InputBuffer> MakeBuffer(IChunk chunk) {
      var content = new byte[chunk.ContentLength];
      if (!await chunk.ReadContentAsync(content, 0)) return null;
      using (var input = new MemoryStream(chunk.ContentLength)) {
        input.Write(content, 0, content.Length);
        input.Seek(0, SeekOrigin.Begin);
        using (var deflate = new DeflateStream(input, CompressionMode.Decompress, leaveOpen: true)) {
          var output = new MemoryStream(4 * chunk.ContentLength);
          try {
            var block = new byte[1024];
            while (true) {
              int n = deflate.Read(block, 0, block.Length);
              if (n <= 0) break;
              output.Write(block, 0, n);
            }
            output.Seek(0, SeekOrigin.Begin);
            return new Buffer(output, chunk.UserData);
          } catch {
            output.Dispose();
            throw;
          }
        }
      }
    }

    class Buffer : InputBuffer {
      readonly MemoryStream _strm;

      public Buffer(MemoryStream strm, UserData userData) : base(userData) {
        _strm = strm;
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
