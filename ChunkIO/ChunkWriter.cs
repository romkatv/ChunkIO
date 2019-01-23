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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  using static Format;

  sealed class ChunkWriter : IDisposable {
    readonly ByteWriter _writer;
    readonly byte[] _header = new byte[ChunkHeader.Size];
    readonly byte[] _meter = new byte[Meter.Size];
    bool _torn = true;

    public ChunkWriter(string fname) {
      _writer = new ByteWriter(fname);
    }

    public string Name => _writer.Name;
    public long Length => _writer.Position;

    public async Task WriteAsync(UserData userData, byte[] array, int offset, int count) {
      if (array == null) throw new ArgumentNullException(nameof(array));
      if (offset < 0 || count < 0 || array.Length - offset < count) {
        throw new Exception($"Invalid range for array of length {array.Length}: [{offset}, {offset} + {count})");
      }
      if (count > MaxContentLength) throw new Exception($"Chunk too big: {count}");
      if (_torn) {
        await WritePadding();
        _torn = false;
      }
      var meter = new Meter() { ChunkBeginPosition = _writer.Position };
      var header = new ChunkHeader() {
        UserData = userData,
        ContentLength = count,
        ContentHash = SipHash.ComputeHash(array, offset, count),
      };
      if (!header.EndPosition(meter.ChunkBeginPosition).HasValue) {
        throw new Exception($"File too big: {meter.ChunkBeginPosition}");
      }
      meter.WriteTo(_meter);
      header.WriteTo(_header);
      try {
        await WriteMetered(_header, 0, _header.Length);
        await WriteMetered(array, offset, count);
      }
      catch {
        _torn = true;
        throw;
      }
    }

    public Task FlushAsync(bool flushToDisk) => _writer.FlushAsync(flushToDisk);

    public void Dispose() => _writer.Dispose();

    async Task WritePadding() {
      int p = (int)(_writer.Position % MeterInterval);
      if (p == 0) return;
      var padding = new byte[MeterInterval - p];
      await _writer.WriteAsync(padding, 0, padding.Length);
    }

    async Task WriteMetered(byte[] array, int offset, int count) {
      while (count > 0) {
        int p = (int)(_writer.Position % MeterInterval);
        if (p == 0) {
          await _writer.WriteAsync(_meter, 0, _meter.Length);
          p += _meter.Length;
        }
        int n = Math.Min(count, MeterInterval - p);
        await _writer.WriteAsync(array, offset, n);
        offset += n;
        count -= n;
      }
    }
  }
}
