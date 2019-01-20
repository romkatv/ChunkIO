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
using System.IO.Compression;
using System.IO;

namespace ChunkIO {
  static class Compression {
    public static ArraySegment<byte> Compress(byte[] array, int offset, int count, CompressionLevel lvl) {
      using (var output = new MemoryStream(count / 4)) {
        using (var deflate = new DeflateStream(output, lvl, leaveOpen: true)) {
          deflate.Write(array, offset, count);
        }
        return new ArraySegment<byte>(output.GetBuffer(), 0, (int)output.Length);
      }
    }

    public static void DecompressTo(byte[] array, int offset, int count, MemoryStream output) {
      output.SetLength(0);
      byte[] block = new byte[1024];
      using (var input = new MemoryStream(array, offset, count, writable: false))
      using (var deflate = new DeflateStream(input, CompressionMode.Decompress, leaveOpen: true)) {
        while (true) {
          int n = deflate.Read(block, 0, block.Length);
          if (n <= 0) break;
          output.Write(block, 0, n);
        }
      }
      output.Seek(0, SeekOrigin.Begin);
    }
  }
}
