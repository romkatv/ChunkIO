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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ChunkIO.Test {
  using static Format;

  [TestClass]
  public class ChunkIOTest {
    [TestMethod]
    public void TrickyTruncateTest() {
      WithFile(Test).Wait();
      async Task Test(string fname) {
        using (var writer = new ChunkWriter(fname)) {
          await Write(writer, Content(1, count: MeterInterval - Meter.Size + 1));
        }
        using (var file = new FileStream(fname, FileMode.Open, FileAccess.Write)) {
          Assert.AreEqual(MeterInterval + Meter.Size + ChunkHeader.Size + 1, file.Length);
          file.SetLength(MeterInterval);
        }
        using (var writer = new ChunkWriter(fname)) {
          await Write(writer, Content(2, count: 1));
          Assert.AreEqual(MeterInterval + Meter.Size + ChunkHeader.Size + 1, writer.Length);
          await Write(writer, Content(3, count: 1));
        }
        using (var reader = new ChunkReader(fname)) {
          IChunk chunk = await reader.ReadFirstAsync(1, long.MaxValue);
          Assert.IsNotNull(chunk);
          byte[] content = new byte[chunk.ContentLength];
          Assert.IsTrue(await chunk.ReadContentAsync(content, 0));
          // A naive implementation can give Content(3, 1) instead of Content(2, 1), which means
          // effectively skipping a valid chunk.
          CollectionAssert.AreEqual(Content(2, count: 1), content);
        }
      }
    }

    [TestMethod]
    public void TrickyEmbedTest() {
      WithFile(Test).Wait();
      async Task Test(string fname) {
        using (var writer = new ChunkWriter(fname)) {
          await Write(writer, Content(1, count: 1));
        }
        byte[] chunkio = File.ReadAllBytes(fname);
        CollectionAssert.AreEqual(FileHeader, chunkio.Take(FileHeader.Length).ToArray());
        File.Delete(fname);
        using (var writer = new ChunkWriter(fname)) {
          await Write(writer, Content(1, count: MeterInterval));
        }
        using (var file = new FileStream(fname, FileMode.Open, FileAccess.Write)) {
          Assert.AreEqual(MeterInterval + 2 * Meter.Size + ChunkHeader.Size, file.Length);
          file.SetLength(MeterInterval);
        }
        using (var writer = new ChunkWriter(fname)) {
          await Write(writer, chunkio);
        }
        using (var reader = new ChunkReader(fname)) {
          IChunk chunk = await reader.ReadFirstAsync(1, long.MaxValue);
          Assert.IsNotNull(chunk);
          byte[] content = new byte[chunk.ContentLength];
          Assert.IsTrue(await chunk.ReadContentAsync(content, 0));
          // A naive implementation can give Content(1, 1) instead, which is an awful thing to do
          // because Content(1, 1) is embedded in the middle of a real chunk.
          CollectionAssert.AreEqual(chunkio, content);
        }
      }
    }

    [TestMethod]
    public void MaxChunkSizeTest() {
      WithFile(Test).Wait();
      async Task Test(string fname) {
        using (var writer = new ChunkWriter(fname)) {
          await Write(writer, Content(1, count: 1));
          await Write(writer, Content(2, count: MaxContentLength));
          await Write(writer, Content(3, count: 1));
        }

        using (var reader = new ChunkReader(fname)) {
          IChunk chunk = await reader.ReadFirstAsync(0, long.MaxValue);
          Assert.IsNotNull(chunk);
          byte[] content = new byte[chunk.ContentLength];
          Assert.IsTrue(await chunk.ReadContentAsync(content, 0));
          CollectionAssert.AreEqual(Content(1, count: 1), content);

          chunk = await reader.ReadFirstAsync(chunk.EndPosition, long.MaxValue);
          Assert.IsNotNull(chunk);
          Assert.AreEqual(MaxContentLength, chunk.ContentLength);
          content = new byte[chunk.ContentLength];
          Assert.IsTrue(await chunk.ReadContentAsync(content, 0));
          foreach (byte x in content) {
            if (x != 2) Assert.Fail($"Invalid byte: {x}");
          }

          chunk = await reader.ReadFirstAsync(chunk.EndPosition, long.MaxValue);
          Assert.IsNotNull(chunk);
          content = new byte[chunk.ContentLength];
          Assert.IsTrue(await chunk.ReadContentAsync(content, 0));
          CollectionAssert.AreEqual(Content(3, count: 1), content);

          chunk = await reader.ReadFirstAsync(chunk.EndPosition, long.MaxValue);
          Assert.IsNull(chunk);
        }
      }
    }

    [TestMethod]
    public void ChunkTooBigTest() {
      try {
        // When this test starts failing, it's time to increase Format.MaxContentLength.
        var chunk = new byte[MaxContentLength + 1];
        Assert.Fail($"Must throw {nameof(OutOfMemoryException)}");
      } catch (OutOfMemoryException) {
      }
    }

    static Task Write(ChunkWriter writer, byte[] chunk) => writer.WriteAsync(new UserData(), chunk, 0, chunk.Length);

    static byte[] Content(byte value, int count) {
      var res = new byte[count];
      for (int i = 0; i != count; ++i) res[i] = value;
      return res;
    }

    static async Task WithFile(Func<string, Task> action) {
      string fname = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
      try {
        await action.Invoke(fname);
      } finally {
        if (File.Exists(fname)) File.Delete(fname);
      }
    }
  }
}
