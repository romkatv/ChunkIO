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
  [TestClass]
  public class ByteWriterTest {
    readonly string _filepath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    [TestCleanup()]
    public void Cleanup() {
      if (File.Exists(_filepath)) File.Delete(_filepath);
    }

    [TestMethod]
    public void NoWrites() {
      using (var writer = new ByteWriter(_filepath)) {
        Assert.AreEqual(0, writer.Position);
        Assert.AreEqual(_filepath, writer.Name);
        Assert.AreEqual("", ReadFile());
      }
      Assert.AreEqual("", ReadFile());
    }

    [TestMethod]
    public void OneWrite() {
      using (var writer = new ByteWriter(_filepath)) {
        var bytes = Encoding.UTF8.GetBytes("<Hello>!");
        writer.WriteAsync(bytes, 1, bytes.Length - 3);
        Assert.AreEqual(5, writer.Position);
        writer.FlushAsync(false).Wait();
        Assert.AreEqual("Hello", ReadFile());
      }
      Assert.AreEqual("Hello", ReadFile());
    }

    [TestMethod]
    public void TwoWrites() {
      using (var writer = new ByteWriter(_filepath)) {
        void Write(string s) {
          var bytes = Encoding.UTF8.GetBytes(s);
          writer.WriteAsync(bytes, 0, bytes.Length);
          writer.FlushAsync(false).Wait();
        }
        Write("Hello");
        Write("Goodbye");
        Assert.AreEqual(12, writer.Position);
        Assert.AreEqual("HelloGoodbye", ReadFile());
      }
      Assert.AreEqual("HelloGoodbye", ReadFile());
    }

    [TestMethod]
    public void ExistingFile() {
      File.WriteAllText(_filepath, "Hello");
      using (var writer = new ByteWriter(_filepath)) {
        Assert.AreEqual(5, writer.Position);
        var bytes = Encoding.UTF8.GetBytes("Goodbye");
        writer.WriteAsync(bytes, 0, bytes.Length);
        Assert.AreEqual(12, writer.Position);
        writer.FlushAsync(false).Wait();
        Assert.AreEqual("HelloGoodbye", ReadFile());
      }
      Assert.AreEqual("HelloGoodbye", ReadFile());
    }

    [TestMethod]
    public void RelativeName() {
      Directory.SetCurrentDirectory(Path.GetTempPath());
      using (var writer = new ByteWriter(new FileInfo(_filepath).Name)) {
        Assert.AreEqual(writer.Name, _filepath);
        Assert.AreEqual(ReadFile(), "");
      }
      Assert.AreEqual(ReadFile(), "");
    }

    string ReadFile() {
      if (!File.Exists(_filepath)) return null;
      using (var file = new FileStream(_filepath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)) {
        var bytes = new byte[file.Length];
        Assert.AreEqual(file.Read(bytes, 0, bytes.Length), bytes.Length);
        return Encoding.UTF8.GetString(bytes);
      }
    }
  }
}
