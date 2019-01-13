﻿using System;
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
        var bytes = System.Text.Encoding.UTF8.GetBytes("<Hello>!");
        writer.Write(bytes, 1, bytes.Length - 3);
        Assert.AreEqual(5, writer.Position);
        writer.Flush(false).Wait();
        Assert.AreEqual("Hello", ReadFile());
      }
      Assert.AreEqual("Hello", ReadFile());
    }

    [TestMethod]
    public void TwoWrites() {
      using (var writer = new ByteWriter(_filepath)) {
        void Write(string s) {
          var bytes = System.Text.Encoding.UTF8.GetBytes(s);
          writer.Write(bytes, 0, bytes.Length);
          writer.Flush(false).Wait();
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
        var bytes = System.Text.Encoding.UTF8.GetBytes("Goodbye");
        writer.Write(bytes, 0, bytes.Length);
        Assert.AreEqual(12, writer.Position);
        writer.Flush(false).Wait();
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
        return System.Text.Encoding.UTF8.GetString(bytes);
      }
    }
  }
}
