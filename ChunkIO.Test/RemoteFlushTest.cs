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
using System.IO.Pipes;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ChunkIO.Test {
  [TestClass]
  public class RemoteFlushTest {
    [TestMethod]
    public void SuccessTest() {
      var flushes = new List<bool>();
      Task<long> Flush(bool flushToDisk) {
        flushes.Add(flushToDisk);
        return Task.FromResult<long>(42);
      }
      using (var listener = RemoteFlush.CreateListener(Id("test"), Flush)) {
        Assert.AreEqual(42, RemoteFlush.FlushAsync(Id("test"), true).Result);
        Assert.AreEqual(42, RemoteFlush.FlushAsync(Id("test"), false).Result);
      }
      CollectionAssert.AreEqual(new bool[] { true, false }, flushes);
    }

    [TestMethod]
    public void FailureTest() {
      Test().Wait();
      async Task Test() {
        Task<long> Flush(bool flushToDisk) => throw new NotImplementedException();
        using (var listener = RemoteFlush.CreateListener(Id("test"), Flush)) {
          try {
            await RemoteFlush.FlushAsync(Id("test"), true);
            Assert.Fail("Must throw");
          } catch (IOException) {
          }
        }
      }
    }

    [TestMethod]
    public void NoListenerTest() {
      var flushes = new List<bool>();
      Assert.AreEqual(new long?(), RemoteFlush.FlushAsync(Id("test"), true).Result);
    }

    [TestMethod]
    public void ManyFlushesOneFileTest() {
      async Task Flush() {
        Assert.AreEqual(42, await RemoteFlush.FlushAsync(Id("test"), false));
      }
      using (var listener = RemoteFlush.CreateListener(Id("test"), _ => Task.FromResult<long>(42))) {
        Task.WhenAll(Enumerable.Range(0, 2048).Select(_ => Flush())).Wait();
      }
    }

    [TestMethod]
    public void ManyFlushesManyFilesTest() {
      async Task Flush(int n) {
        Assert.AreEqual(n, await RemoteFlush.FlushAsync(Id(n.ToString()), false));
      }
      var listeners = new List<IDisposable>();
      try {
        for (int i = 0; i != 2048; ++i) {
          int n = i;
          listeners.Add(RemoteFlush.CreateListener(Id(n.ToString()), _ => Task.FromResult<long>(n)));
        }
        Task.WhenAll(Enumerable.Range(0, listeners.Count).Select(Flush)).Wait();
      } finally {
        foreach (IDisposable x in listeners) x.Dispose();
      }
    }

    static byte[] Id(string s) => Encoding.UTF8.GetBytes(s);
  }
}
