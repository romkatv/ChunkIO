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
    public void SmokeTest() {
      var flushes = new List<bool>();
      using (var listener = RemoteFlush.CreateListener("test", Flush)) {
        Assert.IsTrue(RemoteFlush.FlushAsync("test", true).Result);
      }
      Assert.AreEqual(1, flushes.Count);
      Assert.IsTrue(flushes[0]);
      Task Flush(bool flushToDisk) {
        flushes.Add(flushToDisk);
        return Task.CompletedTask;
      }
    }
  }
}
