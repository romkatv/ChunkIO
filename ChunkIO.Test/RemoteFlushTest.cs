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
