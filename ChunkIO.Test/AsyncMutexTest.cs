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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ChunkIO.Test {
  [TestClass]
  public class AsyncMutexTest {
    [TestMethod]
    public void LockUnlockTest() {
      foreach (bool drain in new[] { false, true }) {
        const int N = 64 << 10;
        long counter = 0;
        var mutex = new AsyncMutex();
        Task[] tasks = Enumerable.Range(0, N).Select(_ => Inc()).ToArray();
        if (drain) {
          mutex.DrainAsync().Wait();
        } else {
          Task.WaitAll(tasks);
        }
        Assert.AreEqual(N, counter);

        async Task Inc() {
          await mutex.LockAsync();
          await Task.Yield();
          long next = counter + 1;
          await Task.Yield();
          counter = next;
          mutex.Unlock();
        }
      }
    }

    [TestMethod]
    public void CancelTest() {
      const int N = 64;
      long counter = 0;
      var mutex = new AsyncMutex();
      Task[] tasks = Enumerable.Range(0, N).Select(_ => Inc()).ToArray();
      Task.WaitAll(tasks);
      Assert.AreEqual(N, counter);

      async Task Inc() {
        while (true) {
          using (var cancel = new CancellationTokenSource()) {
            Task t = mutex.LockAsync(cancel.Token);
            await Task.Yield();
            cancel.Cancel();
            try {
              await t;
              break;
            } catch (TaskCanceledException) {
            }
          }
        }
        long next = counter + 1;
        await Task.Yield();
        counter = next;
        mutex.Unlock();
      }
    }

    [TestMethod]
    public void Benchmark0() {
      Benchmark(1).Wait();
    }

    static async Task Benchmark(int parallelism) {
      long counter = 0;
      var mutex = new AsyncMutex();
      TimeSpan duration = TimeSpan.FromSeconds(5);
      Stopwatch stopwatch = Stopwatch.StartNew();
      await Task.WhenAll(Enumerable.Range(0, parallelism).Select(_ => Inc()));
      double seconds = stopwatch.Elapsed.TotalSeconds;
      Console.Write("Benchmark(parallelism: {0}): {1:N0} calls/sec.", parallelism, counter / seconds);

      async Task Inc() {
        while (stopwatch.Elapsed < duration) {
          for (int i = 0; i != 256; ++i) {
            await mutex.LockAsync();
            ++counter;
            mutex.Unlock();
          }
        }
      }
    }
  }
}
