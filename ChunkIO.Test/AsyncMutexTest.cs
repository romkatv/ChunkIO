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
      foreach (bool sync in new[] { false, true }) {
        const int N = 64 << 10;
        long counter = 0;
        var mutex = new AsyncMutex();
        Task.WhenAll(Enumerable.Range(0, N).Select(_ => Inc())).Wait();
        Assert.AreEqual(N, counter);

        async Task Inc() {
          await mutex.LockAsync();
          await Task.Yield();
          long next = counter + 1;
          await Task.Yield();
          counter = next;
          mutex.Unlock(runNextSynchronously: sync);
        }
      }
    }

    [TestMethod]
    public void CancelTest() {
      foreach (bool sync in new[] { false, true }) {
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
          mutex.Unlock(runNextSynchronously: sync);
        }
      }
    }

    // Sample run:
    //
    //   Benchmark(sync: False, threads: 1): 37.0 ns/call.
    //   Benchmark(sync: False, threads: 2): 68.1 ns/call.
    //   Benchmark(sync: False, threads: 4): 2,708.0 ns/call.
    //   Benchmark(sync: False, threads: 8): 2,699.1 ns/call.
    //   Benchmark(sync: False, threads: 16): 2,743.3 ns/call.
    //   Benchmark(sync: False, threads: 32): 2,651.1 ns/call.
    //
    //   Benchmark(sync: True, threads: 1): 36.4 ns/call.
    //   Benchmark(sync: True, threads: 2): 35.6 ns/call.
    //   Benchmark(sync: True, threads: 4): 39.1 ns/call.
    //   Benchmark(sync: True, threads: 8): 37.9 ns/call.
    //   Benchmark(sync: True, threads: 16): 41.5 ns/call.
    //   Benchmark(sync: True, threads: 32): 41.2 ns/call.
    //
    // For comparison, here's the same benchmark for SemaphoreSlim.
    //
    //   Benchmark(threads: 1): 87.0 ns/call.
    //   Benchmark(threads: 2): 96.8 ns/call.
    //   Benchmark(threads: 4): 2,493.4 ns/call.
    //   Benchmark(threads: 8): 2,443.4 ns/call.
    //   Benchmark(threads: 16): 2,455.1 ns/call.
    //   Benchmark(threads: 32): 2,407.6 ns/call.
    [TestMethod]
    public void LockUnlockBenchmark() {
      Console.WriteLine("Warmup:");
      Benchmark(false, 32).Wait();

      Console.WriteLine("\nThe real thing:");
      foreach (bool sync in new[] { false, true }) {
        foreach (int threads in new[] { 1, 2, 4, 8, 16, 32 }) {
          Benchmark(sync, threads).Wait();
        }
      }

      async Task Benchmark(bool sync, int threads) {
        long counter = 0;
        var mutex = new AsyncMutex();
        TimeSpan duration = TimeSpan.FromSeconds(1);
        Stopwatch stopwatch = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, threads).Select(_ => Inc()));
        double ns = 1e9 * stopwatch.Elapsed.TotalSeconds / counter;
        Console.WriteLine("  Benchmark(sync: {0}, threads: {1}): {2:N1} ns/call.", sync, threads, ns);

        async Task Inc() {
          while (stopwatch.Elapsed < duration) {
            await Task.Yield();
            for (int i = 0; i != 1024; ++i) {
              await mutex.LockAsync();
              ++counter;
              mutex.Unlock(runNextSynchronously: sync);
            }
          }
        }
      }
    }
  }
}
