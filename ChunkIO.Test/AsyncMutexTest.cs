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
using System.Runtime.CompilerServices;
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

    [TestMethod]
    public void FairnessTest() {
      foreach (bool sync in new[] { false, true }) {
        var mutex = new AsyncMutex();
        var tasks = new Queue<Task>(Enumerable.Range(0, 1024).Select(_ => mutex.LockAsync()));
        while (tasks.Count > 0) {
          tasks.Enqueue(mutex.LockAsync());
          for (int i = 0; i != 2; ++i) {
            tasks.Dequeue().Wait();
            mutex.Unlock(sync);
          }
        }
      }
    }

    [TestMethod]
    public void ExampleTest() {
      var expected = new[] {
        "Worker #0: 0",
        "Worker #1: 0",
        "Worker #1: 1",
        "Worker #1: 2",
        "Worker #0: 1",
        "Worker #0: 2",
      };
      CollectionAssert.AreEqual(expected, Example().Result);

      async Task<List<string>> Example() {
        var res = new List<string>();
        var mutex = new AsyncMutex();
        await mutex.LockAsync();
        Task[] tasks = Enumerable.Range(0, 2).Select(Work).ToArray();
        mutex.Unlock(runNextSynchronously: true);
        await Task.WhenAll(tasks);
        return res;

        async Task Work(int worker) {
          for (int i = 0; i != 3; ++i) {
            await mutex.LockAsync();
            res.Add($"Worker #{worker}: {i}");
            mutex.Unlock(runNextSynchronously: true);
          }
        }
      }
    }

    // Sample run:
    //
    //   Benchmark(sync: True, threads: 1): 14.1 ns/call.
    //   Benchmark(sync: True, threads: 2): 14.5 ns/call.
    //   Benchmark(sync: True, threads: 4): 14.6 ns/call.
    //   Benchmark(sync: True, threads: 32): 15.6 ns/call.
    //   Benchmark(sync: True, threads: 1024): 14.3 ns/call.
    //
    //   Benchmark(sync: False, threads: 1): 14.5 ns/call.
    //   Benchmark(sync: False, threads: 2): 2,526.8 ns/call.
    //   Benchmark(sync: False, threads: 4): 2,809.5 ns/call.
    //   Benchmark(sync: False, threads: 32): 2,830.4 ns/call.
    //   Benchmark(sync: False, threads: 1024): 2,796.9 ns/call.
    //
    // For comparison, here's the same benchmark for SemaphoreSlim.
    //
    //   Benchmark(threads: 1): 86.3 ns/call.
    //   Benchmark(threads: 2): 105.1 ns/call.
    //   Benchmark(threads: 4): 2,478.4 ns/call.
    //   Benchmark(threads: 32): 2,502.1 ns/call.
    //   Benchmark(threads: 1024): 2,458.7 ns/call.
    //
    // Plain Monitor.Enter() + Monitor.Exit() on the same machine takes 18ns.
    [TestMethod]
    public void LockUnlockBenchmark() {
      Console.WriteLine("Warmup:");
      Benchmark(false, 32).Wait();

      Console.WriteLine("\nThe real thing:");
      foreach (bool sync in new[] { true }) {
        foreach (int threads in new[] { 1, 2, 4, 32, 1024 }) {
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
          await Task.Yield();
          while (stopwatch.Elapsed < duration) {
            for (int i = 0; i != 64; ++i) {
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
