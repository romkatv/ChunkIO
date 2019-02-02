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
    public void LockUnlockStressTest() {
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
    public void CancelStressTest() {
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
              if (!t.IsCanceled) {
                await t;
                break;
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
    public void CancelTest() {
      var mutex = new AsyncMutex();
      using (var cancel = new CancellationTokenSource()) {
        mutex.LockAsync().Wait();
        Task t = mutex.LockAsync(cancel.Token);
        Assert.AreEqual(TaskStatus.Created, t.Status);
        mutex.Unlock(runNextSynchronously: true);
        t.Wait();

        t = mutex.LockAsync(cancel.Token);
        cancel.Cancel();
        Assert.AreEqual(TaskStatus.Canceled, t.Status);

        t = mutex.LockAsync(cancel.Token);
        Assert.AreEqual(TaskStatus.Canceled, t.Status);
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
    //   Benchmark(runNextSynchronously:  True, cancelable: False, threads:     1):    14.5 ns/call
    //   Benchmark(runNextSynchronously:  True, cancelable: False, threads:     2):    15.1 ns/call
    //   Benchmark(runNextSynchronously:  True, cancelable: False, threads:     4):    14.5 ns/call
    //   Benchmark(runNextSynchronously:  True, cancelable: False, threads:    32):    15.1 ns/call
    //   Benchmark(runNextSynchronously:  True, cancelable: False, threads: 1,024):    14.6 ns/call
    //
    //   Benchmark(runNextSynchronously:  True, cancelable:  True, threads:     1):    15.1 ns/call
    //   Benchmark(runNextSynchronously:  True, cancelable:  True, threads:     2):    14.8 ns/call
    //   Benchmark(runNextSynchronously:  True, cancelable:  True, threads:     4):    14.8 ns/call
    //   Benchmark(runNextSynchronously:  True, cancelable:  True, threads:    32):    15.5 ns/call
    //   Benchmark(runNextSynchronously:  True, cancelable:  True, threads: 1,024):    14.8 ns/call
    //
    //   Benchmark(runNextSynchronously: False, cancelable: False, threads:     1):    14.5 ns/call
    //   Benchmark(runNextSynchronously: False, cancelable: False, threads:     2): 1,689.6 ns/call
    //   Benchmark(runNextSynchronously: False, cancelable: False, threads:     4): 2,231.2 ns/call
    //   Benchmark(runNextSynchronously: False, cancelable: False, threads:    32): 2,229.8 ns/call
    //   Benchmark(runNextSynchronously: False, cancelable: False, threads: 1,024): 2,220.2 ns/call
    //
    //   Benchmark(runNextSynchronously: False, cancelable:  True, threads:     1):    15.1 ns/call
    //   Benchmark(runNextSynchronously: False, cancelable:  True, threads:     2):   171.5 ns/call
    //   Benchmark(runNextSynchronously: False, cancelable:  True, threads:     4): 2,893.5 ns/call
    //   Benchmark(runNextSynchronously: False, cancelable:  True, threads:    32): 3,009.3 ns/call
    //   Benchmark(runNextSynchronously: False, cancelable:  True, threads: 1,024): 3,046.2 ns/call
    //
    // For comparison, here's the same benchmark for SemaphoreSlim(1, 1):
    //
    //   Benchmark(cancelable: False, threads:     1):    86.1 ns/call
    //   Benchmark(cancelable: False, threads:     2):   121.2 ns/call
    //   Benchmark(cancelable: False, threads:     4): 2,394.1 ns/call
    //   Benchmark(cancelable: False, threads:    32): 2,467.8 ns/call
    //   Benchmark(cancelable: False, threads: 1,024): 2,391.4 ns/call
    //   Benchmark(cancelable:  True, threads:     1):    86.9 ns/call
    //   Benchmark(cancelable:  True, threads:     2): 1,666.1 ns/call
    //   Benchmark(cancelable:  True, threads:     4): 8,956.5 ns/call
    //   Benchmark(cancelable:  True, threads:    32): 9,223.9 ns/call
    //   Benchmark(cancelable:  True, threads: 1,024):  11,011 ns/call
    //
    // Plain Monitor.Enter() + Monitor.Exit() on the same machine takes 18ns.
    [TestMethod]
    public void LockUnlockBenchmark() {
      using (var cancel = new CancellationTokenSource()) {
        Console.WriteLine("Warmup:");
        Benchmark(runNextSynchronously: false, cancelable: false, threads: 1024).Wait();
        Benchmark(runNextSynchronously: false, cancelable: true, threads: 1024).Wait();
        Benchmark(runNextSynchronously: true, cancelable: false, threads: 1024).Wait();
        Benchmark(runNextSynchronously: true, cancelable: true, threads: 1024).Wait();

        Console.WriteLine("\nThe real thing:");
        foreach (bool sync in new[] { true, false }) {
          foreach (bool c in new[] { false, true }) {
            foreach (int threads in new[] { 1, 2, 4, 32, 1024 }) {
              Benchmark(runNextSynchronously: sync, cancelable: c, threads: threads).Wait();
            }
          }
        }

        async Task Benchmark(bool runNextSynchronously, bool cancelable, int threads) {
          long counter = 0;
          var mutex = new AsyncMutex();
          TimeSpan duration = TimeSpan.FromSeconds(1);
          Stopwatch stopwatch = Stopwatch.StartNew();
          await Task.WhenAll(Enumerable.Range(0, threads).Select(_ => Inc()));
          double ns = 1e9 * stopwatch.Elapsed.TotalSeconds / counter;
          Console.WriteLine(
              "  Benchmark(runNextSynchronously: {0,5}, cancelable: {1,5}, threads: {2,5:N0}): {3,7:N1} ns/call",
              runNextSynchronously, cancelable, threads, ns);

          async Task Inc() {
            await Task.Yield();
            while (stopwatch.Elapsed < duration) {
              if (cancelable) {
                for (int i = 0; i != 64; ++i) {
                  await mutex.LockAsync(cancel.Token);
                  ++counter;
                  mutex.Unlock(runNextSynchronously: runNextSynchronously);
                }
              } else {
                for (int i = 0; i != 64; ++i) {
                  await mutex.LockAsync();
                  ++counter;
                  mutex.Unlock(runNextSynchronously: runNextSynchronously);
                }
              }
            }
          }
        }
      }
    }

    // Sample run:
    //
    //   Benchmark(threads:     1):   849.7 ns/call
    //   Benchmark(threads:     2):   931.2 ns/call
    //   Benchmark(threads:     4):   946.4 ns/call
    //   Benchmark(threads:    32): 1,976.9 ns/call
    //   Benchmark(threads: 1,024): 1,418.4 ns/call
    //
    // For comparison, here's the same benchmark for SemaphoreSlim(1, 1):
    //
    //   Benchmark(threads:     1): 16,710.7 ns/call
    //   Benchmark(threads:     2): 20,892.6 ns/call
    //   Benchmark(threads:     4): 25,353.9 ns/call
    //   Benchmark(threads:    32): 30,765.6 ns/call
    //   Benchmark(threads: 1,024): 35,038.7 ns/call
    [TestMethod]
    public void LockCancelBenchmark() {
      Console.WriteLine("Warmup:");
      Benchmark(32).Wait();

      Console.WriteLine("\nThe real thing:");
      foreach (int threads in new[] { 1, 2, 4, 32, 1024 }) {
        Benchmark(threads).Wait();
      }

      async Task Benchmark(int threads) {
        long counter = 0;
        var mutex = new AsyncMutex();
        await mutex.LockAsync();
        TimeSpan duration = TimeSpan.FromSeconds(1);
        Stopwatch stopwatch = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, threads).Select(_ => Run()));
        double ns = 1e9 * stopwatch.Elapsed.TotalSeconds / counter;
        Console.WriteLine("  Benchmark(threads: {0,5:N0}): {1,7:N1} ns/call", threads, ns);

        async Task Run() {
          await Task.Yield();
          while (stopwatch.Elapsed < duration) {
            for (int i = 0; i != 64; ++i) {
              using (var c = new CancellationTokenSource()) {
                Task t = mutex.LockAsync(c.Token);
                c.Cancel();
                Debug.Assert(t.IsCanceled);
              }
              ++counter;
            }
          }
        }
      }
    }
  }
}
