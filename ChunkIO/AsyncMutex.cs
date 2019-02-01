﻿// Copyright 2019 Roman Perepelitsa
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

namespace ChunkIO {

  // Similar to SemaphoreSlim(1, 1) but faster and doesn't require Dispose().
  //
  // AsyncMutex is fair. The following assertion holds:
  //
  //   async Task Fair(AsyncMutex mutex) {
  //     Task t1 = mutex.LockAsync();
  //     Task t2 = mutex.LockAsync();
  //     Debug.Assert(!t2.IsCompleted);
  //   }
  //
  // Corollary: To wait until all inflight lock + unlock requests are finished (a.k.a. flush or drain),
  // you can simply lock and unlock.
  //
  // When there is no contention, AsyncMutex is about 30% faster than System.Threading.Monitor
  // and 6 times faster than System.Threading.SemaphoreSlim (~14ns for lock + unlock with AsyncMutex
  // vs ~18ns for Monitor and ~86ns for SemaphoreSlim). When AsyncMutex is used under contention,
  // LockAsync() + Unlock(false) is about as fast as SemaphoreSlim (~2.5us).
  // LockAsync() + Unlock(true), however, takes ~14ns regardless of the number of contending
  // threads.
  //
  // The downside of Unlock(true) is that you have to be careful where you call it.
  // While Unlock(false) never reenters and always returns quickly, Unlock(true) allows the
  // current thread to be hijacked by the next task waiting for the lock. If there is heavy
  // contention, Unlock(true) can take arbitrary long time. In the worst case, the task that
  // hijacked your thread may never yield and thus Unlock(true) will never return. You also
  // need to be on lookout for reentrancy when using Unlock(true). Don't call it while holding
  // some kind of lock or having invariants temporarily broken.
  //
  // The depth of the call stack in Unlock(true) can be proportional to the number of tasks
  // contending on AsyncMutex.
  //
  // Example:
  //
  //   async Task Example() {
  //     var mutex = new AsyncMutex();
  //     await mutex.LockAsync();
  //     Task[] tasks = Enumerable.Range(0, 2).Select(Work).ToArray();
  //     mutex.Unlock(runNextSynchronously: true);
  //     await Task.WhenAll(tasks);
  //   
  //     async Task Work(int worker) {
  //       for (int i = 0; i != 3; ++i) {
  //         await mutex.LockAsync();
  //         Console.WriteLine("Worker #{0}: {1}", worker, i);
  //         mutex.Unlock(runNextSynchronously: true);
  //       }
  //     }
  //   }
  //
  // Output:
  //
  //   Worker #0: 0
  //   Worker #1: 0
  //   Worker #1: 1
  //   Worker #1: 2
  //   Worker #0: 1
  //   Worker #0: 2
  //
  // This output is deterministic. It doesn't depend on the whims of the task and thread scheduler.
  // Note how worker #1 started doing its work as soon as worker #0 unlocked the mutex. The control
  // didn't return to worker #0 until worker #1 had finished. Were worker #1 at some point to yield
  // via Task.Yield(), Task.Delay() or async IO, worker #0 would have resumed earlier.
  //
  // All lines were printed by the same thread. There was no parallelism involved and no ping-ponging
  // between threads. This is as it should be, given that all work is sandwitched between lock and
  // unlock.
  //
  // Here's how the call stack looked like when the line "Worker #1: 2" was being printed (most
  // recent call first):
  //
  //   Work(worker: 1)
  //   [Resuming Async Method]
  //   AsyncMutex.Unlock(runNextSynchronously:true)
  //   Work(worker: 0)
  //   [Resuming Async Method]	
  //   AsyncMutex.Unlock(runNextSynchronously: true)
  //   Example()
  //
  // Here it's clear why worker #0 isn't making progress. It's blocked on Unlock(), which won't
  // return until worker #2 yields or completes.
  public class AsyncMutex {
    readonly object _monitor = new object();
    readonly IntrusiveListNode<Waiter>.List _waiters = new IntrusiveListNode<Waiter>.List();

    // Possible values:
    //
    //   * 0: AsyncMutex is unlocked.
    //   * 1: AsyncMutex is locked and no one is waiting for it to be unlocked.
    //   * 2: When this value is observed while _monitor is locked, AsyncMutex is locked
    //        and there are inflight LockAsync() tasks waiting for it to be unlocked.
    int _clients = 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task LockAsync() => LockAsync(CancellationToken.None);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task LockAsync(CancellationToken cancel) {
      if (cancel.IsCancellationRequested) return Task.FromCanceled(cancel);
      if (Interlocked.CompareExchange(ref _clients, 1, 0) == 0) return Task.CompletedTask;
      return LockSlow(cancel);
    }

    // Read the class comments to understand what runNextSynchronously means.
    // The short version is that it makes no difference if there is no contention.
    // Under contention runNextSynchronously=true is much faster but also more subtle.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Unlock(bool runNextSynchronously) {
      if (Interlocked.CompareExchange(ref _clients, 0, 1) == 1) return;
      UnlockSlow(runNextSynchronously);
    }

    public bool IsLocked => Volatile.Read(ref _clients) != 0;

    [MethodImpl(MethodImplOptions.NoInlining)]
    Task LockSlow(CancellationToken cancel) {
      lock (_monitor) {
        if (Interlocked.Exchange(ref _clients, 2) == 0) {
          Debug.Assert(_waiters.First == null);
          Volatile.Write(ref _clients, 1);
          return Task.CompletedTask;
        }

        var waiter = new Waiter() { Mutex = this };
        // It's important for performance for the lambda to have no capture.
        Task res = waiter.Task = new Task((object state) => {
          var w = (Waiter)state;
          Debug.Assert(w.Task == null);
          if (w.Cancelled) throw new TaskCanceledException(nameof(LockAsync));
        }, waiter);
        _waiters.AddLast(waiter);

        // It's important for performance for the lambda to have no capture.
        waiter.CancelReg = cancel.Register((object state) => {
          var w = (Waiter)state;
          // Note that this code can execute synchronously from cancel.Register().
          // If this happens, _monitor is locked here, but task.RunSynchronously() is
          // still safe to call because the task cannot have continuations.
          w.Mutex.FinishWaiter(w, runSynchronously: true);
        }, waiter);

        return res;
      }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void UnlockSlow(bool runNextSynchronously) {
      FinishWaiter(null, runNextSynchronously);
    }

    // When called from Unlock(), waiter is null. When called due to the cancellation of
    // LockAsync() task, waiter is the object associated with the task.
    void FinishWaiter(Waiter waiter, bool runSynchronously) {
      Task task;

      lock (_monitor) {
        if (waiter == null) {
          if (_clients == 1) {
            Debug.Assert(_waiters.First == null);
            Volatile.Write(ref _clients, 0);
            return;
          }
          Debug.Assert(_waiters.First != null);
          waiter = _waiters.First;
        } else {
          if (waiter.Task == null) return;
          waiter.Cancelled = true;
        }
        Debug.Assert(_clients == 2);
        task = waiter.Task;
        waiter.Task = null;
        _waiters.Remove(waiter);
        if (_waiters.First == null) Volatile.Write(ref _clients, 1);
      }

      waiter.CancelReg.Dispose();
      if (runSynchronously) {
        task.RunSynchronously();
      } else {
        task.Start();
      }
    }

    // Task and Cancelled are protected by _monitor. Only these two fields can be
    // modified after initialization.
    class Waiter : IntrusiveListNode<Waiter> {
      public AsyncMutex Mutex { get; set; }
      public Task Task { get; set; }
      public bool Cancelled { get; set; }
      public CancellationTokenRegistration CancelReg { get; set; }
    }
  }
}
