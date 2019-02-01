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

namespace ChunkIO {

  // Similar to SemaphoreSlim(1, 1) but faster and doesn't require Dispose().
  //
  // AsyncMutex is fair in the following sense:
  //
  //   async Task Fair(AsyncMutex mutex) {
  //     Task t1 = mutex.LockAsync();
  //     Task t2 = mutex.LockAsync();
  //     await t2;
  //     Debug.Assert(t1.IsCompleted);
  //   }
  public class AsyncMutex {
    readonly object _monitor = new object();
    readonly IntrusiveListNode<Waiter>.List _waiters = new IntrusiveListNode<Waiter>.List();
    bool _locked = false;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task LockAsync() => LockAsync(CancellationToken.None);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task LockAsync(CancellationToken cancel) {
      if (cancel.IsCancellationRequested) return Task.FromCanceled(cancel);
      Monitor.Enter(_monitor);
      if (!_locked) {
        Debug.Assert(_waiters.First == null);
        _locked = true;
        Monitor.Exit(_monitor);
        return Task.CompletedTask;
      }
      return LockSlow(cancel);
    }

    // If there are queued LockAsync() tasks and runNextSynchronously is true, runs the
    // next task synchronously. If runNextSynchronously is false, schedules the task
    // on the default task scheduler.
    //
    // When AsyncMutex is used under contention, Unlock(true) is about two orders of magnitude
    // faster. See LockUnlockBenchmark. The downside is that that you have to be careful where
    // you call Unlock(true).
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Unlock(bool runNextSynchronously) {
      Monitor.Enter(_monitor);
      Debug.Assert(_locked);
      if (_waiters.First == null) {
        _locked = false;
        Monitor.Exit(_monitor);
        return;
      }
      UnlockSlow(runNextSynchronously);
    }

    public bool IsLocked {
      get {
        lock (_monitor) return _locked;
      }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    Task LockSlow(CancellationToken cancel) {
      var waiter = new Waiter() { Mutex = this };
      // It's important for performance for the lambda to have no capture.
      Task res = waiter.Task = new Task((object state) => {
        if (((Waiter)state).Cancelled) throw new TaskCanceledException(nameof(LockAsync));
      }, waiter);
      _waiters.AddLast(waiter);
      // It's important for performance for the lambda to have no capture.
      waiter.CancelReg = cancel.Register((object state) => {
        Task task;
        var w = (Waiter)state;
        AsyncMutex m = w.Mutex;
        lock (m._monitor) {
          if (w.Task == null) return;
          task = w.Task;
          w.Task = null;
          w.Cancelled = true;
          m._waiters.Remove(w);
        }
        w.CancelReg.Dispose();
        // Note that this code can execute synchronously from cancel.Register().
        // If this happens, _monitor is locked here, but task.RunSynchronously() is
        // still safe to call because the task cannot have continuations.
        task.RunSynchronously();
      }, waiter);
      Monitor.Exit(_monitor);
      return res;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void UnlockSlow(bool runNextSynchronously) {
      Waiter waiter = _waiters.First;
      Task task = waiter.Task;
      waiter.Task = null;
      _waiters.Remove(waiter);
      Monitor.Exit(_monitor);
      waiter.CancelReg.Dispose();
      if (runNextSynchronously) {
        task.RunSynchronously();
      } else {
        task.Start();
      }
    }

    class Waiter : IntrusiveListNode<Waiter> {
      public AsyncMutex Mutex { get; set; }
      public Task Task { get; set; }
      public bool Cancelled { get; set; }
      public CancellationTokenRegistration CancelReg { get; set; }
    }
  }
}
