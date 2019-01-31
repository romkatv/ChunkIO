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
    static readonly Action DoNothing = delegate { };

    readonly ActionChain<State, bool> _chain = new ActionChain<State, bool>();
    readonly object _monitor = new object();
    readonly LinkedList<Task> _waiters = new LinkedList<Task>();
    bool _locked = false;

    public Task LockAsync() => LockAsync(CancellationToken.None);

    struct State {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public State(AsyncMutex mutex, object extra) {
        Mutex = mutex;
        Extra = extra;
      }
      public AsyncMutex Mutex { get; }
      public object Extra { get; }
    }

    class Extra2 {
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public Extra2(CancellationToken cancel) {
        Cancel = cancel;
        Task = new Task(End, this);
      }

      public Task Task { get; }
      public bool Cancelled { get; set; }
      public CancellationToken Cancel { get; }
      public LinkedListNode<Task> Node { get; set; }
      public AsyncMutex Mutex { get; set; }

      static readonly Action<object> End = (object extra) => {
        if (((Extra2)extra).Cancelled) throw new TaskCanceledException(nameof(LockAsync));
      };
    }

    static readonly Func<bool, State, bool> Lock1 = (bool sync, State state) => {
      if (!sync || state.Mutex._locked) return false;
      Debug.Assert(state.Mutex._waiters.Count == 0);
      state.Mutex._locked = true;
      return true;
    };

    static readonly Func<bool, State, bool> Lock2 = (bool sync, State state) => {
      var extra = (Extra2)state.Extra;
      if (!state.Mutex._locked) {
        Debug.Assert(state.Mutex._waiters.Count == 0);
        state.Mutex._locked = true;
        extra.Task.Start();
      } else {
        extra.Mutex = state.Mutex;
        extra.Node = state.Mutex._waiters.AddLast(new Task(DoNothing, extra.Cancel));
        extra.Node.Value.ContinueWith(Lock3, extra);
      }
      return false;
    };

    static readonly Action<Task, object> Lock3 = (Task t, object extra) => {
      var e = (Extra2)extra;
      e.Mutex._chain.Add(Lock4, new State(e.Mutex, extra), out bool ret);
    };

    static readonly Func<bool, State, bool> Lock4 = (bool sync, State state) => {
      var extra = (Extra2)state.Extra;
      if (extra.Node.List == null) {
        Debug.Assert(state.Mutex._locked);
      } else {
        Debug.Assert(extra.Node.Value.IsCanceled);
        state.Mutex._waiters.Remove(extra.Node);
        extra.Cancelled = true;
      }
      extra.Task.Start();
      return false;
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task LockAsync(CancellationToken cancel) {
      {
        var state = new State(this, null);
        _chain.Add(Lock1, state, out bool ret);
        if (ret) return Task.CompletedTask;
      }

      {
        var extra = new Extra2(cancel);
        var state = new State(this, extra);
        _chain.Add(Lock2, state, out bool ret);
        return extra.Task;
      }
      
      LinkedListNode<Task> node;
      lock (_monitor) {
        if (!_locked) {
          Debug.Assert(_waiters.Count == 0);
          _locked = true;
          return Task.CompletedTask;
        }
        node = _waiters.AddLast(new Task(delegate { }, cancel));
      }
      return node.Value.ContinueWith(_ => {
        lock (_monitor) {
          if (node.List == null) {
            Debug.Assert(_locked);
          } else {
            Debug.Assert(node.Value.IsCanceled);
            _waiters.Remove(node);
            throw new TaskCanceledException(nameof(LockAsync));
          }
        }
      });
    }

    static readonly Func<bool, State, bool> Unlock1 = (bool sync, State state) => {
      Debug.Assert(state.Mutex._locked);
      if (state.Mutex._waiters.Count == 0) {
        state.Mutex._locked = false;
      } else {
        Task next = state.Mutex._waiters.First.Value;
        state.Mutex._waiters.RemoveFirst();
        // Task.Start() has weird semantics when it comes to the handling of concurrent cancellations.
        // Consider the following function:
        //
        //   void Foo(CancellationToken c) {
        //     Task t = new Task(delegate { }, c);
        //     t.Start();
        //   }
        //
        // Here `t.Start()` may or may not throw InvalidOperationException. Its logic is roughly
        // as follows:
        //
        //   lock (t._monitor) {
        //     if (t._cancelled || t._started) throw InvalidOperationException();
        //   }
        //
        //   lock (t._monitor) {
        //     if (t._cancelled || t._started) return;
        //     t._started = true;
        //   }
        //
        //   ActuallyRun();
        //
        // A sane person would write it like this instead:
        //
        //   lock (t._monitor) {
        //     if (t._started) throw InvalidOperationException();
        //     if (t._cancelled) return;
        //     t._started = true;
        //   }
        //
        //   ActuallyRun();
        //
        // Surprisingly enough, Task.Run(delegate { }, c) don't have this problem even though one
        // would expect it to be equivalent to our implementation of Foo().
        //
        // To work around this problem, we catch and ignore InvalidOperationException.
        try {
          next.Start();
        } catch (InvalidOperationException) {
          Debug.Assert(next.IsCanceled);
        }
      }
      return false;
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Unlock() {
      _chain.Add(Unlock1, new State(this, null), out bool ret);
      return;

      Task next;
      lock (_monitor) {
        Debug.Assert(_locked);
        if (_waiters.Count == 0) {
          _locked = false;
          return;
        }
        next = _waiters.First.Value;
        _waiters.RemoveFirst();
      }
      // Task.Start() has weird semantics when it comes to the handling of concurrent cancellations.
      // Consider the following function:
      //
      //   void Foo(CancellationToken c) {
      //     Task t = new Task(delegate { }, c);
      //     t.Start();
      //   }
      //
      // Here `t.Start()` may or may not throw InvalidOperationException. Its logic is roughly
      // as follows:
      //
      //   lock (t._monitor) {
      //     if (t._cancelled || t._started) throw InvalidOperationException();
      //   }
      //
      //   lock (t._monitor) {
      //     if (t._cancelled || t._started) return;
      //     t._started = true;
      //   }
      //
      //   ActuallyRun();
      //
      // A sane person would write it like this instead:
      //
      //   lock (t._monitor) {
      //     if (t._started) throw InvalidOperationException();
      //     if (t._cancelled) return;
      //     t._started = true;
      //   }
      //
      //   ActuallyRun();
      //
      // Surprisingly enough, Task.Run(delegate { }, c) don't have this problem even though one
      // would expect it to be equivalent to our implementation of Foo().
      //
      // To work around this problem, we catch and ignore InvalidOperationException.
      try {
        next.Start();
      } catch (InvalidOperationException) {
        Debug.Assert(next.IsCanceled);
      }
    }

    public bool IsLocked {
      get {
        lock (_monitor) return _locked;
      }
    }

    public async Task DrainAsync() {
      await LockAsync();
      Unlock();
    }
  }

  static class AsyncMutexExtensions {
    public static Task WithLock(this AsyncMutex mutex, Func<Task> action) =>
        WithLock(mutex, CancellationToken.None, action);

    public static Task<T> WithLock<T>(this AsyncMutex mutex, Func<Task<T>> action) =>
        WithLock(mutex, CancellationToken.None, action);

    public static async Task WithLock(this AsyncMutex mutex, CancellationToken cancel, Func<Task> action) {
      Debug.Assert(mutex != null && action != null);
      await mutex.LockAsync(cancel);
      try {
        await action.Invoke();
      } finally {
        mutex.Unlock();
      }
    }

    public static async Task<T> WithLock<T>(this AsyncMutex mutex, CancellationToken cancel, Func<Task<T>> action) {
      Debug.Assert(mutex != null && action != null);
      await mutex.LockAsync(cancel);
      try {
        return await action.Invoke();
      } finally {
        mutex.Unlock();
      }
    }
  }
}
