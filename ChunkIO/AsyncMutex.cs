using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
  class AsyncMutex {
    readonly object _monitor = new object();
    readonly LinkedList<Task> _waiters = new LinkedList<Task>();
    bool _locked = false;

    public Task LockAsync() => LockAsync(CancellationToken.None);

    public Task LockAsync(CancellationToken cancel) {
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

    public void Unlock() {
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
