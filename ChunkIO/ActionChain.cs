using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  // Wait-free queue of actions. Can be used as a scalable alternative to locking
  // that doesn't suffer from contention.
  public class ActionChain<T, U> {
    [ThreadStatic] static Work ts_cache;
    Work _tail;

    public ActionChain() {
      _tail = new Work();
      _tail.Init(delegate { return default(U); }, default(T));
      Work.Run(_tail, out U ret);
    }

    // Either executes `action` synchronously (in which case some other actions added
    // concurrently by other threads may also run synchronously) or schedules it for
    // execution after all previously scheduled actions have completed.
    //
    // Actions are guaranteed to run in the same order they were added.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Add(Func<bool, T, U> f, T arg, out U ret) {
      Debug.Assert(f != null);
      Work work = ts_cache ?? new Work();
      work.Init(f, arg);
      Work tail = Interlocked.Exchange(ref _tail, work);
      if (tail.ContinueWith(work)) {
        Work.Run(work, out ret);
        ts_cache = tail;
        return true;
      } else {
        ret = default(U);
        ts_cache = null;
        return false;
      }
    }

    class Work {
      static readonly Work SEALED = new Work();

      Func<bool, T, U> _f;
      T _arg;
      Work _next;

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public void Init(Func<bool, T, U> f, T arg) {
        _f = f;
        _arg = arg;
        _next = null;
      }

      // Called at most once.
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public bool ContinueWith(Work next) {
        // Invariant: _next is either null or SEALED.
        return Volatile.Read(ref _next) != null || Interlocked.Exchange(ref _next, next) != null;
      }

      // Called at most once.
      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public static void Run(Work w, out U ret) {
        ret = w._f.Invoke(true, w._arg);
        while ((w = Interlocked.Exchange(ref w._next, SEALED)) != null) {
          w._f.Invoke(false, w._arg);
        }
      }
    }
  }
}
