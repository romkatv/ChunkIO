using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  // TODO: Remove this file once C# 8 is out.
  public interface IAsyncEnumerator<out T> : IDisposable {
    T Current { get; }
    Task<bool> MoveNextAsync(CancellationToken cancel);
    void Reset();
  }

  public interface IAsyncEnumerable<out T> {
    IAsyncEnumerator<T> GetAsyncEnumerator();
  }

  public static class AsyncEnumerableExtensions {
    // Note: I couldn't figure out whether C# 8 has a method like this and what it is called.
    public static IEnumerable<T> Sync<T>(this IAsyncEnumerable<T> col) {
      using (IAsyncEnumerator<T> iter = col.GetAsyncEnumerator()) {
        while (iter.MoveNextAsync(CancellationToken.None).Result) yield return iter.Current;
      }
    }

    public static async Task ForEachAsync<T>(this IAsyncEnumerable<T> col, Func<T, Task> f) {
      using (IAsyncEnumerator<T> iter = col.GetAsyncEnumerator()) {
        while (await iter.MoveNextAsync(CancellationToken.None)) await f.Invoke(iter.Current);
      }
    }
  }

  interface IYield<T> {
    Task ReturnAsync(T val);
  }

  class AsyncEnumerable<T> : IAsyncEnumerable<T> {
    readonly Func<IYield<T>, Task> _generator;

    public AsyncEnumerable(Func<IYield<T>, Task> generator) {
      _generator = generator ?? throw new ArgumentNullException(nameof(generator));
    }

    public IAsyncEnumerator<T> GetAsyncEnumerator() => new AsyncEnumerator(_generator);

    class AsyncEnumerator : IAsyncEnumerator<T>, IYield<T> {
      readonly Func<IYield<T>, Task> _generator;
      Exception _exception = null;
      Task _produced = null;
      Task _consumed = null;
      bool _done = false;

      public AsyncEnumerator(Func<IYield<T>, Task> generator) {
        _generator = generator;
      }

      public T Current { get; internal set; }

      public async Task<bool> MoveNextAsync(CancellationToken cancel) {
        if (_produced is null) {
          _produced = new Task(delegate { });
          Task _ = _generator.Invoke(this).ContinueWith(t => {
            if (t.IsFaulted) _exception = t.Exception;
            _done = true;
            _produced.Start();
          });
        } else {
          _produced = new Task(delegate { });
          _consumed.Start();
        }
        await _produced;
        if (_exception != null) throw _exception;
        return !_done;
      }

      public void Reset() => throw new NotImplementedException();

      public Task ReturnAsync(T val) {
        Current = val;
        _consumed = new Task(delegate { });
        _produced.Start();
        return _consumed;
      }

      public void Dispose() { }
    }
  }
}
