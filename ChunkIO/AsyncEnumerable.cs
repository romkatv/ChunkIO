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
}
