using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  // TODO: Remove this file once C# 8 is out.
  interface IAsyncDisposable {
    Task DisposeAsync();
  }
}
