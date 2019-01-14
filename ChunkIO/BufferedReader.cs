﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  // Readable and seakable but not writable.
  abstract class InputBuffer : Stream {
    protected InputBuffer(UserData userData) { UserData = userData; }
    public UserData UserData { get; }
  }

  class BufferedReader : IDisposable {
    public BufferedReader(string fname) {
      throw new NotImplementedException();
    }

    public string Name => throw new NotImplementedException();

    // If false chunks cannot follow true chunks, seeks to the last false chunk if there are any or
    // to the very first chunk otherwise.
    //
    // TODO: Document what it does if there is no ordering guarantee.
    public Task<InputBuffer> ReadAtPartitionAsync(Func<UserData, bool> pred) {
      throw new NotImplementedException();
    }

    public Task<InputBuffer> ReadNextAsync() {
      throw new NotImplementedException();
    }

    public Task FlushRemoteWriterAsync() {
      throw new NotImplementedException();
    }

    public void Dispose() {
      throw new NotImplementedException();
    }
  }
}
