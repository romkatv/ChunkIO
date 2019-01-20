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
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  sealed class PipeServer : IDisposable {
    readonly CancellationTokenSource _cancel = new CancellationTokenSource();
    readonly Task _srv;
    bool _stopped = false;

    // Creates a multi-threaded named pipe server. It keeps the specified number of "free"
    // instances that are listening for incoming connections. The total number of instances
    // never exceeds 254, which may or may not be the limit imposed by .NET or Win32 API.
    // The docs are confusing on this matter.
    //
    // The pipe is created in the constructor. Thus, as soon as it returns, the existence check
    // for the pipe file will succeed.
    public PipeServer(string name, int freeInstances, Func<Stream, CancellationToken, Task> handler) {
      const int MaxNamedPipeServerInstances = 254;
      if (name == null) throw new ArgumentNullException(nameof(name));
      if (name == "" || name.Contains(':')) throw new ArgumentException($"Invalid name: {name}");
      if (handler == null) throw new ArgumentNullException(nameof(handler));
      if (File.Exists(@"\\.\pipe\" + name)) throw new Exception($"Pipe already exists: {name}");
      if (freeInstances <= 0 || freeInstances > MaxNamedPipeServerInstances) {
        throw new ArgumentException($"Invalid number of free instances: {freeInstances}");
      }

      var monitor = new object();
      int free = 0;
      int active = 0;
      Task wake = null;

      _srv = AcceptLoop();

      void Update(Action update) {
        lock (monitor) {
          update.Invoke();
          if (wake.Status == TaskStatus.Created) wake.Start();
        }
      }

      async Task RunServer() {
        NamedPipeServerStream srv = null;
        bool connected = false;
        try {
          srv = new NamedPipeServerStream(
              pipeName: name,
              direction: PipeDirection.InOut,
              maxNumberOfServerInstances: NamedPipeServerStream.MaxAllowedServerInstances,
              transmissionMode: PipeTransmissionMode.Byte,
              options: PipeOptions.Asynchronous | PipeOptions.WriteThrough,
              inBufferSize: 0,
              outBufferSize: 0);
          await srv.WaitForConnectionAsync(_cancel.Token);
          connected = true;
          Update(() => {
            --free;
            ++active;
          });
          await handler.Invoke(srv, _cancel.Token);
        } finally {
          if (connected) {
            try { srv.Disconnect(); } catch { }
          }
          if (srv != null) {
            try { srv.Dispose(); } catch { }
          }
          Update(() => {
            if (connected) {
              --active;
            } else {
              --free;
            }
          });
        }
      }

      async Task AcceptLoop() {
        try {
          while (true) {
            int start;
            lock (monitor) {
              Debug.Assert(free >= 0 && free <= freeInstances);
              Debug.Assert(active >= 0 && free + active <= MaxNamedPipeServerInstances);
              start = Math.Min(freeInstances - free, MaxNamedPipeServerInstances - free - active);
              free += start;
              wake = new Task(delegate { }, _cancel.Token);
            }
            Debug.Assert(start >= 0);
            while (start-- > 0) {
              Task _ = RunServer();
            }
            await wake;
          }
        } catch (Exception e) {
          bool ok = _cancel.IsCancellationRequested;
          _cancel.Cancel();
          while (true) {
            lock (monitor) {
              if (free == 0 && active == 0) break;
              wake = new Task(delegate { });
            }
            await wake;
          }
          if (ok && e is TaskCanceledException) return;
          throw;
        }
      }
    }

    // Cancel all outstanding instances (both free and active) and stop the listening loop.
    // Only after that the task will complete.
    //
    // Does nothing if it's not the first call.
    public async Task Stop() {
      if (_stopped) return;
      _stopped = true;
      _cancel.Cancel();
      try {
        await _srv;
      } finally {
        _cancel.Dispose();
      }
    }

    public void Dispose() => Stop().Wait();
  }
}
