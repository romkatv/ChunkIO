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
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  static class RemoteFlush {
    // Returns:
    //
    //   * false if there is no listener associated with the file.
    //   * true if there is a listener and the file was successfully flushed.
    //
    // Throws in all other cases. For example:
    //
    //   * The remote writer failed to flush because disk is full.
    //   * The remote writer sent invalid response to our request.
    //   * Pipe permission error.
    public static async Task<bool> FlushAsync(string fname, bool flushToDisk) {
      if (fname == null) throw new ArgumentNullException(nameof(fname));
      while (true) {
        using (var pipe = new NamedPipeClientStream(".", PipeNameFromFile(fname), PipeDirection.InOut,
                                                    PipeOptions.Asynchronous | PipeOptions.WriteThrough)) {
          // NamedPipeClientStream has awful API. It doesn't allow us to bail early if there is no pipe and
          // to wait indefinitely if there is. So we do it manually with a file existence check and a loop.
          // Unfortunately, this existence check has a side effect of creating a client pipe connection.
          // This is why our server has to handle connections that don't write any data.
          if (!File.Exists(@"\\.\pipe\" + PipeNameFromFile(fname))) return false;
          try {
            await pipe.ConnectAsync(1000);
          } catch (TimeoutException) {
            continue;
          } catch (IOException) {
            continue;
          }
          var buf = new byte[1] { (byte)(flushToDisk ? 1 : 0) };
          try {
            await pipe.WriteAsync(buf, 0, 1);
            if (await pipe.ReadAsync(buf, 0, 1) != 1) continue;
          } catch {
            continue;
          }
          switch (buf[0]) {
            case 0:
              throw new IOException($"Remote writer failed to Flush: {fname}");
            case 1:
              return true;
            default:
              throw new Exception("Invalid Flush response");
          }
        }
      }
    }

    public static IDisposable CreateListener(string fname, Func<bool, Task> flush) => new Listener(fname, flush);

    static string HexLowerCase(byte[] bytes) {
      return string.Join("", bytes.Select(b => b.ToString("x2")));
    }

    static string PipeNameFromFile(string fname) {
      string id = "romkatv/chunkio:" + fname.ToLowerInvariant();
      using (var md5 = MD5.Create()) {
        return HexLowerCase(md5.ComputeHash(System.Text.Encoding.UTF8.GetBytes(id)));
      }
    }

    sealed class Listener : IDisposable {
      readonly CancellationTokenSource _cancel = new CancellationTokenSource();
      readonly Task _srv;
      bool _disposed = false;

      public Listener(string fname, Func<bool, Task> flush) {
        _srv = RunPipeServer(PipeNameFromFile(fname), 4, _cancel.Token, async (Stream strm, CancellationToken c) => {
          var buf = new byte[1];
          // Comments in FlushAsync() explanain why there may be no data in the stream.
          if (await strm.ReadAsync(buf, 0, 1, c) != 1) return;
          if (buf[0] != 0 && buf[0] != 1) throw new Exception("Invalid Flush request");
          bool flushToDisk = buf[0] == 1;
          try {
            await flush.Invoke(flushToDisk);
            buf[0] = 1;
          } catch {
            buf[0] = 0;
          }
          await strm.WriteAsync(buf, 0, 1, c);
        });
      }

      public void Dispose() {
        if (_disposed) return;
        _disposed = true;
        _cancel.Cancel();
        try {
          _srv.Wait();
        } finally {
          _cancel.Dispose();
        }
      }

      // Generic multi-threaded named pipe server. It keeps the specified number of "free"
      // instances that are listening for incoming connections. The total number of instances
      // never exceeds 254, which may or may not be the limit imposed by dotnet or win32 API.
      // The docs are confusing on this matter.
      //
      // Signalling via the cancellation token will cancel all outstanding instances (both free
      // and active) and stop the listening loop. Only after that the task will complete.
      //
      // The pipe is created during the initial synchronous part of the method. Thus, when
      // RunPipeServer() returns its task, the pipe already exists.
      public static async Task RunPipeServer(string name, int freeInstances, CancellationToken cancel,
                                             Func<Stream, CancellationToken, Task> handler) {
        const int MaxNamedPipeServerInstances = 254;
        if (name == null) throw new ArgumentNullException(nameof(name));
        if (name == "" || name.Contains(':')) throw new ArgumentException($"Invalid name: {name}");
        if (handler == null) throw new ArgumentNullException(nameof(handler));
        if (freeInstances <= 0 || freeInstances > MaxNamedPipeServerInstances) {
          throw new ArgumentException($"Invalid number of free instances: {freeInstances}");
        }

        var monitor = new object();
        int free = 0;
        int active = 0;
        Task wake = null;

        try {
          while (true) {
            int start;
            lock (monitor) {
              Debug.Assert(free >= 0 && free <= freeInstances);
              Debug.Assert(active >= 0 && free + active <= MaxNamedPipeServerInstances);
              start = Math.Min(freeInstances - free, MaxNamedPipeServerInstances - free - active);
              free += start;
              wake = new Task(delegate { }, cancel);
            }
            Debug.Assert(start >= 0);
            while (start-- > 0) {
              Task _ = RunServer();
            }
            await wake;
          }
        } catch {
          if (!cancel.IsCancellationRequested) throw;
          while (true) {
            lock (monitor) {
              if (free == 0 && active == 0) break;
              wake = new Task(delegate { });
            }
            await wake;
          }
        }

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
            await srv.WaitForConnectionAsync(cancel);
            connected = true;
            Update(() => {
              --free;
              ++active;
            });
            await handler.Invoke(srv, cancel);
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
      }
    }
  }
}
