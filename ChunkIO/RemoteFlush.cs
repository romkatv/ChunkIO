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
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChunkIO {
  static class RemoteFlush {
    static AsyncMutex s_connect = new AsyncMutex();

    // Returns:
    //
    //   * null if there is no listener associated with the file.
    //   * file length immediately after flushing (the flushing and the grabbing of the file length are
    //     done atomically) if there is a listener and the file was successfully flushed.
    //
    // Throws in all other cases. For example:
    //
    //   * The remote writer failed to flush because disk is full.
    //   * The remote writer sent invalid response to our request.
    //   * Pipe permission error.
    public static async Task<long?> FlushAsync(IReadOnlyCollection<byte> fileId, bool flushToDisk) {
      if (fileId == null) throw new ArgumentNullException(nameof(fileId));
      while (true) {
        using (var pipe = new NamedPipeClientStream(".", PipeName(fileId), PipeDirection.InOut,
                                                    PipeOptions.Asynchronous | PipeOptions.WriteThrough)) {
          await s_connect.LockAsync();
          try {
            // NamedPipeClientStream has an awful API. It doesn't allow us to bail early if there is no pipe and
            // to wait indefinitely if there is (this can be done with Win32 API). So we do it manually with
            // a mutex existence check and a loop. We create this mutex together with the pipe to serve as
            // a marker of the pipe's existence. It's difficult to use the pipe itself as such marker because
            // server pipes disappear after accepting and serving a request and there may be a short gap before
            // we create new server connections.
            if (Mutex.TryOpenExisting(MutexName(fileId), MutexRights.Synchronize, out Mutex mutex)) {
              mutex.Dispose();
            } else {
              return null;
            }
            // The timeout is meant to avoid waiting forever if the pipe disappears between the moment
            // we have verified its existence and our attempt to connect to it. We use a mutex to
            // restrict the number of simultaneous ConnectAsync() because ConnectAsync() blocks a task
            // thread for the whole duration of its execution. Without the mutex, WaitAsync() calls
            // could block all task threads.
            await pipe.ConnectAsync(100);
          } catch (TimeoutException) {
            continue;
          } catch (IOException) {
            continue;
          } finally {
            s_connect.Unlock(runNextSynchronously: false);
          }
          var buf = new byte[UInt64LE.Size];
          if (flushToDisk) buf[0] = 1;
          try {
            await pipe.WriteAsync(buf, 0, 1);
            await pipe.FlushAsync();
            if (await pipe.ReadAsync(buf, 0, UInt64LE.Size) != UInt64LE.Size) continue;
          } catch {
            continue;
          }
          int offset = 0;
          ulong res = UInt64LE.Read(buf, ref offset);
          if (res < long.MaxValue) return (long)res;
          if (res == ulong.MaxValue) {
            throw new IOException("Remote writer failed to flush");
          }
          throw new Exception($"Invalid Flush response: {res}");
        }
      }
    }

    static string HexLowerCase(byte[] bytes) {
      return string.Join("", bytes.Select(b => b.ToString("x2")));
    }

    static string PipeName(IReadOnlyCollection<byte> fileId) =>
        "romkatv-chunkio-" + string.Join("", fileId.Select(b => b.ToString("x2")));

    static string MutexName(IReadOnlyCollection<byte> fileId) => @"Global\" + PipeName(fileId);

    public sealed class Listener : IDisposable, IAsyncDisposable {
      readonly PipeServer _srv;
      readonly Mutex _mutex;

      public Listener(IReadOnlyCollection<byte> fileId, Func<bool, Task<long>> flush) {
        // This mutex serves as a marker of the existence of flush listener. RemoteFLush.FlushAsync() looks at it.
        // We allow all authenticated users to access the mutex.
        var security = new MutexSecurity();
        security.AddAccessRule(
            new MutexAccessRule(
                new SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, null),
                MutexRights.Synchronize,
                AccessControlType.Allow));
        _mutex = new Mutex(true, MutexName(fileId), out bool createdNew, security);
        try {
          _srv = new PipeServer(PipeName(fileId), 2, async (Stream strm, CancellationToken cancel) => {
            var buf = new byte[UInt64LE.Size];
            if (await strm.ReadAsync(buf, 0, 1, cancel) != 1) throw new Exception("Empty Flush request");
            if (buf[0] != 0 && buf[0] != 1) throw new Exception("Invalid Flush request");
            bool flushToDisk = buf[0] == 1;
            try {
              long len = await flush.Invoke(flushToDisk);
              Debug.Assert(len >= 0);
              int offset = 0;
              UInt64LE.Write(buf, ref offset, (ulong)len);
            } catch {
              int offset = 0;
              UInt64LE.Write(buf, ref offset, ulong.MaxValue);
            }
            await strm.WriteAsync(buf, 0, UInt64LE.Size, cancel);
            await strm.FlushAsync(cancel);
          });
        } catch {
          _mutex.Dispose();
          throw;
        }
      }

      public async Task DisposeAsync() {
        try {
          await _srv.DisposeAsync();
        } finally {
          _mutex.Dispose();
        }
      }

      public void Dispose() => DisposeAsync().Wait();
    }
  }
}
