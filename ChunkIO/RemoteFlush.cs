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
    static SemaphoreSlim s_connect = new SemaphoreSlim(1, 1);

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
    public static async Task<long?> FlushAsync(string fname, bool flushToDisk) {
      if (fname == null) throw new ArgumentNullException(nameof(fname));
      Log.Info("FlushAsync({0}, {1}): start", fname, flushToDisk);
      try {
        while (true) {
          using (var pipe = new NamedPipeClientStream(".", PipeNameFromFile(fname), PipeDirection.InOut,
                                                      PipeOptions.Asynchronous | PipeOptions.WriteThrough)) {
            // NamedPipeClientStream has an awful API. It doesn't allow us to bail early if there is no pipe and
            // to wait indefinitely if there is (this can be done with Win32 API). So we do it manually with
            // a mutex existence check and a loop. We create this mutex together with the pipe to serve as
            // a marker of the pipe's existence. It's difficult to use the pipe itself as such marker because
            // server pipes disappear after accepting and serving a request and there may be a short gap before
            // we create new server connections.
            if (Mutex.TryOpenExisting(MutexNameFromFile(fname), MutexRights.Synchronize, out Mutex mutex)) {
              Log.Info("FlushAsync({0}, {1}): mutex exists: {2}", fname, flushToDisk, MutexNameFromFile(fname));
              mutex.Dispose();
            } else {
              Log.Info("FlushAsync({0}, {1}): mutex doesn't exist: {2}", fname, flushToDisk, MutexNameFromFile(fname));
              return null;
            }
            await s_connect.WaitAsync();
            try {
              // The timeout is meant to avoid waiting forever if the pipe disappears between the moment
              // we have verified its existence and our attempt to connect to it. We use a semaphore to
              // restrict the number of simultaneous ConnectAsync() because ConnectAsync() blocks a task
              // thread for the whole duration of its execution. Without the semaphore, WaitAsync() calls
              // could block all task threads.
              Log.Info("FlushAsync({0}, {1}): connecting", fname, flushToDisk);
              await pipe.ConnectAsync(100);
              Log.Info("FlushAsync({0}, {1}): connected", fname, flushToDisk);
            } catch (TimeoutException) {
              Log.Info("FlushAsync({0}, {1}): ConnectAsync => TimeoutException", fname, flushToDisk);
              continue;
            } catch (IOException) {
              Log.Info("FlushAsync({0}, {1}): ConnectAsync => IOException", fname, flushToDisk);
              continue;
            } finally {
              s_connect.Release();
            }
            var buf = new byte[UInt64LE.Size];
            if (flushToDisk) buf[0] = 1;
            try {
              await pipe.WriteAsync(buf, 0, 1);
              await pipe.FlushAsync();
              if (await pipe.ReadAsync(buf, 0, UInt64LE.Size) != UInt64LE.Size) {
                Log.Info("FlushAsync({0}, {1}): incomplete response", fname, flushToDisk);
                continue;
              }
            } catch (Exception e) {
              Log.Info("FlushAsync({0}, {1}): IO => {2} ({3})", fname, flushToDisk, e.GetType().Name, e.Message);
              continue;
            }
            int offset = 0;
            ulong res = UInt64LE.Read(buf, ref offset);
            if (res < long.MaxValue) {
              Log.Info("FlushAsync({0}, {1}): success", fname, flushToDisk);
              return (long)res;
            }
            if (res == ulong.MaxValue) {
              throw new IOException($"Remote writer failed to Flush: {fname}");
            }
            throw new Exception($"Invalid Flush response: {res}");
          }
        }
      } catch (Exception e) {
        Log.Info("FlushAsync({0}, {1}): failed with {2} ({3})", fname, flushToDisk, e.GetType().Name, e.Message);
        throw;
      }
    }

    public static IDisposable CreateListener(string fname, Func<bool, Task<long>> flush) => new Listener(fname, flush);

    static string HexLowerCase(byte[] bytes) {
      return string.Join("", bytes.Select(b => b.ToString("x2")));
    }

    static string PipeNameFromFile(string fname) {
      using (var md5 = MD5.Create()) {
        string id = Path.GetFullPath(fname).ToLowerInvariant();
        return "romkatv-chunkio-" + HexLowerCase(md5.ComputeHash(Encoding.UTF8.GetBytes(id)));
      }
    }

    static string MutexNameFromFile(string fname) => @"Global\" + PipeNameFromFile(fname);

    sealed class Listener : IDisposable {
      readonly PipeServer _srv;
      readonly Mutex _mutex;

      public Listener(string fname, Func<bool, Task<long>> flush) {
        Log.Info("Listener({0}): creating mutex {1}", fname, MutexNameFromFile(fname));
        // This mutex serves as a marker of the existence of flush listener. RemoteFLush.FlushAsync() looks at it.
        // We allow all authenticated users to access the mutex.
        var security = new MutexSecurity();
        security.AddAccessRule(
            new MutexAccessRule(
                new SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, null),
                MutexRights.Synchronize,
                AccessControlType.Allow));
        _mutex = new Mutex(true, MutexNameFromFile(fname), out bool createdNew, security);
        try {
          _srv = new PipeServer(PipeNameFromFile(fname), 2, async (Stream strm, CancellationToken cancel) => {
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

      public void Dispose() {
        try {
          _srv.Dispose();
        } finally {
          _mutex.Dispose();
        }
      }
    }
  }
}
