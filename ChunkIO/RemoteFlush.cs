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
          // to wait indefinitely if there is (this can be done with Win32 API). So we do it manually with
          // a file existence check and a loop. Unfortunately, this existence check has a side effect of
          // creating a client pipe connection. This is why our server has to handle connections that don't
          // write any data.
          if (!File.Exists(@"\\.\pipe\" + PipeNameFromFile(fname))) return false;
          try {
            // The timeout is meant to avoid waiting forever if the pipe disappears between the moment
            // we have verified its existence and our attempt to connect to it.
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
        return HexLowerCase(md5.ComputeHash(Encoding.UTF8.GetBytes(id)));
      }
    }

    sealed class Listener : IDisposable {
      readonly PipeServer _srv;

      public Listener(string fname, Func<bool, Task> flush) {
        _srv = new PipeServer(PipeNameFromFile(fname), 4, async (Stream strm, CancellationToken cancel) => {
          var buf = new byte[1];
          // Comments in RemoteFlush.FlushAsync() explanain why there may be no data in the stream.
          if (await strm.ReadAsync(buf, 0, 1, cancel) != 1) return;
          if (buf[0] != 0 && buf[0] != 1) throw new Exception("Invalid Flush request");
          bool flushToDisk = buf[0] == 1;
          try {
            await flush.Invoke(flushToDisk);
            buf[0] = 1;
          } catch {
            buf[0] = 0;
          }
          await strm.WriteAsync(buf, 0, 1, cancel);
        });
      }

      public void Dispose() => _srv.Dispose();
    }
  }
}
