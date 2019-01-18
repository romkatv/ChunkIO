using System;
using System.Collections.Generic;
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
    // Throws in all other cases.
    public static async Task<bool> FlushAsync(string fname, bool flushToDisk) {
      if (fname == null) throw new ArgumentNullException(nameof(fname));
      while (true) {
        using (var pipe = new NamedPipeClientStream(".", PipeNameFromFile(fname), PipeDirection.InOut,
                                                    PipeOptions.Asynchronous | PipeOptions.WriteThrough)) {
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
        _srv = RunPipeServer(PipeNameFromFile(fname), 2, _cancel.Token, async (Stream strm, CancellationToken c) => {
          var buf = new byte[1];
          if (await strm.ReadAsync(buf, 0, 1, c) != 1) throw new Exception("Empty Flush request");
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
        int listening = 0;
        int active = 0;
        Task wake = null;

        try {
          while (true) {
            lock (monitor) {
              while (listening < freeInstances && listening + active < MaxNamedPipeServerInstances) {
                ++listening;
                Task _ = RunServer();
              }
              wake = new Task(delegate { }, cancel);
            }
            await wake;
          }
        } catch {
          if (!cancel.IsCancellationRequested) throw;
          while (true) {
            lock (monitor) {
              if (listening == 0 && active == 0) break;
              wake = new Task(delegate { });
            }
            await wake;
          }
        }

        void Update(Action update) {
          lock (monitor) {
            update.Invoke();
            if (wake != null && wake.Status == TaskStatus.Created) wake.Start();
          }
        }

        async Task RunServer() {
          await Task.Yield();
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
              --listening;
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
                --listening;
              }
            });
          }
        }
      }
    }
  }
}
