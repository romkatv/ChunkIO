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
  static class NamedPipeServer {
    public const int MaxNumInstances = 254;

    public static async Task Run(string name, int freeInstances, int maxInstances, CancellationToken cancel,
                                 Func<Stream, CancellationToken, Task> handler) {
      if (name == null) throw new ArgumentNullException(nameof(name));
      if (name == "" || name.Contains(':')) throw new ArgumentException($"Invalid name: {name}");
      if (handler == null) throw new ArgumentNullException(nameof(handler));
      if (freeInstances <= 0 || freeInstances > maxInstances || maxInstances > MaxNumInstances) {
        throw new ArgumentException($"Invalid number of instances: free = {freeInstances}, max = {maxInstances}");
      }

      var monitor = new object();
      int listening = 0;
      int active = 0;
      Task wake = null;

      void Update(Action update) {
        lock (monitor) {
          update.Invoke();
          if (wake != null && wake.Status == TaskStatus.Created) wake.Start();
        }
      }

      try {
        while (true) {
          lock (monitor) {
            while (listening < freeInstances && listening + active < maxInstances) {
              ++listening;
              Task _ = RunServer();
            }
            wake = new Task(delegate { }, cancel);
          }
          await wake;
        }
      } finally {
        if (cancel.IsCancellationRequested) {
          while (true) {
            lock (monitor) {
              if (listening == 0 && active == 0) break;
              wake = new Task(delegate { });
            }
            await wake;
          }
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

  static class RemoteFlush {
    public static Task FlushAsync(string fname, bool flushToDisk) {
      throw new NotImplementedException();
    }

    public static IDisposable CreateListener(string fname, Func<bool, Task> flush) {
      throw new NotImplementedException();
    }

    static string HexLowerCase(byte[] bytes) {
      return string.Join("", bytes.Select(b => b.ToString("x2")));
    }

    static string PipeNameFromFile(string fname) {
      using (var md5 = MD5.Create()) {
        return HexLowerCase(md5.ComputeHash(System.Text.Encoding.UTF8.GetBytes(fname.ToLowerInvariant())));
      }
    }

    sealed class Listener : IDisposable {
      readonly CancellationTokenSource _cancel = new CancellationTokenSource();
      readonly Task _srv;
      bool _disposed = false;

      public Listener(string fname, Func<bool, Task> flush) {
        _srv = NamedPipeServer.Run(PipeNameFromFile(fname), 4, NamedPipeServer.MaxNumInstances, _cancel.Token, Flush);

        async Task Flush(Stream strm, CancellationToken cancel) {
          var buf = new byte[1];
          if (await strm.ReadAsync(buf, 0, 1, cancel) != 1) throw new Exception("Empty Flush request");
          if (buf[0] != 0 && buf[0] != 1) throw new Exception("Invalid Flush request");
          bool flushToDisk = buf[0] == 1;
          try {
            await flush.Invoke(flushToDisk);
            buf[0] = 1;
          } catch {
            buf[0] = 0;
          }
          await strm.WriteAsync(buf, 0, 1, cancel);
        }
      }

      public void Dispose() {
        if (_disposed) return;
        _disposed = true;
        _cancel.Cancel();
        try { _srv.Wait(); } catch { }
        _cancel.Dispose();
      }
    }
  }
}
