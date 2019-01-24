using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  static class Log {
    const bool Enabled = false;

    static readonly string Dir = @"C:\ChunkIO";
    static readonly string FilePath = Path.Combine(Dir, "chunkio.log");

    static readonly object s_monitor = new object();

    static Log() {
      if (!Enabled) return;
      try {
        if (!Directory.Exists(Dir)) Directory.CreateDirectory(Dir);
      } catch {
      }
    }

    public static void Info(string msg) {
      if (!Enabled) return;
      try {
        string rec = string.Format("[{0:yyyy-MM-dd HH:mm:ss.ffff} INFO] {1}\n", DateTime.UtcNow, msg);
        lock (s_monitor) File.AppendAllText(FilePath, rec, Encoding.UTF8);
      } catch {
      }
    }

    public static void Info(string fmt, params object[] p) {
      if (!Enabled) return;
      try {
        Info(p == null || p.Length == 0 ? fmt : string.Format(fmt, p));
      } catch {
      }
    }
  }
}
