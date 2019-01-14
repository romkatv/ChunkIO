using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  static class Chunk {
    public const int MaxContentLength = int.MaxValue;
    public const long MaxPosition = long.MaxValue;
    public const int MeterInterval = 64 << 10;

    public static bool IsValidContentLength(int len) => len >= 0;
    public static bool IsValidContentLength(ulong len) => len <= MaxContentLength;

    public static bool IsValidPosition(long pos) =>
        pos >= 0 &&
        ((ulong)pos + MeterInterval - 1) % MeterInterval >= Meter.Size;

    public static bool IsValidPosition(ulong pos) =>
        pos <= MaxPosition && IsValidPosition((long)pos);

    public static long? MeteredPosition(long begin, long offset) {
      if (!IsValidPosition(begin)) return null;
      if (offset - MaxContentLength - ChunkHeader.Size > 0) return null;
      long p = begin % MeterInterval;
      long n = offset / (MeterInterval - Meter.Size);
      long m = offset % (MeterInterval - Meter.Size);
      if (p == 0 && m > 0 || p + m > MeterInterval) ++n;
      ulong res = (ulong)begin + (ulong)offset + (ulong)n * Meter.Size;
      if (res > MaxPosition) return null;
      Debug.Assert(IsValidPosition((long)res));
      return (long)res;
    }
  }

  struct ChunkHeader {
    public const int Size = UserData.Size + 3 * Encoding.UInt64.Size;

    public UserData UserData { get; set; }
    public int ContentLength { get; set; }
    public ulong ContentHash { get; set; }

    public void WriteTo(byte[] array) {
      if (!IsValid()) throw new Exception("Internal error: writing invalid ChunkHeader");
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      UserData.WriteTo(array, ref offset);
      Encoding.UInt64.Write(array, ref offset, (ulong)ContentLength);
      Encoding.UInt64.Write(array, ref offset, ContentHash);
      Encoding.UInt64.Write(array, ref offset, SipHash.ComputeHash(array, 0, offset));
    }

    public bool ReadFrom(byte[] array) {
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      UserData.ReadFrom(array, ref offset);
      ulong len = Encoding.UInt64.Read(array, ref offset);
      if (!Chunk.IsValidContentLength(len)) return false;
      ContentLength = (int)len;
      ContentHash = Encoding.UInt64.Read(array, ref offset);
      ulong expectedHash = SipHash.ComputeHash(array, 0, offset);
      ulong actualHash = Encoding.UInt64.Read(array, ref offset);
      return expectedHash == actualHash && IsValid();
    }

    bool IsValid() => Chunk.IsValidContentLength(ContentLength);
  }

  struct Meter {
    public const int Size = 3 * Encoding.UInt64.Size;

    public long ChunkBeginPosition { get; set; }
    public int ContentLength { get; set; }

    public void WriteTo(byte[] array) {
      if (!IsValid()) throw new Exception("Internal error: writing invalid Meter");
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      Encoding.UInt64.Write(array, ref offset, (ulong)ChunkBeginPosition);
      Encoding.UInt64.Write(array, ref offset, (ulong)ContentLength);
      Encoding.UInt64.Write(array, ref offset, SipHash.ComputeHash(array, 0, offset));
    }

    public bool ReadFrom(byte[] array) {
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      ulong pos = Encoding.UInt64.Read(array, ref offset);
      if (!Chunk.IsValidPosition(pos)) return false;
      ChunkBeginPosition = (long)pos;
      ulong len = Encoding.UInt64.Read(array, ref offset);
      if (!Chunk.IsValidContentLength(len)) return false;
      ContentLength = (int)len;
      ulong expectedHash = SipHash.ComputeHash(array, 0, offset);
      ulong actualHash = Encoding.UInt64.Read(array, ref offset);
      return expectedHash == actualHash && IsValid();
    }

    bool IsValid() =>
        Chunk.IsValidContentLength(ContentLength) &&
        Chunk.IsValidPosition(ChunkBeginPosition) &&
        Chunk.MeteredPosition(ChunkBeginPosition, (long)ChunkHeader.Size + ContentLength).HasValue;
  }
}
