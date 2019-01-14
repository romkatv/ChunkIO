using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  static class Chunk {
    public const ulong MaxContentLength = int.MaxValue;
    public const ulong MaxPosition = long.MaxValue;
    public const int MeterInterval = 64 << 10;

    public static bool IsValidBeginPosition(ulong pos) =>
        pos <= MaxPosition &&
        (pos + MeterInterval - 1) % MeterInterval >= Meter.Size;

    public static ulong? MeteredPosition(ulong begin, ulong offset) {
      if (!IsValidBeginPosition(begin)) return null;
      if (offset > MaxPosition + ChunkHeader.Size) return null;
      ulong p = begin % MeterInterval;
      ulong n = offset / (MeterInterval - Meter.Size);
      ulong m = offset % (MeterInterval - Meter.Size);
      if (p == 0 && m > 0 || p + m > MeterInterval) ++n;
      ulong res = begin + offset + n * Meter.Size;
      Debug.Assert(IsValidBeginPosition(res));
      if (res > MaxPosition) return null;
      return res;
    }
  }

  struct ChunkHeader {
    public const int Size = UserData.Size + 3 * Encoding.UInt64.Size;

    public UserData UserData { get; set; }
    public ulong ContentLength { get; set; }
    public ulong ContentHash { get; set; }

    public void WriteTo(byte[] array) {
      Debug.Assert(IsValid());
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      UserData.WriteTo(array, ref offset);
      Encoding.UInt64.Write(array, ref offset, ContentLength);
      Encoding.UInt64.Write(array, ref offset, ContentHash);
      Encoding.UInt64.Write(array, ref offset, SipHash.ComputeHash(array, 0, offset));
    }

    public bool ReadFrom(byte[] array) {
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      UserData.ReadFrom(array, ref offset);
      ContentLength = Encoding.UInt64.Read(array, ref offset);
      ContentHash = Encoding.UInt64.Read(array, ref offset);
      ulong expectedHash = SipHash.ComputeHash(array, 0, offset);
      ulong actualHash = Encoding.UInt64.Read(array, ref offset);
      return expectedHash == actualHash && IsValid();
    }

    bool IsValid() => ContentLength <= Chunk.MaxContentLength;
  }

  struct Meter {
    public const int Size = 3 * Encoding.UInt64.Size;

    public ulong ChunkBeginPosition { get; set; }
    public ulong ContentLength { get; set; }

    public void WriteTo(byte[] array) {
      Debug.Assert(IsValid());
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      Encoding.UInt64.Write(array, ref offset, ChunkBeginPosition);
      Encoding.UInt64.Write(array, ref offset, ContentLength);
      Encoding.UInt64.Write(array, ref offset, SipHash.ComputeHash(array, 0, offset));
    }

    public bool ReadFrom(byte[] array) {
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      ChunkBeginPosition = Encoding.UInt64.Read(array, ref offset);
      ContentLength = Encoding.UInt64.Read(array, ref offset);
      ulong expectedHash = SipHash.ComputeHash(array, 0, offset);
      ulong actualHash = Encoding.UInt64.Read(array, ref offset);
      return expectedHash == actualHash && IsValid();
    }

    bool IsValid() =>
        ContentLength <= Chunk.MaxContentLength &&
        Chunk.IsValidBeginPosition(ChunkBeginPosition) &&
        Chunk.MeteredPosition(ChunkBeginPosition, ChunkHeader.Size + ContentLength).HasValue;
  }
}
