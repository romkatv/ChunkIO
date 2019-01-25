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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  static class Format {
    // This is the maximum length of byte[] in .NET.
    public const int MaxContentLength = 2147483591;
    public const long MaxPosition = long.MaxValue;
    public const int MeterInterval = 64 << 10;

    public static byte[] FileHeader { get; } = Encoding.UTF8.GetBytes("romkatv/chunkio\n");

    static Format() { Debug.Assert(FileHeader.Length == Meter.Size); }

    public static bool IsValidContentLength(int len) => len >= 0;
    public static bool IsValidContentLength(ulong len) => len <= MaxContentLength;

    public static bool IsValidPosition(long pos) =>
        pos >= 0 &&
        ((ulong)pos + MeterInterval - 1) % MeterInterval >= Meter.Size;

    public static bool IsValidPosition(ulong pos) => pos <= MaxPosition && IsValidPosition((long)pos);

    public static long? MeteredPosition(long begin, long offset) {
      if (!IsValidPosition(begin)) return null;
      if (offset < 0 || offset - MaxContentLength - ChunkHeader.Size > 0) return null;
      long p = begin % MeterInterval;
      long n = offset / (MeterInterval - Meter.Size);
      long m = offset % (MeterInterval - Meter.Size);
      if (p == 0 && m > 0 || p + m > MeterInterval) ++n;
      ulong res = (ulong)begin + (ulong)offset + (ulong)n * Meter.Size;
      if (res > MaxPosition) return null;
      Debug.Assert(IsValidPosition(res));
      return (long)res;
    }

    public static bool VerifyHash(byte[] array, ref int offset) {
      ulong expected = SipHash.ComputeHash(array, 0, offset);
      ulong actual = UInt64LE.Read(array, ref offset);
      return expected == actual;
    }
  }

  struct ChunkHeader {
    public const int Size = UserData.Size + 3 * UInt64LE.Size;

    public UserData UserData { get; set; }
    public int ContentLength { get; set; }
    public ulong ContentHash { get; set; }

    public void WriteTo(byte[] array) {
      Debug.Assert(array.Length >= Size);
      Debug.Assert(Format.IsValidContentLength(ContentLength));
      int offset = 0;
      UserData.WriteTo(array, ref offset);
      UInt64LE.Write(array, ref offset, (ulong)ContentLength);
      UInt64LE.Write(array, ref offset, ContentHash);
      UInt64LE.Write(array, ref offset, SipHash.ComputeHash(array, 0, offset));
    }

    public bool ReadFrom(byte[] array) {
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      UserData = UserData.ReadFrom(array, ref offset);
      ulong len = UInt64LE.Read(array, ref offset);
      if (!Format.IsValidContentLength(len)) return false;
      ContentLength = (int)len;
      ContentHash = UInt64LE.Read(array, ref offset);
      return Format.VerifyHash(array, ref offset);
    }

    public long? EndPosition(long begin) => Format.MeteredPosition(begin, (long)ContentLength + Size);
  }

  struct Meter {
    public const int Size = 2 * UInt64LE.Size;

    public long ChunkBeginPosition { get; set; }

    public void WriteTo(byte[] array) {
      Debug.Assert(array.Length >= Size);
      Debug.Assert(Format.IsValidPosition(ChunkBeginPosition));
      int offset = 0;
      UInt64LE.Write(array, ref offset, (ulong)ChunkBeginPosition);
      UInt64LE.Write(array, ref offset, SipHash.ComputeHash(array, 0, offset));
    }

    public bool ReadFrom(byte[] array) {
      Debug.Assert(array.Length >= Size);
      int offset = 0;
      ulong pos = UInt64LE.Read(array, ref offset);
      if (!Format.IsValidPosition(pos)) return false;
      ChunkBeginPosition = (long)pos;
      return Format.VerifyHash(array, ref offset);
    }
  }
}
