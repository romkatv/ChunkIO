using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  static class Encoding {
    public static class UInt64 {
      public const int Size = 8;

      public static void Write(byte[] array, ref int offset, ulong val) {
        array[offset++] = (byte)(val >> 0);
        array[offset++] = (byte)(val >> 8);
        array[offset++] = (byte)(val >> 16);
        array[offset++] = (byte)(val >> 24);
        array[offset++] = (byte)(val >> 32);
        array[offset++] = (byte)(val >> 40);
        array[offset++] = (byte)(val >> 48);
        array[offset++] = (byte)(val >> 56);
      }

      public static ulong Read(byte[] array, ref int offset) {
        return (ulong)array[offset++] << 0 |
               (ulong)array[offset++] << 8 |
               (ulong)array[offset++] << 16 |
               (ulong)array[offset++] << 24 |
               (ulong)array[offset++] << 32 |
               (ulong)array[offset++] << 40 |
               (ulong)array[offset++] << 48 |
               (ulong)array[offset++] << 56;
      }
    }
  }
}
