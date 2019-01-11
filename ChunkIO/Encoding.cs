using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    static class Encoding
    {
        public static class UInt64
        {
            public const int Size = 8;

            public static void Write(byte[] array, int offset, ulong val)
            {
                array[offset + 0] = (byte)(val >> 0);
                array[offset + 1] = (byte)(val >> 8);
                array[offset + 2] = (byte)(val >> 16);
                array[offset + 3] = (byte)(val >> 24);
                array[offset + 4] = (byte)(val >> 32);
                array[offset + 5] = (byte)(val >> 40);
                array[offset + 6] = (byte)(val >> 48);
                array[offset + 7] = (byte)(val >> 56);
            }

            public static ulong Read(byte[] array, int offset)
            {
                return (ulong)array[offset + 0] << 0 |
                       (ulong)array[offset + 1] << 8 |
                       (ulong)array[offset + 2] << 16 |
                       (ulong)array[offset + 3] << 24 |
                       (ulong)array[offset + 4] << 32 |
                       (ulong)array[offset + 5] << 40 |
                       (ulong)array[offset + 6] << 48 |
                       (ulong)array[offset + 7] << 56;
            }
        }
    }
}
