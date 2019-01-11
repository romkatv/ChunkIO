using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    struct UserData
    {
        public const int Size = 16;

        public byte B0 { get; set; }
        public byte B1 { get; set; }
        public byte B2 { get; set; }
        public byte B3 { get; set; }
        public byte B4 { get; set; }
        public byte B5 { get; set; }
        public byte B6 { get; set; }
        public byte B7 { get; set; }
        public byte B8 { get; set; }
        public byte B9 { get; set; }
        public byte B10 { get; set; }
        public byte B11 { get; set; }
        public byte B12 { get; set; }
        public byte B13 { get; set; }
        public byte B14 { get; set; }
        public byte B15 { get; set; }

        public uint UInt0
        {
            get
            {
                return (uint)B0 << 0 |
                       (uint)B1 << 8 |
                       (uint)B2 << 16 |
                       (uint)B3 << 24;
            }
            set
            {
                B0 = (byte)(value >> 0 & byte.MaxValue);
                B1 = (byte)(value >> 8 & byte.MaxValue);
                B2 = (byte)(value >> 16 & byte.MaxValue);
                B3 = (byte)(value >> 24 & byte.MaxValue);
            }
        }

        public uint UInt1
        {
            get
            {
                return (uint)B4 << 0 |
                       (uint)B5 << 8 |
                       (uint)B6 << 16 |
                       (uint)B7 << 24;
            }
            set
            {
                B4 = (byte)(value >> 0 & byte.MaxValue);
                B5 = (byte)(value >> 8 & byte.MaxValue);
                B6 = (byte)(value >> 16 & byte.MaxValue);
                B7 = (byte)(value >> 24 & byte.MaxValue);
            }
        }

        public uint UInt2
        {
            get
            {
                return (uint)B8 << 0 |
                       (uint)B9 << 8 |
                       (uint)B10 << 16 |
                       (uint)B11 << 24;
            }
            set
            {
                B8 = (byte)(value >> 0 & byte.MaxValue);
                B9 = (byte)(value >> 8 & byte.MaxValue);
                B10 = (byte)(value >> 16 & byte.MaxValue);
                B11 = (byte)(value >> 24 & byte.MaxValue);
            }
        }

        public uint UInt3
        {
            get
            {
                return (uint)B12 << 0 |
                       (uint)B13 << 8 |
                       (uint)B14 << 16 |
                       (uint)B15 << 24;
            }
            set
            {
                B12 = (byte)(value >> 0 & byte.MaxValue);
                B13 = (byte)(value >> 8 & byte.MaxValue);
                B14 = (byte)(value >> 16 & byte.MaxValue);
                B15 = (byte)(value >> 24 & byte.MaxValue);
            }
        }

        public ulong ULong0
        {
            get { return UInt0 | (ulong)UInt1 << 32; }
            set
            {
                UInt0 = (uint)(value >> 0 & uint.MaxValue);
                UInt1 = (uint)(value >> 32 & uint.MaxValue);
            }
        }

        public ulong ULong1
        {
            get { return UInt2 | (ulong)UInt3 << 32; }
            set
            {
                UInt2 = (uint)(value >> 0 & uint.MaxValue);
                UInt3 = (uint)(value >> 32 & uint.MaxValue);
            }
        }

        public int Int0
        {
            get { return (int)UInt0; }
            set { UInt0 = (uint)value; }
        }

        public int Int1
        {
            get { return (int)UInt1; }
            set { UInt1 = (uint)value; }
        }

        public int Int2
        {
            get { return (int)UInt2; }
            set { UInt2 = (uint)value; }
        }

        public int Int3
        {
            get { return (int)UInt3; }
            set { UInt3 = (uint)value; }
        }

        public long Long0
        {
            get { return (long)ULong0; }
            set { ULong0 = (ulong)value; }
        }

        public long Long1
        {
            get { return (long)ULong1; }
            set { ULong1 = (ulong)value; }
        }

        public void WriteTo(byte[] array, int offset)
        {
            array[offset + 0] = B0;
            array[offset + 1] = B1;
            array[offset + 2] = B2;
            array[offset + 3] = B3;
            array[offset + 4] = B4;
            array[offset + 5] = B5;
            array[offset + 6] = B6;
            array[offset + 7] = B7;
            array[offset + 8] = B8;
            array[offset + 9] = B9;
            array[offset + 10] = B10;
            array[offset + 11] = B11;
            array[offset + 12] = B12;
            array[offset + 13] = B13;
            array[offset + 14] = B14;
            array[offset + 15] = B15;
        }

        public void ReadFrom(byte[] array, int offset)
        {
            B0 = array[offset + 0];
            B1 = array[offset + 1];
            B2 = array[offset + 2];
            B3 = array[offset + 3];
            B4 = array[offset + 4];
            B5 = array[offset + 5];
            B6 = array[offset + 6];
            B7 = array[offset + 7];
            B8 = array[offset + 8];
            B9 = array[offset + 9];
            B10 = array[offset + 10];
            B11 = array[offset + 11];
            B12 = array[offset + 12];
            B13 = array[offset + 13];
            B14 = array[offset + 14];
            B15 = array[offset + 15];
        }
    }
}
