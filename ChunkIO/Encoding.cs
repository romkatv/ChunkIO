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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  static class UInt32LE {
    public const int Size = 4;

    public static void Write(byte[] array, ref int offset, uint val) {
      array[offset++] = (byte)(val >> 0);
      array[offset++] = (byte)(val >> 8);
      array[offset++] = (byte)(val >> 16);
      array[offset++] = (byte)(val >> 24);
    }

    public static uint Read(byte[] array, ref int offset) {
      return (uint)array[offset++] << 0 |
             (uint)array[offset++] << 8 |
             (uint)array[offset++] << 16 |
             (uint)array[offset++] << 24;
    }
  }

  static class UInt64LE {
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
