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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  // Port of the SipHash 2.4 reference C implementation.
  static class SipHash {
    public static ulong ComputeHash(byte[] array) => ComputeHash(array, 0, array.Length);

    // These keys say "romkatv/chunkio1" in ASCII.
    public static ulong ComputeHash(byte[] array, int offset, int count) =>
        ComputeHash(array, offset, count, 0x2f7674616b6d6f72, 0x316f696b6e756863);

    public static ulong ComputeHash(byte[] array, int offset, int count, ulong k0, ulong k1) {
      var v0 = 0x736f6d6570736575 ^ k0;
      var v1 = 0x646f72616e646f6d ^ k1;
      var v2 = 0x6c7967656e657261 ^ k0;
      var v3 = 0x7465646279746573 ^ k1;

      ulong b = (ulong)count << 56;

      for (int i = 0, e = count & ~7; i != e; i += 8) {
        ulong x = U8To64Le(array, offset + i);
        v3 ^= x;
        SipRound(ref v0, ref v1, ref v2, ref v3);
        SipRound(ref v0, ref v1, ref v2, ref v3);
        v0 ^= x;
      }

      for (int i = count & ~7; i != count; ++i) {
        b |= (ulong)array[offset + i] << (8 * (i & 7));
      }

      v3 ^= b;
      SipRound(ref v0, ref v1, ref v2, ref v3);
      SipRound(ref v0, ref v1, ref v2, ref v3);
      v0 ^= b;
      v2 ^= 0xff;

      SipRound(ref v0, ref v1, ref v2, ref v3);
      SipRound(ref v0, ref v1, ref v2, ref v3);
      SipRound(ref v0, ref v1, ref v2, ref v3);
      SipRound(ref v0, ref v1, ref v2, ref v3);

      return v0 ^ v1 ^ v2 ^ v3;
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void SipRound(ref ulong v0, ref ulong v1, ref ulong v2, ref ulong v3) {
      v0 += v1;
      v1 = Rotl(v1, 13);
      v1 ^= v0;
      v0 = Rotl(v0, 32);

      v2 += v3;
      v3 = Rotl(v3, 16);
      v3 ^= v2;

      v0 += v3;
      v3 = Rotl(v3, 21);
      v3 ^= v0;

      v2 += v1;
      v1 = Rotl(v1, 17);
      v1 ^= v2;
      v2 = Rotl(v2, 32);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ulong Rotl(ulong x, int b) => x << b | x >> 64 - b;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ulong U8To64Le(byte[] array, int offset) =>
        (ulong)array[offset + 0] << 0 |
        (ulong)array[offset + 1] << 8 |
        (ulong)array[offset + 2] << 16 |
        (ulong)array[offset + 3] << 24 |
        (ulong)array[offset + 4] << 32 |
        (ulong)array[offset + 5] << 40 |
        (ulong)array[offset + 6] << 48 |
        (ulong)array[offset + 7] << 56;
  }
}
