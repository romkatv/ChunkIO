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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  public struct Event<T> {
    public Event(DateTime timestamp, T value) {
      Timestamp = timestamp;
      Value = value;
    }

    public DateTime Timestamp { get; }
    public T Value { get; }
  }

  public abstract class EventEncoder<T> : ITimeSeriesEncoder<Event<T>> {
    BinaryWriter _writer = null;

    public DateTime EncodePrimary(Stream strm, Event<T> e) {
      RefreshWriter(strm);
      Encode(_writer, e.Value, isPrimary: true);
      return e.Timestamp;
    }

    public void EncodeSecondary(Stream strm, Event<T> e) {
      RefreshWriter(strm);
      _writer.Write(e.Timestamp.ToUniversalTime().Ticks);
      Encode(_writer, e.Value, isPrimary: false);
    }

    protected abstract void Encode(BinaryWriter writer, T val, bool isPrimary);

    void RefreshWriter(Stream strm) {
      if (_writer != null && ReferenceEquals(strm, _writer.BaseStream)) return;
      _writer?.Dispose();
      _writer = new BinaryWriter(strm, Encoding.UTF8, leaveOpen: true);
    }
  }

  public abstract class EventDecoder<T> : ITimeSeriesDecoder<Event<T>> {
    BinaryReader _reader = null;

    public void DecodePrimary(Stream strm, DateTime t, out Event<T> val) {
      RefreshReader(strm);
      val = new Event<T>(t, Decode(_reader, isPrimary: true));
    }

    public bool DecodeSecondary(Stream strm, out Event<T> val) {
      RefreshReader(strm);
      // This check assumes that BinaryReader has no internal buffer, which is true as of Jan 2019 but
      // it's not guaranteed to stay that way. BinaryReader has PeekChar() but no PeekByte(), even though
      // the latter would be trivial to implement and would fit the API better.
      if (strm.Position == strm.Length) {
        val = default(Event<T>);
        return false;
      }
      val = new Event<T>(new DateTime(_reader.ReadInt64(), DateTimeKind.Utc), Decode(_reader, isPrimary: false));
      return true;
    }

    protected abstract T Decode(BinaryReader reader, bool isPrimary);

    void RefreshReader(Stream strm) {
      if (_reader != null && ReferenceEquals(strm, _reader.BaseStream)) return;
      _reader?.Dispose();
      _reader = new BinaryReader(strm, Encoding.UTF8, leaveOpen: true);
    }
  }
}
