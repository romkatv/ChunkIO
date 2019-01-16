using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  struct Tick<T> {
    public Tick(DateTime timestamp, T value) {
      Timestamp = timestamp;
      Value = value;
    }

    public DateTime Timestamp { get; }

    public T Value { get; }
  }

  abstract class TickEncoder<T> : ITimeSeriesEncoder<Tick<T>> {
    BinaryWriter _writer = null;

    public void Dispose() => Dispose(true);

    protected virtual void Dispose(bool disposing) {
      if (disposing) _writer?.Dispose();
    }

    public DateTime EncodePrimary(Stream strm, Tick<T> tick) {
      RefreshWriter(strm);
      Encode(_writer, tick.Value, isPrimary: true);
      return tick.Timestamp;
    }

    public void EncodeSecondary(Stream strm, Tick<T> tick) {
      RefreshWriter(strm);
      _writer.Write(tick.Timestamp.ToUniversalTime().Ticks);
      Encode(_writer, tick.Value, isPrimary: false);
    }

    protected abstract void Encode(BinaryWriter writer, T val, bool isPrimary);

    void RefreshWriter(Stream strm) {
      if (_writer != null && ReferenceEquals(strm, _writer.BaseStream)) return;
      _writer?.Dispose();
      _writer = new BinaryWriter(strm, System.Text.Encoding.UTF8, leaveOpen: true);
    }
  }

  abstract class TickDecoder<T> : ITimeSeriesDecoder<Tick<T>> {
    BinaryReader _reader = null;

    public void Dispose() => Dispose(true);

    protected virtual void Dispose(bool disposing) {
      if (disposing) _reader?.Dispose();
    }

    public void DecodePrimary(Stream strm, DateTime t, out Tick<T> val) {
      RefreshReader(strm);
      val = new Tick<T>(t, Decode(_reader, isPrimary: true));
    }

    public bool DecodeSecondary(Stream strm, out Tick<T> val) {
      RefreshReader(strm);
      // This check assumes that BinaryReader has no internal buffer, which is true as of Jan 2019 but
      // it's not guaranteed to stay that way. BinaryReader has PeekChar() but no PeekByte(), even though
      // the latter would be trivial to implement and would fit the API better.
      if (strm.Position == strm.Length) {
        val = default(Tick<T>);
        return false;
      }
      val = new Tick<T>(new DateTime(_reader.ReadInt64(), DateTimeKind.Utc), Decode(_reader, isPrimary: false));
      return true;
    }

    protected abstract T Decode(BinaryReader reader, bool isPrimary);

    void RefreshReader(Stream strm) {
      if (_reader != null && ReferenceEquals(strm, _reader.BaseStream)) return;
      _reader?.Dispose();
      _reader = new BinaryReader(strm, System.Text.Encoding.UTF8, leaveOpen: true);
    }
  }
}
