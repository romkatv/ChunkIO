using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    struct Tick<T>
    {
        public Tick(DateTime timestamp, T value)
        {
            Timestamp = timestamp;
            Value = value;
        }

        public DateTime Timestamp { get; }

        public T Value { get; }
    }

    abstract class TickEncoder<T> : ITimeSeriesEncoder<Tick<T>>
    {
        public DateTime EncodePrimary(Stream strm, Tick<T> tick)
        {
            using (var writer = new BinaryWriter(strm)) Encode(writer, tick.Value, isPrimary: true);
            return tick.Timestamp;
        }

        public void EncodeSecondary(Stream strm, Tick<T> tick)
        {
            using (var writer = new BinaryWriter(strm))
            {
                writer.Write(tick.Timestamp.ToUniversalTime().Ticks);
                Encode(writer, tick.Value, isPrimary: false);
            }
        }

        protected abstract void Encode(BinaryWriter writer, T val, bool isPrimary);
    }

    abstract class TickDecoder<T> : ITimeSeriesDecoder<Tick<T>>
    {
        public void DecodePrimary(Stream strm, DateTime t, out Tick<T> val)
        {
            using (var reader = new BinaryReader(strm))
            {
                val = new Tick<T>(t, Decode(reader, isPrimary: true));
            }
        }

        public bool DecodeSecondary(Stream strm, out Tick<T> val)
        {
            if (strm.Position == strm.Length)
            {
                val = default(Tick<T>);
                return false;
            }
            using (var reader = new BinaryReader(strm))
            {
                val = new Tick<T>(new DateTime(reader.ReadInt64(), DateTimeKind.Utc), Decode(reader, isPrimary: false));
            }
            return true;
        }

        protected abstract T Decode(BinaryReader reader, bool isPrimary);
    }
}
