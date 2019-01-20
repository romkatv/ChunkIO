# ChunkIO

ChunkIO is a file format with the following properties:

  * Data is written and read in chunks of arbitrary size.
  * Writes are append-only. Concurrent writes are disallowed.
  * Efficient chunk lookup: `O(1)` to find the first or the last chunk whose start file position is in the specified range.
  * ChunkIO files can be appended to and read in the face of corruptions and truncations. Corrupted chunks are ignored by readers.
  * Chunk overhead is 40 bytes, out of which 16 bytes are user-defined data. Plus additional overhead of 16 bytes for every 64KB of chunk data, which is used to implement efficient chunk lookup and to allow readers to continue past corrupted data.
  * No assumptions are made about ordering or atomicity of file writes. This makes ChunkIO usable on any filesystem.

## Examples

Using ChunkIO to store market data: [Example.cs](https://github.com/romkatv/ChunkIO/blob/master/Example/Example.cs).

## Distribution

The source code is all there is. There is no official NuGet package.

## Dependencies

No dependencies other than .NET Framework.

## Supported Platofrms

Lightly tested (and heavily used in production) on Windows 10 with .NET Framework 4.7.2.

## File Format

**Stability:** The file format may change without notice. No compatibility guarantees, neither forward nor backward, are provided.

All numbers are serialized in little-endian format. All hashes are SipHash 2.4.

### Meter

ChunkIO files have a fixed-size marker called *meter* at every file position divisible by 64K. The first meter is at the beginning of the file.

Meter bytes:

  * `[0, 8)`: Start position of the first chunk whose end position is after the meter. As a convention, chunks never start nor end immediately after a meter. Instead, such chunks are assumed to start/end at the 64KB boundary.
  * `[8, 16)`: Hash of bytes `[0, 8)`. Readers ignore meters whose hashes don't match.

### Chunk

Every chunk starts with a header, followed by content.

Chunk header bytes:

  * `[0, 16)`: User data. Can be anything.
  * `[16, 24)`: Content length. Currently required to be in `[0, 2^31)`.
  * `[24, 32)`: Hash of the content. Readers ignore content of chunks whose hash doesn't match.
  * `[32, 40)`: Hash of bytes `[0, 32)`. Readers ignore chunks whose header hash doesn't match.

## API

**Stability:** The API may change without notice. No compatibility guarantees, neither forward nor backward, are provided.

### Core

`ChunkWriter` and `ChunkReader` allow you to read and write ChunkIO files without making any assumptions about the content of chunks and user data embedded in their headers.

*This API is currently private.*

### Buffered IO

`BufferedWriter` and `BufferedReader` provide extra features on top of the core API.

  * Incremental writing and reading of chunks. This is what "buffered" in the class names refers to.
  * Chunk content compression.
  * Automatic flushing of data based on size and/or time triggers.
  * Remote flush, which allows readers to ask writers to flush their buffers even if they happen to run in different processes (communications is done via named pipes, whose names are derived from file names).
  * Binary search for chunks based on the user-supplied predicate for user data.

*This API is currently private.*

### Time Series

`TimeSeriesWriter` and `TimeSeriesReader` provide extra features on top of the buffered IO API.

  * Chunks are made of one or more individual records.
  * The first record in a chunk may use different encoding from the rest. This can be used to implement efficient delta-encoding of records that relies on the data-specific domain knowledge.
  * Chunks are timestamped. These timestamps are encoded in the user data section of chunk headers.
  * Chunks can be looked up by timestamp in `O(log(filesize))`.

### Events

`Event`, `EventEncoder` and `EventDecoder` are optional complimentary classes that can be used with the time series API. They add the following features:

  * Each record has a timestamp.
  * Records are encoded with `BinaryWriter` and decoded with `BinaryReader`.

## License

Apache License 2.0. See [LICENSE](https://github.com/romkatv/ChunkIO/blob/master/LICENSE).
