using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO {
  using FILETIME = System.Runtime.InteropServices.ComTypes.FILETIME;

  static class FileId {
    [StructLayout(LayoutKind.Sequential)]
    struct BY_HANDLE_FILE_INFORMATION {
      public uint FileAttributes;
      public FILETIME CreationTime;
      public FILETIME LastAccessTime;
      public FILETIME LastWriteTime;
      public uint VolumeSerialNumber;
      public uint FileSizeHigh;
      public uint FileSizeLow;
      public uint NumberOfLinks;
      public uint FileIndexHigh;
      public uint FileIndexLow;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool GetFileInformationByHandle(SafeFileHandle handle, out BY_HANDLE_FILE_INFORMATION info);

    // Returns unique file ID. Two file handles have the same ID if they are attached to the same kernel object.
    // That is, writes through one handle can be seen through the other.
    public static byte[] Get(SafeFileHandle file) {
      if (!GetFileInformationByHandle(file, out BY_HANDLE_FILE_INFORMATION info)) {
        int error = Marshal.GetLastWin32Error();
        throw new Exception($"GetFileInformationByHandle() failed with Win32 error {error}");
      }
      int offset = 0;
      var id = new byte[3 * UInt32LE.Size];
      UInt32LE.Write(id, ref offset, info.VolumeSerialNumber);
      UInt32LE.Write(id, ref offset, info.FileIndexLow);
      UInt32LE.Write(id, ref offset, info.FileIndexHigh);
      return id;
    }
  }
}
