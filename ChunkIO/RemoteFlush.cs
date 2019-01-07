using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChunkIO
{
    static class RemoteFlush
    {
        public static Task Flush(string fname, bool flushToDisk)
        {
            throw new NotImplementedException();
        }

        public static IDisposable CreateListener(string fname, Func<bool, Task> flush)
        {
            throw new NotImplementedException();
        }
    }
}
