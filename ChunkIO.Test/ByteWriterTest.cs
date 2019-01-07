using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ChunkIO.Test
{
    [TestClass]
    public class ByteWriterTest
    {
        readonly string _filepath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        [TestCleanup()]
        public void Cleanup()
        {
            if (File.Exists(_filepath)) File.Delete(_filepath);
        }

        [TestMethod]
        public void NoWrites()
        {
            using (var writer = new ByteWriter(_filepath)) { }
        }
    }
}
