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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ChunkIO.Test {
  [TestClass]
  public class ByteReaderTest {
    readonly string _filepath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

    [TestCleanup()]
    public void Cleanup() {
      if (File.Exists(_filepath)) File.Delete(_filepath);
    }

    [TestMethod]
    public void NoFile() {
      try {
        new ByteReader(_filepath);
        Assert.Fail("Must throw");
      } catch (FileNotFoundException) {
      }
      Assert.IsFalse(File.Exists(_filepath));
    }
  }
}
