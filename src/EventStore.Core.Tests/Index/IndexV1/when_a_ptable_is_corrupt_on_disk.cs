using System;
using System.IO;
using EventStore.Common.Options;
using EventStore.Core.Exceptions;
using EventStore.Core.Index;
using EventStore.Core.Settings;
using EventStore.Core.Tests.TransactionLog;
using NUnit.Framework;

namespace EventStore.Core.Tests.Index.IndexV1 {
	[TestFixture(PTableVersions.IndexV1)]
	[TestFixture(PTableVersions.IndexV2)]
	[TestFixture(PTableVersions.IndexV3)]
	[TestFixture(PTableVersions.IndexV4)]
	public class when_a_ptable_is_corrupt_on_disk : SpecificationWithDirectory {
		private string _filename;
		private PTable _table;
		private string _copiedfilename;
		protected byte _ptableVersion = PTableVersions.IndexV1;

		public when_a_ptable_is_corrupt_on_disk(byte version) {
			_ptableVersion = version;
		}

		[SetUp]
		public override void SetUp() {
			base.SetUp();

			_filename = GetTempFilePath();
			_copiedfilename = GetTempFilePath();
			var mtable = new HashListMemTable(_ptableVersion, maxSize: 10);
			mtable.Add(0x010100000000, 0x0001, 0x0001);
			mtable.Add(0x010500000000, 0x0001, 0x0002);
			_table = PTable.FromMemtable(mtable, _filename, ESConsts.PTableInitialReaderCount, TFChunkHelper.PTableMaxReaderCountDefault);
			_table.Dispose();
			File.Copy(_filename, _copiedfilename);
			using (var f = new FileStream(_copiedfilename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)) {
				f.Seek(130, SeekOrigin.Begin);
				f.WriteByte(0x22);
			}
		}

		[TearDown]
		public override void TearDown() {
			_table.MarkForDestruction();
			_table.WaitForDisposal(1000);

			base.TearDown();
		}

		[Test]
		public void the_hash_is_invalid() {
			var exc = Assert.Throws<CorruptIndexException>(() => PTable.FromFile(_copiedfilename, ESConsts.PTableInitialReaderCount, TFChunkHelper.PTableMaxReaderCountDefault, 16, false));
			Assert.IsInstanceOf<HashValidationException>(exc.InnerException);
		}
	}
}
