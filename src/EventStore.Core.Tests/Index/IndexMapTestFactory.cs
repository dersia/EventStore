using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.Core.Index;
using EventStore.Core.Tests.TransactionLog;

namespace EventStore.Core.Tests.Index {
	public static class IndexMapTestFactory {
		public static IndexMap FromFile(string filename, int maxTablesPerLevel = 4,
			bool loadPTables = true, int cacheDepth = 16, bool skipIndexVerify = false,
			int threads = 1,
			int maxAutoMergeLevel = int.MaxValue,
			int pTableMaxReaderCount = TFChunkHelper.PTableMaxReaderCountDefault) {
			return IndexMap.FromFile(filename, maxTablesPerLevel, loadPTables, cacheDepth, skipIndexVerify, threads,
				maxAutoMergeLevel, pTableMaxReaderCount);
		}
	}
}
