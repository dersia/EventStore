using System;
using EventStore.Core.Messaging;

namespace EventStore.Projections.Core.Messages {
	public static class ReaderCoreServiceMessage {
		public class StartReader : Message {
			public Guid CorrelationId { get; }
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public StartReader(Guid correlationId) {
				CorrelationId = correlationId;
			}
		}

		public class StopReader : Message {
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}
			
			public Guid CorrelationId { get; }

			public StopReader(Guid correlationId) {
				CorrelationId = correlationId;
			}
		}

		public class ReaderTick : Message {
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			private readonly Action _action;

			public ReaderTick(Action action) {
				_action = action;
			}

			public Action Action {
				get { return _action; }
			}
		}
	}
}
