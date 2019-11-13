using System;
using EventStore.Core.Messaging;

namespace EventStore.Projections.Core.Messages {
	public class ProjectionSubsystemMessage {
	
		public class RestartSubsystem : Message  {
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public IEnvelope ReplyEnvelope { get; }
			
			public RestartSubsystem(IEnvelope replyEnvelope) {
				ReplyEnvelope = replyEnvelope;
			}
		}

		public class InvalidSubsystemRestart : Message {
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public string SubsystemState { get; }

			public InvalidSubsystemRestart(string subsystemState) {
				SubsystemState = subsystemState;
			}
		}

		public class SubsystemRestarting : Message {
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}
		}

		public class StartComponents : Message  {
			public Guid CorrelationId { get; }
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public StartComponents(Guid correlationId) {
				CorrelationId = correlationId;
			}
		}	
			
		public class ComponentStarted : Message  {
			public string ComponentName { get; }
			public Guid CorrelationId { get; }
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public ComponentStarted(string componentName, Guid correlationId) {
				ComponentName = componentName;
				CorrelationId = correlationId;
			}
		}	
	
		public class StopComponents : Message  {
			public Guid CorrelationId { get; }
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public StopComponents(Guid correlationId) {
				CorrelationId = correlationId;
			}
		}
		
		public class ComponentStopped : Message  {
			public string ComponentName { get; }
			public Guid CorrelationId { get; }
			private static readonly int TypeId = System.Threading.Interlocked.Increment(ref NextMsgId);

			public override int MsgTypeId {
				get { return TypeId; }
			}

			public ComponentStopped(string componentName, Guid correlationId) {
				ComponentName = componentName;
				CorrelationId = correlationId;
			}
		}
	}
}
