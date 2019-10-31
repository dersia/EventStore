using System;
using NUnit.Framework;
using EventStore.Projections.Core.Services.Management;
using EventStore.Common.Options;
using EventStore.Projections.Core.Messages;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Services.Replication;
using System.Collections.Generic;

namespace EventStore.Projections.Core.Tests.Services.core_coordinator {
	[TestFixture]
	public class when_stopping_with_projection_type_none {
		private FakePublisher[] queues;
		private FakePublisher publisher;
		private ProjectionCoreCoordinator _coordinator;
		private TimeoutScheduler[] timeoutScheduler = { };
		private FakeEnvelope envelope = new FakeEnvelope();
		
		private Guid startCorrelation = Guid.NewGuid();

		[SetUp]
		public void Setup() {
			queues = new List<FakePublisher>() {new FakePublisher()}.ToArray();
			publisher = new FakePublisher();

			_coordinator =
				new ProjectionCoreCoordinator(ProjectionType.None, timeoutScheduler, queues, publisher, envelope);
			_coordinator.Handle(new ProjectionSubsystemMessage.StartComponents(startCorrelation));

			//force stop
			_coordinator.Handle(new ProjectionSubsystemMessage.StopComponents(startCorrelation));
		}

		[Test]
		public void should_publish_stop_reader_messages() {
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StopReader).Count);
		}

		[Test]
		public void should_not_publish_stop_core_messages() {
			Assert.AreEqual(0,
				queues[0].Messages.FindAll(x => x.GetType() == typeof(ProjectionCoreServiceMessage.StopCore)).Count);
		}
	}
}
