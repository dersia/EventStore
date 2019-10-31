using System;
using System.Linq;
using NUnit.Framework;
using EventStore.Projections.Core.Services.Management;
using EventStore.Common.Options;
using EventStore.Projections.Core.Messages;
using EventStore.Core.Tests.Fakes;
using EventStore.Core.Tests.Services.Replication;
using System.Collections.Generic;

namespace EventStore.Projections.Core.Tests.Services.core_coordinator {
	[TestFixture]
	public class when_stopping_with_projection_type_all {
		private FakePublisher[] queues;
		private FakePublisher publisher;
		private ProjectionCoreCoordinator _coordinator;
		private TimeoutScheduler[] timeoutScheduler = { };
		private FakeEnvelope envelope = new FakeEnvelope();

		private List<ProjectionCoreServiceMessage.StopCore> stopCoreMessages =
			new List<ProjectionCoreServiceMessage.StopCore>();
		
		private Guid startCorrelation = Guid.NewGuid();
		
		[SetUp]
		public void Setup() {
			queues = new List<FakePublisher>() {new FakePublisher()}.ToArray();
			publisher = new FakePublisher();

			_coordinator =
				new ProjectionCoreCoordinator(ProjectionType.All, timeoutScheduler, queues, publisher, envelope);
			_coordinator.Handle(new ProjectionSubsystemMessage.StartComponents(startCorrelation));

			//force stop
			_coordinator.Handle(new ProjectionSubsystemMessage.StopComponents(startCorrelation));
			
			// Publish SubComponent stopped messages for the projection core service
			stopCoreMessages = queues[0].Messages
				.FindAll(x => x.GetType() == typeof(ProjectionCoreServiceMessage.StopCore))
				.Select(x => x as ProjectionCoreServiceMessage.StopCore)
				.ToList();
			foreach(var msg in stopCoreMessages)
				_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService", msg.CorrelationId));
		}

		[Test]
		public void should_publish_stop_core_messages() {
			Assert.AreEqual(1, stopCoreMessages.Count);
		}
		
		[Test]
		public void should_publish_stop_reader_messages() {
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StopReader).Count);
		}
	}
}
