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
	public class when_restarting_with_projection_type_none {
		private FakePublisher[] queues;
		private FakePublisher publisher;
		private ProjectionCoreCoordinator _coordinator;
		private TimeoutScheduler[] timeoutScheduler = { };
		private FakeEnvelope envelope = new FakeEnvelope();
		private Guid initialCorrelationId = Guid.NewGuid();

		[SetUp]
		public void Setup() {
			queues = new List<FakePublisher>() {new FakePublisher()}.ToArray();
			publisher = new FakePublisher();

			_coordinator =
				new ProjectionCoreCoordinator(ProjectionType.None, timeoutScheduler, queues, publisher, envelope);
			
			StartComponents(initialCorrelationId);
			CoreReaderStarted(initialCorrelationId);

			//force stop
			StopComponents(initialCorrelationId);

			//clear queues for clearer testing
			queues[0].Messages.Clear();
		}

		private void StartComponents(Guid correlationId) {
			_coordinator.Handle(new ProjectionSubsystemMessage.StartComponents(correlationId));
		}

		private void StopComponents(Guid correlationId) {
			_coordinator.Handle(new ProjectionSubsystemMessage.StopComponents(correlationId));
		}

		private void CoreReaderStarted(Guid correlationId) {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("EventReaderCoreService", correlationId));
		}

		private void CoreReaderStopped(Guid correlationId) {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService", correlationId));
		}

		[Test]
		public void should_not_start_reader_if_subcomponents_not_stopped() {
			/* None of the subcomponents stopped */

			StartComponents(Guid.NewGuid());
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
		}

		
		[Test]
		public void should_start_reader_if_subcomponents_stopped_before_starting_components_again() {
			CoreReaderStopped(initialCorrelationId);

			StartComponents(Guid.NewGuid());
			
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
		}
		
		[Test]
		public void should_not_stop_reader_if_subcomponents_not_started_yet() {
			CoreReaderStopped(initialCorrelationId);

			var correlationId = Guid.NewGuid();
			StartComponents(correlationId);
			/* Don't respond with component started messages */
			StopComponents(correlationId);
			
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StopReader).Count);
		}
			
		[Test]
		public void should_not_stop_reader_if_subcomponents_not_started() {
			CoreReaderStopped(initialCorrelationId);

			StopComponents(Guid.NewGuid());
			
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StopReader).Count);
		}
	}
}
