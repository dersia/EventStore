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
	public class when_restarting_with_projection_type_system {
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
				new ProjectionCoreCoordinator(ProjectionType.System, timeoutScheduler, queues, publisher, envelope);

			// fully start
			StartComponents(initialCorrelationId);
			AllSubComponentsStarted(initialCorrelationId);
			
			// stopping
			StopComponents(initialCorrelationId);

			//clear queues for clearer testing
			queues[0].Messages.Clear();
		}

		private void StartComponents(Guid correlationId) {
			_coordinator.Handle(new ProjectionSubsystemMessage.StartComponents(correlationId));
		}

		private void StopComponents(Guid correlationId) {
			_coordinator.Handle(new ProjectionSubsystemMessage.StopComponents(correlationId));;
		}

		private void AllSubComponentsStarted(Guid correlationId) {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("EventReaderCoreService", correlationId));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreService", correlationId));
			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreServiceCommandReader", correlationId));
		}

		private void AllSubComponentsStopped(Guid correlationId) {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService", correlationId));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService", correlationId));
			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreServiceCommandReader", correlationId));
		}

		[Test]
		public void should_not_start_if_subcomponents_not_stopped() {
			/* None of the subcomponents stopped */

			StartComponents(Guid.NewGuid());
			
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}

		[Test]
		public void should_not_start_if_only_some_subcomponents_stopped() {
			/*Only some subcomponents stopped*/
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService", initialCorrelationId));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService", initialCorrelationId));

			StartComponents(Guid.NewGuid());
			
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}

		[Test]
		public void should_start_if_subcomponents_stopped_before_starting_components_again() {
			AllSubComponentsStopped(initialCorrelationId);
			
			StartComponents(Guid.NewGuid());
			
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}

		[Test]
		public void should_not_stop_if_all_subcomponents_not_started() {
			AllSubComponentsStopped(initialCorrelationId);

			var startCorrelationId = Guid.NewGuid();
			StartComponents(startCorrelationId);
			
			/*Not all subcomponents started*/
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("EventReaderCoreService", startCorrelationId));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreService", startCorrelationId));

			StopComponents(startCorrelationId);
			
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StopReader).Count);
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StopCore).Count);
		}

		[Test]
		public void should_not_stop_if_not_started() {
			AllSubComponentsStopped(initialCorrelationId);

			StopComponents(Guid.NewGuid());

			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StopReader).Count);
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StopCore).Count);
		}

		[Test]
		public void should_not_stop_if_correlation_id_is_different() {
			AllSubComponentsStopped(initialCorrelationId);

			var startCorrelationId = Guid.NewGuid();
			StartComponents(startCorrelationId);
			AllSubComponentsStarted(startCorrelationId);
			
			/* Use a different correlation id for stopping */
			var incorrectStopCorrelationId = Guid.NewGuid();
			StopComponents(incorrectStopCorrelationId);
			
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StopReader).Count);
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StopCore).Count);
		}
	}
}
