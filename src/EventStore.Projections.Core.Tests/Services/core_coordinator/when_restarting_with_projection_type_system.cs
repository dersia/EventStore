using System;
using System.Linq;
using NUnit.Framework;
using EventStore.Core.Data;
using EventStore.Projections.Core.Services.Management;
using EventStore.Common.Options;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.TransactionLog.LogRecords;
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
		private Guid stopCorrelationId = Guid.NewGuid();

		[SetUp]
		public void Setup() {
			queues = new List<FakePublisher>() {new FakePublisher()}.ToArray();
			publisher = new FakePublisher();

			_coordinator =
				new ProjectionCoreCoordinator(ProjectionType.System, timeoutScheduler, queues, publisher, envelope);
			_coordinator.Handle(new SystemMessage.SystemCoreReady());
			_coordinator.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			_coordinator.Handle(new SystemMessage.EpochWritten(new EpochRecord(0, 0, Guid.NewGuid(), 0, DateTime.Now)));

			//force stop
			_coordinator.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			//clear queues for clearer testing
			queues[0].Messages.Clear();
		}

		private void BecomeReady() {
			//become ready
			_coordinator.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			_coordinator.Handle(new SystemMessage.EpochWritten(new EpochRecord(0, 0, Guid.NewGuid(), 0, DateTime.Now)));
		}

		private void AllSubComponentsStarted() {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("EventReaderCoreService"));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreService"));
			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreServiceCommandReader"));
		}

		private void AllSubComponentsStopped() {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService", stopCorrelationId));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService", stopCorrelationId));
			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreServiceCommandReader", stopCorrelationId));
		}

		[Test]
		public void should_not_start_if_subcomponents_not_stopped() {
			AllSubComponentsStarted();

			BecomeReady();
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}

		[Test]
		public void should_not_start_if_only_some_subcomponents_stopped() {
			AllSubComponentsStarted();

			/*Only some subcomponents stopped*/
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService", stopCorrelationId));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService", stopCorrelationId));

			BecomeReady();
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}

		[Test]
		public void should_start_if_subcomponents_stopped_before_becoming_ready() {
			AllSubComponentsStarted();

			AllSubComponentsStopped();
			BecomeReady();
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}

		[Test]
		public void should_start_if_subcomponents_stopped_after_becoming_ready() {
			AllSubComponentsStarted();

			BecomeReady();
			AllSubComponentsStopped();
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}

		[Test]
		public void should_start_if_some_subcomponents_stopped_before_becoming_ready_and_some_after_becoming_ready() {
			AllSubComponentsStarted();

			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService", stopCorrelationId));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService", stopCorrelationId));

			BecomeReady();

			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreServiceCommandReader", stopCorrelationId));

			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}

		[Test]
		public void should_start_if_subcomponents_started_and_stopped_late_after_becoming_ready() {
			BecomeReady();
			AllSubComponentsStarted();
			AllSubComponentsStopped();
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}

		[Test]
		public void should_start_if_subcomponents_started_and_stopped_in_a_different_random_order() {
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("EventReaderCoreService"));
			BecomeReady();

			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreService"));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService", stopCorrelationId));

			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStarted("ProjectionCoreServiceCommandReader"));


			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreServiceCommandReader", stopCorrelationId));
			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService", stopCorrelationId));

			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count);
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count);
		}
		
		[Test]
		public void should_stop_and_start_readers_and_core_when_subsystem_restarted() {
			BecomeReady();
			AllSubComponentsStopped();
			AllSubComponentsStarted();
			queues[0].Messages.Clear();
			
			_coordinator.Handle(new ProjectionCoreServiceMessage.RestartSubComponents());

			var stopCore =
				queues[0].Messages.SingleOrDefault(x => x is ProjectionCoreServiceMessage.StopCore) as
					ProjectionCoreServiceMessage.StopCore;
			Assert.NotNull(stopCore, "StopCore");

			_coordinator.Handle(new ProjectionCoreServiceMessage.SubComponentStopped("ProjectionCoreService", stopCore.CorrelationId));
			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService", stopCore.CorrelationId));
			
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StopReader).Count, "StopReader");
			
			_coordinator.Handle(
				new ProjectionCoreServiceMessage.SubComponentStopped("EventReaderCoreService", stopCorrelationId));
			
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StartReader).Count, "StartReader");
			Assert.AreEqual(1, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StartCore).Count, "StartCore");
		}
		
		[Test]
		public void should_ignore_if_subsystem_restarted_and_not_running() {
			_coordinator.Handle(new ProjectionCoreServiceMessage.RestartSubComponents());
			
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ProjectionCoreServiceMessage.StopCore).Count, "StopReader");
			Assert.AreEqual(0, queues[0].Messages.FindAll(x => x is ReaderCoreServiceMessage.StopReader).Count, "StopReader");
		}
	}
}
