using System;
using System.Linq;
using System.Threading;
using EventStore.Core.Messages;
using EventStore.Projections.Core.Messages;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Subsystem {
	[TestFixture]
	public class when_projection_subsystem_starting_and_all_components_started
		: TestFixtureWithProjectionSubsystem {
		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);

			AllComponentsStarted(startMessage.CorrelationId);
		}

		[Test]
		public void should_publish_subsystem_initialized_when_all_components_started() {
			Assert.AreEqual(1, GetSubsystemInitializedMessages().Count);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_starting_and_wrong_components_started
		: TestFixtureWithProjectionSubsystem {
		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var wrongCorrelation = Guid.NewGuid();

			AllComponentsStarted(wrongCorrelation);
		}

		[Test]
		public void should_ignore_component_started_for_incorrect_correlation() {
			Assert.IsEmpty(GetSubsystemInitializedMessages());
		}
	}

	[TestFixture]
	public class when_projection_subsystem_started_and_master_changes
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));
		}

		[Test]
		public void should_stop_the_subsystem_with_start_correlation() {
			var stopMessage = GetStopMessages().SingleOrDefault();
			Assert.NotNull(stopMessage);
			Assert.AreEqual(_startCorrelation, stopMessage.CorrelationId);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_stopping_and_all_components_stopped
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			AllComponentsStopped(_startCorrelation);
		}

		[Test]
		public void should_allow_starting_the_subsystem_again() {
			ClearMessages();
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			Assert.AreEqual(1, GetStartMessages().Count);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_starting_and_node_becomes_unknown
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;

			// Become unknown before components started
			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			AllComponentsStarted(_startCorrelation);
		}

		[Test]
		public void should_stop_the_subsystem() {
			var stopMessages = GetStopMessages();
			Assert.AreEqual(1, stopMessages.Count);
			Assert.AreEqual(_startCorrelation, stopMessages[0].CorrelationId);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_stopping_and_node_becomes_master
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			ClearMessages();

			// Become unknown to stop the subsystem
			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			// Become master again before subsystem fully stopped
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			AllComponentsStopped(_startCorrelation);
		}

		[Test]
		public void should_start_the_subsystem_again() {
			var startMessages = GetStartMessages();
			Assert.AreEqual(1, startMessages.Count);
			Assert.AreNotEqual(_startCorrelation, startMessages[0].CorrelationId);
		}
	}
}
