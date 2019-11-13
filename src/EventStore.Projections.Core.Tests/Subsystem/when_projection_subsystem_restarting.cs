using System;
using System.Linq;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Projections.Core.Messages;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Subsystem {

	[TestFixture]
	public class when_projection_subsystem_restarted
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;
		private Message _restartResponse;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			
			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			ClearMessages();
			
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(
				new CallbackEnvelope(message => _restartResponse = message)));
			
			AllComponentsStopped(_startCorrelation);
		}

		[Test]
		public void should_respond_that_subsystem_is_restarting() {
			Assert.IsInstanceOf<ProjectionSubsystemMessage.SubsystemRestarting>(_restartResponse);
		}

		[Test]
		public void should_stop_the_subsystem() {
			Assert.AreEqual(1, GetStopMessages().Count);
		}
		
		[Test]
		public void should_start_the_subsystem() {
			var startMessages = GetStartMessages();
			Assert.AreEqual(1, startMessages.Count());
			Assert.AreNotEqual(_startCorrelation, startMessages[0].CorrelationId);
		}
	}
	
	[TestFixture]
	public class when_projection_subsystem_restarted_twice
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;
		private Message _secondRestartResponse;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			
			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			ClearMessages();
			
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(new NoopEnvelope()));
			AllComponentsStopped(_startCorrelation);
			
			var restartMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(restartMessage);
			
			AllComponentsStarted(restartMessage.CorrelationId);

			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(
				new CallbackEnvelope(message => _secondRestartResponse = message)));
			AllComponentsStopped(restartMessage.CorrelationId);
		}

		[Test]
		public void should_respond_success_on_second_restart() {
			Assert.IsInstanceOf<ProjectionSubsystemMessage.SubsystemRestarting>(_secondRestartResponse);
		}

		[Test]
		public void should_stop_the_subsystem() {
			Assert.AreEqual(2, GetStopMessages().Count);
		}
		
		[Test]
		public void should_start_the_subsystem() {
			Assert.AreEqual(2, GetStartMessages().Count);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_restarting_and_node_becomes_unknown
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

			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(new NoopEnvelope()));

			// Become unknown before components stopped
			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			AllComponentsStopped(_startCorrelation);
		}

		[Test]
		public void should_stop_the_subsystem_for_the_restart() {
			Assert.AreEqual(1, GetStopMessages().Count);
		}

		[Test]
		public void should_not_start_the_subsystem() {
			Assert.IsEmpty(GetStartMessages());
		}
	}
	
	[TestFixture]
	public class when_projection_subsystem_starting_and_told_to_restart
		: TestFixtureWithProjectionSubsystem {
		private Message _restartResponse;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			// Restart subsystem before fully started
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(
				new CallbackEnvelope(message => _restartResponse = message)));
		}

		[Test]
		public void should_report_restart_failure() {
			Assert.IsInstanceOf<ProjectionSubsystemMessage.InvalidSubsystemRestart>(_restartResponse);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_stopping_and_told_to_restart
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;
		private Message _restartResponse;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;
			AllComponentsStarted(_startCorrelation);

			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			// Restart subsystem before fully stopped
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(
				new CallbackEnvelope(message => _restartResponse = message)));
		}

		[Test]
		public void should_report_restart_failure() {
			Assert.IsInstanceOf<ProjectionSubsystemMessage.InvalidSubsystemRestart>(_restartResponse);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_restarting_and_told_to_restart
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;
		private Message _restartResponse;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;
			AllComponentsStarted(_startCorrelation);
			// First restart
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(new NoopEnvelope()));

			// Restart subsystem before finished restart
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(
				new CallbackEnvelope(message => _restartResponse = message)));
		}

		[Test]
		public void should_report_restart_failure() {
			Assert.IsInstanceOf<ProjectionSubsystemMessage.InvalidSubsystemRestart>(_restartResponse);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_stopped_and_told_to_restart
		: TestFixtureWithProjectionSubsystem {
		private Message _restartResponse;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			// Don't start the subsystem
			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			// Restart subsystem while stopped
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(
				new CallbackEnvelope(message => _restartResponse = message)));
		}

		[Test]
		public void should_report_restart_failure() {
			Assert.IsInstanceOf<ProjectionSubsystemMessage.InvalidSubsystemRestart>(_restartResponse);
		}
	}
}
