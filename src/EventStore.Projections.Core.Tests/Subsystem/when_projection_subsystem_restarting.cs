using System;
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

			var startMsg = WaitForStartMessage();
			_startCorrelation = startMsg.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			ResetMessageEvents();

			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(
				new CallbackEnvelope(message => _restartResponse = message)));
		}

		[Test]
		public void should_respond_that_subsystem_is_restarting() {
			Assert.IsInstanceOf<ProjectionSubsystemMessage.SubsystemRestarting>(_restartResponse);
		}

		[Test]
		public void should_stop_the_subsystem() {
			var stopMsg = WaitForStopMessage();
			Assert.AreEqual(_startCorrelation, stopMsg.CorrelationId);
		}
		
		[Test]
		public void should_start_the_subsystem_when_fully_stopped() {
			WaitForStopMessage();
			AllComponentsStopped(_startCorrelation);
			var restartMsg = WaitForStartMessage("Timed out waiting for restart StartComponents");
			
			Assert.AreNotEqual(_startCorrelation, restartMsg.CorrelationId);
		}
	}
	
	[TestFixture]
	public class when_projection_subsystem_restarted_twice
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;
		private Guid _restartCorrelation;
		
		private Message _secondRestartResponse;

		protected override void Given() {
			// Start subsystem
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			 var startMsg = WaitForStartMessage();
			_startCorrelation = startMsg.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			// Restart subsystem
			ResetMessageEvents();
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(new NoopEnvelope()));

			WaitForStopMessage("Timed out waiting for StopComponents on first restart");
			AllComponentsStopped(_startCorrelation);

			var restartMsg = WaitForStartMessage("Timed out waiting for StartComponents on first restart");
			_restartCorrelation = restartMsg.CorrelationId;
			AllComponentsStarted(_restartCorrelation);

			// Restart subsystem again
			ResetMessageEvents();
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(
				new CallbackEnvelope(message => _secondRestartResponse = message)));
		}

		[Test]
		public void should_respond_success_on_second_restart() {
			Assert.IsInstanceOf<ProjectionSubsystemMessage.SubsystemRestarting>(_secondRestartResponse);
		}

		[Test]
		public void should_stop_the_subsystem() {
			var stopMsg = WaitForStopMessage("Timed out waiting for StopComponents on second restart");
			Assert.AreEqual(_restartCorrelation, stopMsg.CorrelationId);
		}
		
		[Test]
		public void should_start_the_subsystem() {
			WaitForStopMessage("Timed out waiting for StopComponents on second restart");
			AllComponentsStopped(_restartCorrelation);
			
			var restartMsg = WaitForStartMessage("Timed out waiting for StartComponents on second restart");
			Assert.AreNotEqual(_restartCorrelation, restartMsg.CorrelationId);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_restarting_and_node_becomes_unknown
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;
		private ProjectionSubsystemMessage.StopComponents _stopMsg;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMsg = WaitForStartMessage();
			_startCorrelation = startMsg.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			ResetMessageEvents();
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(new NoopEnvelope()));

			_stopMsg = WaitForStopMessage();
			
			// Become unknown before components stopped
			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			AllComponentsStopped(_startCorrelation);
		}

		[Test]
		public void should_stop_the_subsystem_for_the_restart() {
			Assert.AreEqual(_startCorrelation, _stopMsg.CorrelationId);
		}

		[Test]
		public void should_not_start_the_subsystem() {
			var startMsg = WaitForStartMessage(failOnTimeout: false);
			Assert.IsNull(startMsg);
		}
	}
	
	[TestFixture]
	public class when_projection_subsystem_starting_and_told_to_restart
		: TestFixtureWithProjectionSubsystem {
		private Message _restartResponse;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			WaitForStartMessage();
			
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

			var startMsg = WaitForStartMessage();
			_startCorrelation = startMsg.CorrelationId;
			AllComponentsStarted(_startCorrelation);

			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			WaitForStopMessage();

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

			var startMsg = WaitForStartMessage();
			_startCorrelation = startMsg.CorrelationId;
			AllComponentsStarted(_startCorrelation);
			
			// First restart
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem(new NoopEnvelope()));

			WaitForStopMessage();

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
