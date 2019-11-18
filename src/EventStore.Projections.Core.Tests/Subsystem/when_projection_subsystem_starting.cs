using System;
using System.Threading;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Subsystem {
	[TestFixture]
	public class when_projection_subsystem_starting_and_all_components_started
		: TestFixtureWithProjectionSubsystem {
		private readonly ManualResetEventSlim _initializedReceived = new ManualResetEventSlim();
		
		protected override void Given() {
			Subsystem.MasterOutputBus.Subscribe(
				new AdHocHandler<SystemMessage.SubSystemInitialized>(msg => {
					_initializedReceived.Set();
				}));
			
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMsg = WaitForStartMessage();
			AllComponentsStarted(startMsg.CorrelationId);
		}

		[Test]
		public void should_publish_subsystem_initialized_when_all_components_started() {
			if (!_initializedReceived.Wait(WaitTimeoutMs)) {
				Assert.Fail("Timed out waiting for Subsystem Initialized");
			}
		}
	}

	[TestFixture]
	public class when_projection_subsystem_starting_and_wrong_components_started
		: TestFixtureWithProjectionSubsystem {
		private readonly ManualResetEventSlim _initializedReceived = new ManualResetEventSlim();
		
		protected override void Given() {
			Subsystem.MasterOutputBus.Subscribe(
				new AdHocHandler<SystemMessage.SubSystemInitialized>(msg => {
					_initializedReceived.Set();
				}));
			
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			
			WaitForStartMessage();

			var wrongCorrelation = Guid.NewGuid();

			AllComponentsStarted(wrongCorrelation);
		}

		[Test]
		public void should_ignore_component_started_for_incorrect_correlation() {
			Assert.False(_initializedReceived.Wait(WaitTimeoutMs));
		}
	}

	[TestFixture]
	public class when_projection_subsystem_started_and_master_changes
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMsg = WaitForStartMessage();
			_startCorrelation = startMsg.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));
		}

		[Test]
		public void should_stop_the_subsystem_with_start_correlation() {
			var stopMessage = WaitForStopMessage();
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

			var startMessage = WaitForStartMessage();
			_startCorrelation = startMessage.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));
			WaitForStopMessage();
			AllComponentsStopped(_startCorrelation);
		}

		[Test]
		public void should_allow_starting_the_subsystem_again() {
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			var startMessage = WaitForStartMessage();
			
			Assert.AreNotEqual(_startCorrelation, startMessage.CorrelationId);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_starting_and_node_becomes_unknown
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMessage = WaitForStartMessage();
			_startCorrelation = startMessage.CorrelationId;

			// Become unknown before components started
			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

			AllComponentsStarted(_startCorrelation);
		}

		[Test]
		public void should_stop_the_subsystem() {
			var stopMessage = WaitForStopMessage();
			Assert.AreEqual(_startCorrelation, stopMessage.CorrelationId);
		}
	}

	[TestFixture]
	public class when_projection_subsystem_stopping_and_node_becomes_master
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			var startMessage = WaitForStartMessage();
			_startCorrelation = startMessage.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			// Become unknown to stop the subsystem
			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));
			WaitForStopMessage();

			// Become master again before subsystem fully stopped
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

			AllComponentsStopped(_startCorrelation);
		}

		[Test]
		public void should_start_the_subsystem_again() {
			var startMessages = WaitForStartMessage();
			Assert.AreNotEqual(_startCorrelation, startMessages.CorrelationId);
		}
	}
}
