using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using EventStore.Common.Options;
using EventStore.Core;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Services.Transport.Http;
using EventStore.Core.Tests.TransactionLog;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Management;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Subsystem {
	public class TestFixtureWithProjectionSubsystem {
		protected ProjectionsSubsystem Subsystem;
		protected readonly List<Message> MasterMainBusMessages = new List<Message>();
		protected readonly List<Message> MasterOutputBusMessages = new List<Message>();

		private StandardComponents CreateStandardComponents() {
			var db = new TFChunkDb(TFChunkHelper.CreateDbConfig(Path.GetTempPath(), 0));
			var mainQueue = QueuedHandler.CreateQueuedHandler
			(new AdHocHandler<Message>(msg => {
				/* Ignore messages */
			}), "MainQueue");
			var mainBus = new InMemoryBus("mainBus");
			var threadBasedScheduler = new ThreadBasedScheduler(new RealTimeProvider());
			var timerService = new TimerService(threadBasedScheduler);
			
			return new StandardComponents(db, mainQueue, mainBus,
				timerService, timeProvider: null, httpForwarder: null, new HttpService[] { }, networkSendService: null);
		}

		[OneTimeSetUp]
		public void SetUp() {
			var standardComponents = CreateStandardComponents();
			
			Subsystem = new ProjectionsSubsystem(1, ProjectionType.All, true, TimeSpan.FromSeconds(3), true);
			Subsystem.Register(standardComponents);

			Subsystem.MasterMainBus.Subscribe(
				new AdHocHandler<Message>(msg => MasterMainBusMessages.Add(msg)));
			Subsystem.MasterOutputBus.Subscribe(
				new AdHocHandler<Message>(msg => MasterOutputBusMessages.Add(msg)));

			Subsystem.Start();

			Given();
		}

		protected virtual void Given() {
		}

		protected void AllComponentsStarted(Guid correlationId) {
			Subsystem.Handle(new ProjectionSubsystemMessage.ComponentStarted(
				nameof(ProjectionManager), correlationId));
			Subsystem.Handle(new ProjectionSubsystemMessage.ComponentStarted(
				nameof(ProjectionCoreCoordinator), correlationId));
		}

		protected void AllComponentsStopped(Guid correlationId) {
			Subsystem.Handle(new ProjectionSubsystemMessage.ComponentStopped(
				nameof(ProjectionManager), correlationId));
			Subsystem.Handle(new ProjectionSubsystemMessage.ComponentStopped(
				nameof(ProjectionCoreCoordinator), correlationId));
		}

		protected List<ProjectionSubsystemMessage.StartComponents> GetStartMessages() {
			var messages = MasterMainBusMessages.ToArray();
			return messages.OfType<ProjectionSubsystemMessage.StartComponents>().ToList();
		}
	}

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
			Assert.AreEqual(1, MasterOutputBusMessages
				.OfType<SystemMessage.SubSystemInitialized>().Count());
		}
	}
	
	[TestFixture]
	public class when_projection_subsystem_starting_and_wrong_components_started
		: TestFixtureWithProjectionSubsystem{
		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			
			var wrongCorrelation = Guid.NewGuid();
			
			AllComponentsStarted(wrongCorrelation);
		}

		[Test]
		public void should_ignore_component_started_for_incorrect_correlation() {
			Assert.IsEmpty(MasterOutputBusMessages.OfType<SystemMessage.SubSystemInitialized>());
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
			var stopMessage = MasterMainBusMessages.OfType<ProjectionSubsystemMessage.StopComponents>()
				.SingleOrDefault();
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
			MasterMainBusMessages.Clear();
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			
			Assert.AreEqual(1, MasterMainBusMessages
				.OfType<ProjectionSubsystemMessage.StartComponents>().Count());
		}
	}
	
	[TestFixture]
	public class when_projection_subsystem_restarted
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			
			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			MasterMainBusMessages.Clear();
			MasterOutputBusMessages.Clear();
			
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem());
			
			AllComponentsStopped(_startCorrelation);
		}

		[Test]
		public void should_stop_the_subsystem() {
			Assert.AreEqual(1, MasterMainBusMessages
				.OfType<ProjectionSubsystemMessage.StopComponents>().Count());
		}
		
		[Test]
		public void should_start_the_subsystem() {
			var startMessages = MasterMainBusMessages
				.OfType<ProjectionSubsystemMessage.StartComponents>().ToList();
			Assert.AreEqual(1, startMessages.Count());
			Assert.AreNotEqual(_startCorrelation, startMessages[0].CorrelationId);
		}
	}
	
	[TestFixture]
	public class when_projection_subsystem_restarted_twice
		: TestFixtureWithProjectionSubsystem {
		private Guid _startCorrelation;

		protected override void Given() {
			Subsystem.Handle(new SystemMessage.SystemCoreReady());
			Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));
			
			var startMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(startMessage);
			_startCorrelation = startMessage.CorrelationId;

			AllComponentsStarted(_startCorrelation);

			MasterMainBusMessages.Clear();
			MasterOutputBusMessages.Clear();
			
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem());
			AllComponentsStopped(_startCorrelation);
			
			var restartMessage = GetStartMessages().SingleOrDefault();
			Assert.NotNull(restartMessage);
			
			AllComponentsStarted(restartMessage.CorrelationId);
			
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem());
			AllComponentsStopped(restartMessage.CorrelationId);
		}

		[Test]
		public void should_stop_the_subsystem() {
			Assert.AreEqual(2, MasterMainBusMessages
				.OfType<ProjectionSubsystemMessage.StopComponents>().Count());
		}
		
		[Test]
		public void should_start_the_subsystem() {
			Assert.AreEqual(2, MasterMainBusMessages
				.OfType<ProjectionSubsystemMessage.StartComponents>().Count());
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

			MasterMainBusMessages.Clear();
			MasterOutputBusMessages.Clear();
			
			Subsystem.Handle(new ProjectionSubsystemMessage.RestartSubsystem());
			
			// Become unknown before components stopped
			Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));
			
			AllComponentsStopped(_startCorrelation);
		}

		[Test]
		public void should_stop_the_subsystem_for_the_restart() {
			Assert.AreEqual(1, MasterMainBusMessages
				.OfType<ProjectionSubsystemMessage.StopComponents>().Count());
		}
		
		[Test]
		public void should_not_start_the_subsystem() {
			Assert.IsEmpty(MasterMainBusMessages
				.OfType<ProjectionSubsystemMessage.StartComponents>());
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
				var stopMessages = MasterMainBusMessages
					.OfType<ProjectionSubsystemMessage.StopComponents>()
					.ToList();
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

				MasterMainBusMessages.Clear();
				MasterOutputBusMessages.Clear();

				// Become unknown to stop the subsystem
				Subsystem.Handle(new SystemMessage.BecomeUnknown(Guid.NewGuid()));

				// Become master again before subsystem fully stopped
				Subsystem.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid()));

				AllComponentsStopped(_startCorrelation);
			}

			[Test]
			public void should_start_the_subsystem_again() {
				var startMessages = MasterMainBusMessages
					.OfType<ProjectionSubsystemMessage.StartComponents>()
					.ToList();
				Assert.AreEqual(1, startMessages.Count);
				Assert.AreNotEqual(_startCorrelation, startMessages[0].CorrelationId);
			}
		}
	}
}
