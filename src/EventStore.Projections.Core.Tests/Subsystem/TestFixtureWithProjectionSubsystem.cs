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
		private StandardComponents _standardComponents;
		private readonly List<Message> _masterMainBusMessages = new List<Message>();
		private readonly List<Message> _masterOutputBusMessages = new List<Message>();
		
		protected ProjectionsSubsystem Subsystem;

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
				timerService, timeProvider: null, httpForwarder: null, httpServices: new HttpService[] { }, networkSendService: null);
		}

		[OneTimeSetUp]
		public void SetUp() {
			_standardComponents = CreateStandardComponents();

			Subsystem = new ProjectionsSubsystem(1, ProjectionType.All, true, TimeSpan.FromSeconds(3), true);
			Subsystem.Register(_standardComponents);

			// Unsubscribe from the actual components so we can test in isolation
			Subsystem.MasterMainBus.Unsubscribe<ProjectionSubsystemMessage.ComponentStarted>(Subsystem);
			Subsystem.MasterMainBus.Unsubscribe<ProjectionSubsystemMessage.ComponentStopped>(Subsystem);
			
			Subsystem.MasterMainBus.Subscribe(
				new AdHocHandler<Message>(msg => _masterMainBusMessages.Add(msg)));
			Subsystem.MasterOutputBus.Subscribe(
				new AdHocHandler<Message>(msg => _masterOutputBusMessages.Add(msg)));

			Subsystem.Start();

			Given();
		}

		[OneTimeTearDown]
		public void TearDown() {
			_standardComponents.Db.Dispose();
			_standardComponents.TimerService.Dispose();

			_masterMainBusMessages.Clear();
			_masterOutputBusMessages.Clear();
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

		protected void ClearMessages() {
			_masterMainBusMessages.Clear();
			_masterOutputBusMessages.Clear();
		}
		
		protected List<ProjectionSubsystemMessage.StartComponents> GetStartMessages() {
			var messages = _masterMainBusMessages.ToArray();
			return messages.OfType<ProjectionSubsystemMessage.StartComponents>().ToList();
		}
		
		protected List<ProjectionSubsystemMessage.StopComponents> GetStopMessages() {
			var messages = _masterMainBusMessages.ToArray();
			return messages.OfType<ProjectionSubsystemMessage.StopComponents>().ToList();
		}
		
		protected List<SystemMessage.SubSystemInitialized> GetSubsystemInitializedMessages() {
			var messages = _masterOutputBusMessages.ToArray();
			return messages.OfType<SystemMessage.SubSystemInitialized>().ToList();
		}
	}
}
