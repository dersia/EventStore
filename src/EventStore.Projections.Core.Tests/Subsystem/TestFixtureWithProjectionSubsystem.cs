﻿using System;
using System.IO;
using System.Threading;
using EventStore.Common.Options;
using EventStore.Core;
using EventStore.Core.Bus;
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
		
		protected ProjectionsSubsystem Subsystem;
		protected const int WaitTimeoutMs = 3000;

		private readonly ManualResetEvent _stopReceived = new ManualResetEvent(false);
		private ProjectionSubsystemMessage.StopComponents _lastStopMessage;

		private readonly ManualResetEvent _startReceived = new ManualResetEvent(false);
		private ProjectionSubsystemMessage.StartComponents _lastStartMessage;
	
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
			
			Subsystem.MasterMainBus.Subscribe(new AdHocHandler<Message>(
				msg => {
					switch (msg) {
						case ProjectionSubsystemMessage.StartComponents start: {
							_lastStartMessage = start;
							_startReceived.Set();
							break;
						}
						case ProjectionSubsystemMessage.StopComponents stop: {
							_lastStopMessage = stop;
							_stopReceived.Set();
							break;
						}
					}
				}));

			Subsystem.Start();

			Given();
		}

		[OneTimeTearDown]
		public void TearDown() {
			_standardComponents.Db.Dispose();
			_standardComponents.TimerService.Dispose();
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

		protected ProjectionSubsystemMessage.StartComponents WaitForStartMessage
			(string timeoutMsg = null, bool failOnTimeout = true) {
			timeoutMsg = timeoutMsg ?? "Timed out waiting for Start Components";
			if (_startReceived.WaitOne(WaitTimeoutMs))
				return _lastStartMessage;
			if (failOnTimeout)
				Assert.Fail(timeoutMsg);
			return null;
		}

		protected ProjectionSubsystemMessage.StopComponents WaitForStopMessage(string timeoutMsg = null) {
			timeoutMsg = timeoutMsg ?? "Timed out waiting for Stop Components";
			if (_stopReceived.WaitOne(WaitTimeoutMs)) {
				return _lastStopMessage;
			}

			Assert.Fail(timeoutMsg);
			return null;
		}

		protected void ResetMessageEvents() {
			_stopReceived.Reset();
			_startReceived.Reset();
			_lastStopMessage = null;
			_lastStartMessage = null;
		}
	}
}
