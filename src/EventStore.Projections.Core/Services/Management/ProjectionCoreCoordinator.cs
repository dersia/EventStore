using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Options;
using EventStore.Core.Bus;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Projections.Core.Messages;
using EventStore.Common.Log;

namespace EventStore.Projections.Core.Services.Management {
	public class ProjectionCoreCoordinator
		: IHandle<ProjectionManagementMessage.Internal.RegularTimeout>,
			IHandle<ProjectionSubsystemMessage.StartComponents>,
			IHandle<ProjectionSubsystemMessage.StopComponents>,
			IHandle<ProjectionCoreServiceMessage.SubComponentStarted>,
			IHandle<ProjectionCoreServiceMessage.SubComponentStopped> {
		private readonly ILogger Log = LogManager.GetLoggerFor<ProjectionCoreCoordinator>();
		private readonly ProjectionType _runProjections;
		private readonly TimeoutScheduler[] _timeoutSchedulers;

		private readonly Dictionary<Guid, IPublisher> _queues = new Dictionary<Guid, IPublisher>();
		private readonly IPublisher _publisher;
		private readonly IEnvelope _publishEnvelope;

		private int _pendingSubComponentsStarts;
		private int _activeSubComponents;
		
		private Guid _runCorrelationId = Guid.Empty;
		private CoreCoordinatorState _currentState = CoreCoordinatorState.Stopped;
		
		private const string ComponentName = "ProjectionCoreCoordinator";

		public ProjectionCoreCoordinator(
			ProjectionType runProjections,
			TimeoutScheduler[] timeoutSchedulers,
			IPublisher[] queues,
			IPublisher publisher,
			IEnvelope publishEnvelope) {
			_runProjections = runProjections;
			_timeoutSchedulers = timeoutSchedulers;
			_queues = queues.ToDictionary(_ => Guid.NewGuid(), q => q);
			_publisher = publisher;
			_publishEnvelope = publishEnvelope;
		}

		public void Handle(ProjectionManagementMessage.Internal.RegularTimeout message) {
			ScheduleRegularTimeout();
			for (var i = 0; i < _timeoutSchedulers.Length; i++)
				_timeoutSchedulers[i].Tick();
		}

		public void Handle(ProjectionSubsystemMessage.StartComponents message) {
			if (_currentState > CoreCoordinatorState.Stopped) {
				Log.Debug("PROJECTIONS: Projection Core Coordinator already started. Correlation: {correlation}",
					message.CorrelationId);
				return;
			}
			_runCorrelationId = message.CorrelationId;
			Log.Debug("PROJECTIONS: Projection Core Coordinator component starting. Correlation: {correlation}",
				_runCorrelationId);
			Start();
		}

		public void Handle(ProjectionSubsystemMessage.StopComponents message) {
			if (_currentState <= CoreCoordinatorState.Stopped) {
				Log.Debug("PROJECTIONS: Projection Core Coordinator already stopped. Correlation: {correlation}",
					message.CorrelationId);
				return;
			}
			if (_runCorrelationId != message.CorrelationId) {
				Log.Debug("PROJECTIONS: Projection Core Coordinator received stop request for incorrect correlation id." +
				          "Current: {correlationId}. Requested: {requestedCorrelationId}", _runCorrelationId, message.CorrelationId);
				return;
			}
			Stop(message);
		}

		private void ScheduleRegularTimeout() {
			// TODO: We may need to change this to allow getting timeouts while stopping
			if (_currentState <= CoreCoordinatorState.Stopped)
				return;
			_publisher.Publish(
				TimerMessage.Schedule.Create(
					TimeSpan.FromMilliseconds(100),
					_publishEnvelope,
					new ProjectionManagementMessage.Internal.RegularTimeout()));
		}

		private void Start() {
			if (_currentState > CoreCoordinatorState.Stopped) {
				Log.Warn("PROJECTIONS: Projection Core Coordinated tried to start when already started.");
				return;
			}
			Log.Debug("PROJECTIONS: Starting Projections Core Coordinator");
			_pendingSubComponentsStarts = 0;
			_activeSubComponents = 0;
			_currentState = CoreCoordinatorState.Starting;
			
			ScheduleRegularTimeout();
			
			foreach (var queue in _queues.Values) {
				queue.Publish(new ReaderCoreServiceMessage.StartReader(_runCorrelationId));
				_pendingSubComponentsStarts += 1 /*EventReaderCoreService*/;

				if (_runProjections >= ProjectionType.System) {
					queue.Publish(new ProjectionCoreServiceMessage.StartCore(_runCorrelationId));
					_pendingSubComponentsStarts += 1 /*ProjectionCoreService*/
					                               + 1 /*ProjectionCoreServiceCommandReader*/;
				}
			}
		}

		private void Stop(ProjectionSubsystemMessage.StopComponents message) {
			if (_currentState != CoreCoordinatorState.Started) {
				Log.Debug("PROJECTIONS: Projections Core Coordinator trying to stop when not started. " +
				          "Current state: {currentState}. StopCorrelation: {correlation}", _currentState,
					message.CorrelationId);
				return;
			}

			Log.Debug("PROJECTIONS: Stopping Projections Core Coordinator");
			_currentState = CoreCoordinatorState.Stopping;
			foreach (var queue in _queues) {
				if (_runProjections >= ProjectionType.System) {
					 queue.Value.Publish(new ProjectionCoreServiceMessage.StopCore(queue.Key));
				} else {
					 // TODO: Find out why projections still run even when ProjectionType.None
					 queue.Value.Publish(new ReaderCoreServiceMessage.StopReader(queue.Key));
				}
			}
		}
		
		public void Handle(ProjectionCoreServiceMessage.SubComponentStarted message) {
			if (_currentState != CoreCoordinatorState.Starting) {
				Log.Debug("PROJECTIONS: Projection Core Coordinator received SubComponent Started when not starting. " +
					"SubComponent: {subComponent}, CorrelationId: {correlationId}, CurrentState: {currentState}",
					message.SubComponent, message.CorrelationId, _currentState);
				return;
			}
			if (message.CorrelationId != _runCorrelationId) {
				Log.Debug("PROJECTIONS: Projection Core Coordinator received SubComponent Started for wrong correlation id. " +
					"SubComponent: {subComponent}, RequestedCorrelation: {requestedCorrelation}, CurrentCorrelation: {currentCorrelation}",
					message.SubComponent, message.CorrelationId, _runCorrelationId);
				return;
			}
			_pendingSubComponentsStarts--;
			_activeSubComponents++;
			Log.Debug("PROJECTIONS: SubComponent Started: {subComponent}", message.SubComponent);

			if (_pendingSubComponentsStarts == 0) {
				_publisher.Publish(
					new ProjectionSubsystemMessage.ComponentStarted(ComponentName, _runCorrelationId));
				_currentState = CoreCoordinatorState.Started;
			}
		}

		public void Handle(ProjectionCoreServiceMessage.SubComponentStopped message) {
			if (_currentState != CoreCoordinatorState.Stopping) {
				Log.Debug("PROJECTIONS: Projection Core Coordinator received SubComponent Stopped when not stopping. " +
				          "SubComponent: {subComponent}, CorrelationId: {correlationId}, CurrentState: {currentState}",
					message.SubComponent, message.CorrelationId, _currentState);
				return;
			}
			_activeSubComponents--;
			Log.Debug("PROJECTIONS: SubComponent Stopped: {subComponent}", message.SubComponent);

			if (message.SubComponent == "ProjectionCoreService") {
				if (!_queues.TryGetValue(message.CorrelationId, out var queue))
					return;
				queue.Publish(new ReaderCoreServiceMessage.StopReader(message.CorrelationId));
			}

			if (_activeSubComponents == 0) {
				_publisher.Publish(
					new ProjectionSubsystemMessage.ComponentStopped(ComponentName, _runCorrelationId));
				_currentState = CoreCoordinatorState.Stopped;
			}
		}

		public void SetupMessaging(IBus bus) {
			bus.Subscribe<ProjectionCoreServiceMessage.SubComponentStarted>(this);
			bus.Subscribe<ProjectionCoreServiceMessage.SubComponentStopped>(this);
			bus.Subscribe<ProjectionSubsystemMessage.StartComponents>(this);
			bus.Subscribe<ProjectionSubsystemMessage.StopComponents>(this);
			if (_runProjections >= ProjectionType.System) {
				bus.Subscribe<ProjectionManagementMessage.Internal.RegularTimeout>(this);
			}
		}

		private enum CoreCoordinatorState {
			Stopped,
			Stopping,
			Starting,
			Started
		}
	}
}
