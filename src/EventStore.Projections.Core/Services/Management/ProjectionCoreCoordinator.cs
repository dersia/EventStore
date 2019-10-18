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
		private bool _started;
		private readonly IPublisher _publisher;
		private readonly IEnvelope _publishEnvelope;

		private int _pendingSubComponentsStarts = 0;
		private int _activeSubComponents = 0;
		private bool _newInstanceWaiting = false;
		
		private bool _running;
		private Guid _runCorrelationId = Guid.Empty;

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
			if (_started) {
				Log.Debug("PROJECTIONS: Projection Core Coordinator already started. Correlation: {correlation}",
					message.CorrelationId);
				return;
			}

			_runCorrelationId = message.CorrelationId;
			_running = true;
			Log.Debug("PROJECTIONS: Projection Core Coordinator component starting. Correlation: {correlation}",
				_runCorrelationId);
			StartWhenConditionsAreMet();
		}

		public void Handle(ProjectionSubsystemMessage.StopComponents message) {
			if (!_started) {
				Log.Debug("PROJECTIONS: Projection Core Coordinator already stopped. Correlation: {correlation}",
					message.CorrelationId);
				return;
			}
			
			if (_runCorrelationId != message.CorrelationId) {
				Log.Debug("PROJECTIONS: Projection Core Coordinator received stop request for incorrect correlation id." +
				          "Current: {correlationId}. Requested: {requestedCorrelationId}", _runCorrelationId, message.CorrelationId);
				return;
			}
			_running = false;
			StartWhenConditionsAreMet();
		}

		private void StartWhenConditionsAreMet() {
			//run if and only if these conditions are met
			if (_running) {
				if (!_started) {
					if (_pendingSubComponentsStarts + _activeSubComponents != 0) {
						_newInstanceWaiting = true;
						return;
					} else {
						_newInstanceWaiting = false;
						_pendingSubComponentsStarts = 0;
						_activeSubComponents = 0;
					}

					Log.Debug("PROJECTIONS: Starting Projections Core Coordinator");
					Start();
				}
			} else {
				if (_started) {
					Log.Debug("PROJECTIONS: Stopping Projections Core Coordinator");
					Stop();
				}
			}
		}

		private void ScheduleRegularTimeout() {
			// TODO: We may need to change this to allow getting timeouts while stopping
			if (!_started)
				return;
			_publisher.Publish(
				TimerMessage.Schedule.Create(
					TimeSpan.FromMilliseconds(100),
					_publishEnvelope,
					new ProjectionManagementMessage.Internal.RegularTimeout()));
		}

		private void Start() {
			if (_started)
				throw new InvalidOperationException();
			_started = true;
			ScheduleRegularTimeout();
			foreach (var queue in _queues.Values) {
				queue.Publish(new ReaderCoreServiceMessage.StartReader());
				_pendingSubComponentsStarts += 1 /*EventReaderCoreService*/;

				if (_runProjections >= ProjectionType.System) {
					queue.Publish(new ProjectionCoreServiceMessage.StartCore(_runCorrelationId));
					_pendingSubComponentsStarts += 1 /*ProjectionCoreService*/
					                               + 1 /*ProjectionCoreServiceCommandReader*/;
				} else {
					_publisher.Publish(
						new ProjectionSubsystemMessage.ComponentStarted("ProjectionCoreCoordinator",
							_runCorrelationId));
				}
			}
		}

		private void Stop() {
			if (_started) {
				_started = false;
				foreach (var queue in _queues) {
					if (_runProjections >= ProjectionType.System) {
						queue.Value.Publish(new ProjectionCoreServiceMessage.StopCore(queue.Key));
					} else {
						// TODO: Find out why projections still run even when ProjectionType.None
						queue.Value.Publish(new ReaderCoreServiceMessage.StopReader(queue.Key));
					}
				}
			}
		}
		
		public void Handle(ProjectionCoreServiceMessage.SubComponentStarted message) {
			_pendingSubComponentsStarts--;
			_activeSubComponents++;
			Log.Debug("PROJECTIONS: SubComponent Started: {subComponent}", message.SubComponent);

			if (_pendingSubComponentsStarts == 0) {
				_publisher.Publish(
					new ProjectionSubsystemMessage.ComponentStarted("ProjectionCoreCoordinator", _runCorrelationId));
			}
			if (_newInstanceWaiting)
				StartWhenConditionsAreMet();
		}

		public void Handle(ProjectionCoreServiceMessage.SubComponentStopped message) {
			_activeSubComponents--;
			Log.Debug("PROJECTIONS: SubComponent Stopped: {subComponent}", message.SubComponent);

			if (message.SubComponent == "ProjectionCoreService") {
				if (!_queues.TryGetValue(message.CorrelationId, out var queue))
					return;
				queue.Publish(new ReaderCoreServiceMessage.StopReader(message.CorrelationId));
			}

			if (_activeSubComponents == 0) {
				_publisher.Publish(
					new ProjectionSubsystemMessage.ComponentStopped("ProjectionCoreCoordinator", _runCorrelationId));
			}
			if (_newInstanceWaiting)
				StartWhenConditionsAreMet();
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
	}
}
