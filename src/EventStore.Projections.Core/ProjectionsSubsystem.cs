using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Common.Log;
using EventStore.Common.Options;
using EventStore.Core;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.AwakeReaderService;
using EventStore.Projections.Core.Messages;

namespace EventStore.Projections.Core {
	public sealed class ProjectionsSubsystem :ISubsystem,
		IHandle<SystemMessage.SystemCoreReady>,
		IHandle<SystemMessage.StateChangeMessage>,
		IHandle<CoreProjectionStatusMessage.Stopped>,
		IHandle<ProjectionSubsystemMessage.RestartSubsystem>,
		IHandle<ProjectionSubsystemMessage.ComponentStarted>,
		IHandle<ProjectionSubsystemMessage.ComponentStopped> {
		public InMemoryBus MasterMainBus {
			get { return _masterMainBus; }
		}
		
		private readonly int _projectionWorkerThreadCount;
		private readonly ProjectionType _runProjections;
		private readonly bool _startStandardProjections;
		private readonly TimeSpan _projectionsQueryExpiry;
		private readonly ILogger _logger = LogManager.GetLoggerFor<ProjectionsSubsystem>();
		public const int VERSION = 3;

		private IQueuedHandler _masterInputQueue;
		private InMemoryBus _masterMainBus;
		private InMemoryBus _masterOutputBus;
		private IDictionary<Guid, IQueuedHandler> _coreQueues;
		private Dictionary<Guid, IPublisher> _queueMap;
		private bool _subsystemStarted;

		private readonly bool _faultOutOfOrderProjections;
		
		private const int ComponentCount = 2; /* ProjectionManager & ProjectionCoreCoordinator */
		private bool _restarting;
		private int _pendingComponentStarts;
		private int _runningComponentCount;
		
		private VNodeState _nodeState;
		private SubsystemState _subsystemState = SubsystemState.NotReady;
		private Guid _currentCorrelationId;

		public ProjectionsSubsystem(int projectionWorkerThreadCount, ProjectionType runProjections,
			bool startStandardProjections, TimeSpan projectionQueryExpiry, bool faultOutOfOrderProjections) {
			if (runProjections <= ProjectionType.System)
				_projectionWorkerThreadCount = 1;
			else
				_projectionWorkerThreadCount = projectionWorkerThreadCount;

			_runProjections = runProjections;
			_startStandardProjections = startStandardProjections;
			_projectionsQueryExpiry = projectionQueryExpiry;
			_faultOutOfOrderProjections = faultOutOfOrderProjections;
		}

		public void Register(StandardComponents standardComponents) {
			_masterMainBus = new InMemoryBus("manager input bus");
			_masterInputQueue = QueuedHandler.CreateQueuedHandler(_masterMainBus, "Projections Master");
			_masterOutputBus = new InMemoryBus("ProjectionManagerAndCoreCoordinatorOutput");
			
			_masterMainBus.Subscribe<ProjectionSubsystemMessage.RestartSubsystem>(this);
			_masterMainBus.Subscribe<ProjectionSubsystemMessage.ComponentStarted>(this);
			_masterMainBus.Subscribe<ProjectionSubsystemMessage.ComponentStopped>(this);
			_masterMainBus.Subscribe<SystemMessage.SystemCoreReady>(this);
			_masterMainBus.Subscribe<SystemMessage.StateChangeMessage>(this);
			
			var projectionsStandardComponents = new ProjectionsStandardComponents(
				_projectionWorkerThreadCount,
				_runProjections,
				_masterOutputBus,
				_masterInputQueue,
				_masterMainBus, _faultOutOfOrderProjections);

			CreateAwakerService(standardComponents);
			_coreQueues =
				ProjectionCoreWorkersNode.CreateCoreWorkers(standardComponents, projectionsStandardComponents);
			_queueMap = _coreQueues.ToDictionary(v => v.Key, v => (IPublisher)v.Value);

			ProjectionManagerNode.CreateManagerService(standardComponents, projectionsStandardComponents, _queueMap,
				_projectionsQueryExpiry);
			projectionsStandardComponents.MasterMainBus.Subscribe<CoreProjectionStatusMessage.Stopped>(this);
		}
		
		private static void CreateAwakerService(StandardComponents standardComponents) {
			var awakeReaderService = new AwakeService();
			standardComponents.MainBus.Subscribe<StorageMessage.EventCommitted>(awakeReaderService);
			standardComponents.MainBus.Subscribe<StorageMessage.TfEofAtNonCommitRecord>(awakeReaderService);
			standardComponents.MainBus.Subscribe<AwakeServiceMessage.SubscribeAwake>(awakeReaderService);
			standardComponents.MainBus.Subscribe<AwakeServiceMessage.UnsubscribeAwake>(awakeReaderService);
		}

		public void Handle(SystemMessage.SystemCoreReady message) {
			if (_subsystemState != SubsystemState.NotReady) return;
			_subsystemState = SubsystemState.Ready;
			if (_nodeState == VNodeState.Master)
				StartComponents();
		}

		public void Handle(SystemMessage.StateChangeMessage message) {
			_nodeState = message.State;
			if (_nodeState == VNodeState.Master) {
				StartComponents();
			} else {
				StopComponents();
			}
		}

		private void StartComponents() {
			if (_nodeState != VNodeState.Master) {
				_logger.Debug("PROJECTIONS SUBSYSTEM: Not starting because node is not master. Node state: {nodeState}",
					_nodeState);
			}
			if (_subsystemState != SubsystemState.Ready && _subsystemState != SubsystemState.Stopped) {
				_logger.Debug("PROJECTIONS SUBSYSTEM: Not starting because system is not ready or stopped. State: {state}",
					_subsystemState);
				return;
			}
			if (_runningComponentCount != 0) {
				_logger.Warn("PROJECTIONS SUBSYSTEM: Subsystem is stopped, but components are still running.");
				return;
			}

			_currentCorrelationId = Guid.NewGuid();
			_logger.Info("PROJECTIONS SUBSYSTEM: Starting components. Correlation: {correlationId}", _currentCorrelationId);
			_subsystemState = SubsystemState.Starting;
			_pendingComponentStarts = ComponentCount;
			_masterMainBus.Publish(new ProjectionSubsystemMessage.StartComponents(_currentCorrelationId));
		}

		private void StopComponents() {
			if (_subsystemState != SubsystemState.Started) {
				_logger.Debug("PROJECTIONS SUBSYSTEM: Not stopping because system is not in a started state. State: {state}", _subsystemState);
				return;
			}
			
			_logger.Info("PROJECTIONS SUBSYSTEM: Stopping components. Correlation: {correlationId}", _currentCorrelationId);
			_subsystemState = SubsystemState.Stopping;
			_masterMainBus.Publish(new ProjectionSubsystemMessage.StopComponents(_currentCorrelationId));
		}
		
		public void Handle(ProjectionSubsystemMessage.RestartSubsystem message) {
			if (_restarting) {
				_logger.Info("PROJECTIONS SUBSYSTEM: Not restarting because subsystem is already being restarted.");
				return;
			}

			if (_subsystemState != SubsystemState.Started) {
				_logger.Info(
					"PROJECTIONS SUBSYSTEM: Not restarting because the subsystem is not started. State: {state}",
					_subsystemState);
				return;
			}

			_logger.Info("PROJECTIONS SUBSYSTEM: Restarting subsystem.");
			_restarting = true;
			StopComponents();
		}
		
		public void Handle(ProjectionSubsystemMessage.ComponentStarted message) {
			if (message.CorrelationId != _currentCorrelationId) {
				_logger.Debug(
					"PROJECTIONS SUBSYSTEM: Received component started for incorrect correlation id. " +
					"Requested: {startCorrelation} | Current: {currentCorrelation}",
					message.CorrelationId, _currentCorrelationId);
			}

			if (_pendingComponentStarts <= 0 || _subsystemState != SubsystemState.Starting)
				return;
			
			_logger.Debug("PROJECTIONS SUBSYSTEM: Component '{componentName}' started. Correlation: {correlation}",
				message.ComponentName, message.CorrelationId);
			_pendingComponentStarts--;
			_runningComponentCount++;
				
			if (_pendingComponentStarts == 0) {
				AllComponentsStarted();
			}
		}

		private void AllComponentsStarted() {
			_logger.Info("PROJECTIONS SUBSYSTEM: All components started. Correlation: {correlation}",
				_currentCorrelationId);
			_subsystemState = SubsystemState.Started;
			_masterMainBus.Publish(new SystemMessage.SubSystemInitialized("Projections"));

			if (_nodeState != VNodeState.Master) {
				_logger.Info("PROJECTIONS SUBSYSTEM: Node state is no longer Master. Stopping projections. Current state: {nodeState}",
					_nodeState);
				StopComponents();
			}
		}

		public void Handle(ProjectionSubsystemMessage.ComponentStopped message) {
			if (message.CorrelationId != _currentCorrelationId) {
				_logger.Debug(
					"PROJECTIONS SUBSYSTEM: Received component stopped for incorrect correlation id. " +
					"Requested: {stopCorrelation} | Current: {currentCorrelation}",
					message.CorrelationId, _currentCorrelationId);
				return;
			}

			if (_runningComponentCount <= 0 || _subsystemState != SubsystemState.Stopping)
				return;

			_logger.Debug("PROJECTIONS SUBSYSTEM: Component '{componentName}' stopped. Correlation: {correlation}",
				message.ComponentName, message.CorrelationId);
			_runningComponentCount--;
			
			if (_runningComponentCount == 0) {
				AllComponentsStopped();
			}
		}

		private void AllComponentsStopped() {
			_logger.Info("PROJECTIONS SUBSYSTEM: All components stopped. Correlation: {correlation}",
				_currentCorrelationId);
			_subsystemState = SubsystemState.Stopped;
			
			if (_restarting) {
				StartComponents();
				return;
			}

			if (_nodeState == VNodeState.Master) {
				_logger.Info("PROJECTIONS SUBSYSTEM: Node state has changed to Master. Starting projections.");
				StartComponents();
			}
		}

		public IEnumerable<Task> Start() {
			var tasks = new List<Task>();
			if (_subsystemStarted == false) {
				if (_masterInputQueue != null)
					tasks.Add(_masterInputQueue.Start());
				foreach (var queue in _coreQueues)
					tasks.Add(queue.Value.Start());
			}

			_subsystemStarted = true;
			return tasks;
		}

		public void Stop() {
			if (_subsystemStarted) {
				if (_masterInputQueue != null)
					_masterInputQueue.Stop();
				foreach (var queue in _coreQueues)
					queue.Value.Stop();
			}

			_subsystemStarted = false;
		}

		private readonly List<string> _standardProjections = new List<string> {
			"$by_category",
			"$stream_by_category",
			"$streams",
			"$by_event_type",
			"$by_correlation_id"
		};

		public void Handle(CoreProjectionStatusMessage.Stopped message) {
			if (_startStandardProjections) {
				if (_standardProjections.Contains(message.Name)) {
					_standardProjections.Remove(message.Name);
					var envelope = new NoopEnvelope();
					_masterMainBus.Publish(new ProjectionManagementMessage.Command.Enable(envelope, message.Name,
						ProjectionManagementMessage.RunAs.System));
				}
			}
		}

		private enum SubsystemState {
			NotReady,
			Ready,
			Starting,
			Started,
			Stopping,
			Stopped
		}
	}
}
