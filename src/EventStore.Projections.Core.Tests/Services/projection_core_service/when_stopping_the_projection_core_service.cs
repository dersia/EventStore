using System;
using System.Linq;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Management;
using EventStore.Projections.Core.Services.Processing;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Services.projection_core_service {
	[TestFixture]
	public class when_stopping_the_projection_core_service_with_no_running_projections 
		: TestFixtureWithProjectionCoreService {
		private readonly Guid _stopCorrelationId = Guid.NewGuid();

		[SetUp]
		public override void Setup() {
			base.Setup();
			_service.Handle(new ProjectionCoreServiceMessage.StopCore(_stopCorrelationId));
		}
		
		[Test]
		public void should_handle_subcomponent_stopped() {
			var componentStopped = _consumer.HandledMessages
				.OfType<ProjectionCoreServiceMessage.SubComponentStopped>()
				.LastOrDefault(x => x.SubComponent == "ProjectionCoreService");
			Assert.IsNotNull(componentStopped);
			Assert.AreEqual(_stopCorrelationId, componentStopped.CorrelationId);
		}
	}

	[TestFixture]
	public class when_stopping_the_projection_core_service_with_running_projections 
		: TestFixtureWithProjectionCoreService  {
		private readonly Guid _projectionId = Guid.NewGuid();
		private readonly Guid _stopCorrelationId = Guid.NewGuid();

		[SetUp]
		public override void Setup() {
			base.Setup();
			_bus.Subscribe<CoreProjectionStatusMessage.Suspended>(_service);
			_service.Handle(new CoreProjectionManagementMessage.CreateAndPrepare(
				_projectionId, _workerId, "test-projection", 
				new ProjectionVersion(), ProjectionConfig.GetTest(),
				"JS", "fromStream('$user-admin').outputState()"));
			_service.Handle(new ProjectionCoreServiceMessage.StopCore(_stopCorrelationId));
		}

		[Test]
		public void should_handle_projection_suspended_message() {
			var suspended = _consumer.HandledMessages
				.OfType<CoreProjectionStatusMessage.Suspended>()
				.LastOrDefault(x => x.ProjectionId == _projectionId);
			Assert.IsNotNull(suspended);	
		}
		
		[Test]
		public void should_handle_subcomponent_stopped() {
			var componentStopped = _consumer.HandledMessages
				.OfType<ProjectionCoreServiceMessage.SubComponentStopped>()
				.LastOrDefault(x => x.SubComponent == "ProjectionCoreService");
			Assert.IsNotNull(componentStopped);
		}
	}
}
