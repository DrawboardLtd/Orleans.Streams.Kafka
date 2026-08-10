using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Consumer;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SerializationContext = Orleans.Streams.Kafka.Serialization.SerializationContext;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterReceiver : IQueueAdapterReceiver
	{
		private readonly ILogger<KafkaAdapterReceiver> _logger;
		private readonly string _providerName;
		private readonly KafkaStreamOptions _options;
		private readonly SerializationManager _serializationManager;
		private readonly IGrainFactory _grainFactory;
		private readonly IExternalStreamDeserializer _externalDeserializer;
		private readonly QueueProperties _queueProperties;

		private IConsumer<byte[], byte[]> _consumer;
		private Task _commitPromise = Task.CompletedTask;
		private Task<IList<IBatchContainer>> _consumePromise;
		private long _pollCount;
		private long _emptyPollCount;

		// Enough to redo the Assign() from Initialize after a transient failure. _lastConsumedOffset
		// is the raw offset of the last message this receiver actually returned, so recovery can
		// resume from where we got to rather than from the configured start position.
		private TopicPartition _topicPartition;
		private Offset _initialOffset;
		private long? _lastConsumedOffset;

		public KafkaAdapterReceiver(
			string providerName,
			QueueProperties queueProperties,
			KafkaStreamOptions options,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory,
			IExternalStreamDeserializer externalDeserializer
		)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_providerName = providerName;
			_queueProperties = queueProperties;
			_serializationManager = serializationManager;
			_grainFactory = grainFactory;
			_externalDeserializer = externalDeserializer;
			_logger = loggerFactory.CreateLogger<KafkaAdapterReceiver>();
		}

		public Task Initialize(TimeSpan timeout)
		{
			_consumer = new ConsumerBuilder<byte[], byte[]>(_options.ToConsumerProperties())
				.SetErrorHandler((sender, errorEvent) =>
					_logger.LogError(
						"Consume error reason: {reason}, code: {code}, is broker error: {errorType}",
						errorEvent.Reason,
						errorEvent.Code,
						errorEvent.IsBrokerError
					))
				.Build();

			var offsetMode = Offset.Stored;
			switch (_options.ConsumeMode)
			{
				case ConsumeMode.LastCommittedMessage:
					offsetMode = Offset.Stored;
					break;
				case ConsumeMode.StreamEnd:
					offsetMode = Offset.End;
					break;
				case ConsumeMode.StreamStart:
					offsetMode = Offset.Beginning;
					break;
			}

			var kafkaTopicName = (_options.TopicPrefix ?? string.Empty) + _queueProperties.Namespace;
			_logger.LogInformation("[KafkaReceiver] Initialize: topic={Topic}, partition={Partition}, offset={Offset}, consumeMode={ConsumeMode}, brokers={Brokers}",
				kafkaTopicName, _queueProperties.PartitionId, offsetMode, _options.ConsumeMode, _options.BrokerList);

			_topicPartition = new TopicPartition(kafkaTopicName, (int)_queueProperties.PartitionId);
			_initialOffset = offsetMode;
			_consumer.Assign(new TopicPartitionOffset(_topicPartition, _initialOffset));

			return Task.CompletedTask;
		}

		public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
		{
			var consumerRef = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.

			if (consumerRef == null)
				return Task.FromResult<IList<IBatchContainer>>(new List<IBatchContainer>());

			var cancellationSource = new CancellationTokenSource();
			cancellationSource.CancelAfter(_options.PollBufferTimeout);

			_consumePromise = Task.Run(
				() => PollForMessages(
					maxCount,
					cancellationSource
				),
				cancellationSource.Token
			);

			return _consumePromise;
		}

		public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
		{
			KafkaBatchContainer batchWithHighestOffset = null;

			try
			{
				if (!messages.Any())
					return;

				batchWithHighestOffset = messages
					.Cast<KafkaBatchContainer>()
					.Max();

				_commitPromise = _consumer.Commit(batchWithHighestOffset);
				await _commitPromise;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to commit message offset: {@offset}", batchWithHighestOffset?.TopicPartitionOffSet);
				throw;
			}
		}

		public async Task Shutdown(TimeSpan timeout)
		{
			try
			{
				var tasks = new List<Task>();

				if (_commitPromise != null)
					tasks.Add(_commitPromise);

				if (_consumePromise != null)
					tasks.Add(_consumePromise);

				await Task.WhenAll(tasks);
			}
			finally
			{
				_consumer.Unassign();
				_consumer.Unsubscribe();
				_consumer.Close();
				_consumer = null;
			}
		}

		private async Task<IList<IBatchContainer>> PollForMessages(int maxCount, CancellationTokenSource cancellation)
		{
			// Declared outside the try so the transient-error path can hand back whatever this
			// poll already consumed. Consume() has advanced the consumer past those messages, so
			// dropping them loses them for good -- doubly so now that recovery re-assigns at the
			// offset after the last one we returned.
			var batches = new List<IBatchContainer>();

			try
			{
				for (var i = 0; i < maxCount && !cancellation.IsCancellationRequested; i++)
				{
					var consumeResult = _consumer.Consume(_options.PollTimeout);
					if (consumeResult == null)
						break;

					var batchContainer = consumeResult.ToBatchContainer(
						new SerializationContext
						{
							SerializationManager = _serializationManager,
							ExternalStreamDeserializer = _externalDeserializer
						},
						_queueProperties
					);

					await TrackMessage(batchContainer);

					batches.Add(batchContainer);
					_lastConsumedOffset = consumeResult.Offset.Value;
				}

				_pollCount++;
				if (batches.Count > 0)
				{
					_logger.LogDebug("[KafkaReceiver] Polled {Count} messages from topic={Topic}, partition={Partition}",
						batches.Count, _queueProperties.Namespace, _queueProperties.PartitionId);
					_emptyPollCount = 0;
				}
				else
				{
					_emptyPollCount++;
					if (_emptyPollCount == 1 || _emptyPollCount % 300 == 0) // log first empty poll, then every ~30s
						_logger.LogWarning("[KafkaReceiver] Empty poll #{EmptyCount} (total polls: {TotalPolls}) topic={Topic}, partition={Partition}",
							_emptyPollCount, _pollCount, _queueProperties.Namespace, _queueProperties.PartitionId);
				}

				return batches;
			}
			catch (OperationCanceledException ex) when (ex.CancellationToken.IsCancellationRequested)
			{
				return new List<IBatchContainer>();
			}
			catch (ConsumeException ex) when (IsTransientError(ex.Error))
			{
				// Transient broker errors (e.g. "Not coordinator" while __consumer_offsets
				// leader is still electing) are safe to retry.  Return empty so the
				// PersistentStreamPullingAgent retries on its normal poll interval instead
				// of entering its multi-minute error-backoff cycle.
				//
				// Re-polling alone is NOT enough to recover, which is why the re-Assign below
				// exists.  We assign manually (no Subscribe), so there is no group rebalance to
				// re-drive the assignment, and an Offset.Stored assignment resolves its start
				// position through the group coordinator.  If that resolution is what failed,
				// the partition is left with no valid fetch position and every subsequent
				// Consume() returns null forever -- the receiver reports empty polls for the
				// rest of the process lifetime while producers keep publishing.  Observed on a
				// silo boot that raced Redpanda's coordinator election: one NotCoordinatorForGroup
				// on a topic, then 6600 consecutive empty polls and zero messages consumed.
				_logger.LogWarning(ex,
					"[KafkaReceiver] Transient consume error (code={Code}) after {Consumed} message(s), re-assigning and retrying on next poll. topic={Topic}, partition={Partition}",
					ex.Error.Code, batches.Count, _queueProperties.Namespace, _queueProperties.PartitionId);

				TryReassign();

				return batches;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to poll for messages queueId: {@queueProperties}", _queueProperties);
				throw;
			}
			finally
			{
				cancellation.Dispose();
			}
		}

		/// <summary>
		/// Re-establishes the manual partition assignment after a transient consume error, so a
		/// partition left without a valid fetch position starts moving again.
		/// </summary>
		/// <remarks>
		/// Resumes at the message after the last one we returned, NOT at the configured start
		/// position: re-assigning at <see cref="Offset.Beginning"/> would replay the whole topic
		/// on every blip, and at <see cref="Offset.End"/> would silently skip whatever arrived
		/// while we were broken.  Only when nothing has been consumed yet -- which is the case
		/// this fix is really about, a failure during startup -- does it fall back to the
		/// configured offset, and that is exactly what <see cref="Initialize"/> already asked for.
		/// <para>
		/// Assign() is local: it records the position and lets the background fetcher resolve it,
		/// so a broker still in trouble surfaces on the next Consume() as another transient error
		/// and we simply come back through here. That makes this self-healing rather than a
		/// one-shot repair, at the cost of one Assign per failed poll while the outage lasts.
		/// </para>
		/// </remarks>
		private void TryReassign()
		{
			// Shutdown nulls the field, and it can land between the failed poll and this call.
			var consumerRef = _consumer;
			if (consumerRef == null)
				return;

			var resumeFrom = ResolveResumeOffset(_lastConsumedOffset, _initialOffset);

			try
			{
				consumerRef.Assign(new TopicPartitionOffset(_topicPartition, resumeFrom));

				_logger.LogInformation("[KafkaReceiver] Re-assigned topic={Topic}, partition={Partition} at offset={Offset}",
					_topicPartition.Topic, _queueProperties.PartitionId, resumeFrom);
			catch (Exception ex)
			{
				// Never let recovery be the thing that kills the poll: the caller is about to
				// return an empty batch either way, and the next poll gets another attempt.
				_logger.LogWarning(ex, "[KafkaReceiver] Failed to re-assign topic={Topic}, partition={Partition} at offset={Offset}",
					_queueProperties.Namespace, _queueProperties.PartitionId, resumeFrom);
			}
		}

		/// <summary>
		/// Where a re-assignment should resume: the message after the last one consumed, or the
		/// offset <see cref="Initialize"/> used when this receiver has not consumed anything yet.
		/// </summary>
		internal static Offset ResolveResumeOffset(long? lastConsumedOffset, Offset initialOffset)
			=> lastConsumedOffset.HasValue
				? new Offset(lastConsumedOffset.Value + 1)
				: initialOffset;

		private static bool IsTransientError(Error error)
			=> error.Code is Confluent.Kafka.ErrorCode.NotCoordinatorForGroup
				or Confluent.Kafka.ErrorCode.GroupCoordinatorNotAvailable
				or Confluent.Kafka.ErrorCode.NotLeaderForPartition
				or Confluent.Kafka.ErrorCode.LeaderNotAvailable
				or Confluent.Kafka.ErrorCode.RequestTimedOut
				or Confluent.Kafka.ErrorCode.BrokerNotAvailable;

		private Task TrackMessage(IBatchContainer container)
		{
			if (!_options.MessageTrackingEnabled)
				return Task.CompletedTask;

			var trackingGrain = _grainFactory.GetMessageTrackerGrain(_providerName, _queueProperties.QueueName);
			return trackingGrain.Track(new Immutable<IBatchContainer>(container));
		}
	}
}