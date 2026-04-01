using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Utils;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Core
{
	using System.Globalization;

	public class KafkaAdapterFactory : IQueueAdapterFactory
	{
		private readonly string _name;
		private readonly KafkaStreamOptions _options;
		private readonly SerializationManager _serializationManager;
		private readonly ILoggerFactory _loggerFactory;
		private readonly IGrainFactory _grainFactory;
		private readonly IExternalStreamDeserializer _externalDeserializer;
		private readonly IQueueAdapterCache _adapterCache;
		private readonly IStreamQueueMapper _streamQueueMapper;
		private readonly ILogger<KafkaAdapterFactory> _logger;
		private readonly IDictionary<string, QueueProperties> _queueProperties;
		private readonly AdminClientBuilder _adminConfig;
		private readonly AdminClientConfig _config;

		public KafkaAdapterFactory(
			string name,
			KafkaStreamOptions options,
			SimpleQueueCacheOptions cacheOptions,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory
		) : this(name, options, cacheOptions, serializationManager, loggerFactory, grainFactory, null)
		{
			if (options.Topics.Any(topic => topic.IsExternal))
				throw new InvalidOperationException(
				"Cannot have external topic with no 'IExternalDeserializer' defined. Use 'AddJson' or 'AddAvro'"
			);
		}

		public KafkaAdapterFactory(
			string name,
			KafkaStreamOptions options,
			SimpleQueueCacheOptions cacheOptions,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory,
			IExternalStreamDeserializer externalDeserializer
		)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_name = name;
			_serializationManager = serializationManager;
			_loggerFactory = loggerFactory;
			_grainFactory = grainFactory;
			_externalDeserializer = externalDeserializer;
			_logger = loggerFactory.CreateLogger<KafkaAdapterFactory>();
			_adminConfig = new AdminClientBuilder(options.ToAdminProperties());

			if (options.Topics != null && options.Topics.Count == 0)
				throw new ArgumentNullException(nameof(options.Topics));

			_adapterCache = new SimpleQueueAdapterCache(
				cacheOptions,
				name,
				loggerFactory
			);

			_queueProperties = GetQueuesProperties().ToDictionary(q => q.QueueName);
			_logger.LogInformation("[KafkaFactory] Created for provider={Name}, topics configured={TopicCount}, queues discovered={QueueCount}, prefix={Prefix}, brokers={Brokers}",
				name, options.Topics?.Count ?? 0, _queueProperties.Count, options.TopicPrefix, options.BrokerList);
			foreach (var q in _queueProperties)
				_logger.LogInformation("[KafkaFactory] Queue: {QueueName} namespace={Namespace} partition={Partition}", q.Key, q.Value.Namespace, q.Value.PartitionId);
			_streamQueueMapper = new ExternalQueueMapper(_queueProperties.Values);

			_config = _options.ToAdminProperties();
		}

		public Task<IQueueAdapter> CreateAdapter()
			=> Task.FromResult((IQueueAdapter)new KafkaAdapter(
				_name,
				_options,
				_queueProperties,
				_serializationManager,
				_loggerFactory,
				_grainFactory,
				_externalDeserializer
			));

		public IQueueAdapterCache GetQueueAdapterCache()
			=> _adapterCache;

		public IStreamQueueMapper GetStreamQueueMapper()
			=> _streamQueueMapper;

		public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
			=> Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));

		public static KafkaAdapterFactory Create(IServiceProvider services, string name)
		{
			var streamsConfig = services.GetOptionsByName<KafkaStreamOptions>(name);
			var cacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(name);
			var deserializer = services.GetServiceByName<IExternalStreamDeserializer>(name);

			KafkaAdapterFactory factory;
			if (deserializer != null)
				factory = ActivatorUtilities.CreateInstance<KafkaAdapterFactory>(
					services,
					name,
					streamsConfig,
					cacheOptions,
					deserializer
				);
			else
				factory = ActivatorUtilities.CreateInstance<KafkaAdapterFactory>(
					services,
					name,
					streamsConfig,
					cacheOptions
				);

			return factory;
		}

		private IEnumerable<QueueProperties> GetQueuesProperties()
		{
			try
			{
				using var admin = _adminConfig.Build();
				var meta = admin.GetMetadata(_options.AdminRequestTimeout);
				var currentMetaTopics = meta.Topics.ToList();

				var prefix = _options.TopicPrefix ?? string.Empty;
				_logger.LogInformation("[KafkaFactory] GetQueuesProperties: prefix={Prefix}, configured topics={ConfiguredCount}, existing Kafka topics={ExistingCount}",
					prefix, _options.Topics?.Count ?? 0, currentMetaTopics.Count);

				var props = new List<QueueProperties>();
				var autoProps = new List<(QueueProperties props, short replicationFactor, ulong? retentionPeriodInMs, ulong? retentionBytes)>();

				foreach (var topic in _options.Topics)
				{
					if (!topic.AutoCreate || meta.Topics.Any(kt => kt.Topic == prefix + topic.Name))
						continue;

					var noOfPartitions = topic.Partitions == -1 ? 1 : topic.Partitions;
					for (var i = 0; i < noOfPartitions; i++)
					{
						var prop = CreateQueueProperty(topic, partitionId: i);
						props.Add(prop);
						autoProps.Add((prop, topic.ReplicationFactor, topic.RetentionPeriodInMs, topic.RetentionBytes));
					}
				}

				AsyncHelper.RunSync(() => CreateAutoTopics(admin, autoProps, prefix));

				// Wait for newly created topics to have leaders before producers start.
				// CreateTopicsAsync returns when the broker accepts the request, not when
				// topics are ready for produce/consume.
				if (autoProps.Count > 0)
				{
					var topicNames = autoProps
						.Select(a => prefix + a.props.Namespace)
						.Distinct()
						.ToHashSet();

					AsyncHelper.RunSync(() => WaitForTopicLeaders(admin, topicNames));
				}

				var retentionTargets = _options.Topics
					.Where(t => t.RetentionBytes.HasValue)
					.Select(t => (topicName: prefix + t.Name, retentionBytes: t.RetentionBytes.Value))
					.ToList();
				AsyncHelper.RunSync(() => EnforceTopicRetention(admin, retentionTargets, _logger));

				var joinedProps = (
					from kafkaTopic in currentMetaTopics
					join userTopic in _options.Topics on kafkaTopic.Topic equals prefix + userTopic.Name
					from partition in kafkaTopic.Partitions
					select CreateQueueProperty(userTopic, partition)
				).ToList();

				_logger.LogInformation("[KafkaFactory] Topic join: auto-created={AutoCreated}, joined from existing={Joined}, total queues={Total}",
					props.Count, joinedProps.Count, props.Count + joinedProps.Count);

				props.AddRange(joinedProps);

				return props;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to retrieve Kafka meta data. {@config}", _config);
				throw;
			}

			static QueueProperties CreateQueueProperty(
				TopicConfig userTopic,
				PartitionMetadata partition = null,
				int partitionId = -1
			) => new QueueProperties(
					userTopic.Name,
					(uint)(partition?.PartitionId ?? partitionId),
					userTopic.IsExternal,
					userTopic.ExternalContractType
				);
		}

		private static Task CreateAutoTopics(IAdminClient admin, IEnumerable<(QueueProperties prop, short replicationFactor, ulong? retentionPeriodInMs, ulong? retentionBytes)> autoQueues, string topicPrefix = "")
		{
			var topics = autoQueues
					.GroupBy(queue => queue.prop.Namespace)
					.Aggregate(
						new List<TopicSpecification>(),
						(result, queues) =>
						{
							var tuple = queues.First();

							var topicSpecification = new TopicSpecification
							                         {
								                         Name = topicPrefix + queues.Key,
								                         NumPartitions = queues.Count(),
								                         ReplicationFactor = tuple.replicationFactor
							                         };

							var configs = new Dictionary<string, string>();
							if (tuple.retentionPeriodInMs.HasValue)
								configs["retention.ms"] = tuple.retentionPeriodInMs.ToString();
							if (tuple.retentionBytes.HasValue)
								configs["retention.bytes"] = tuple.retentionBytes.ToString();
							if (configs.Count > 0)
								topicSpecification.Configs = configs;

							result.Add(topicSpecification);

							return result;
						}
					)
				;

			return topics.Any()
				? admin.CreateTopicsAsync(topics)
				: Task.CompletedTask;
		}

		private static async Task WaitForTopicLeaders(IAdminClient admin, ISet<string> topicNames)
		{
			const int maxAttempts = 20;
			const int delayMs = 500;

			for (var attempt = 0; attempt < maxAttempts; attempt++)
			{
				var meta = admin.GetMetadata(TimeSpan.FromSeconds(5));
				var allReady = topicNames.All(name =>
				{
					var topic = meta.Topics.FirstOrDefault(t => t.Topic == name);
					return topic != null
						&& !topic.Error.IsError
						&& topic.Partitions.Count > 0
						&& topic.Partitions.All(p => p.Leader >= 0);
				});

				if (allReady)
					return;

				await Task.Delay(delayMs);
			}
		}

		private static async Task EnforceTopicRetention(IAdminClient admin, IEnumerable<(string topicName, ulong retentionBytes)> topics, ILogger logger = null)
		{
			var topicList = topics.ToList();
			if (topicList.Count == 0)
				return;

			// Read current config to avoid unnecessary AlterConfigs calls that churn the controller log.
			var resources = topicList
				.Select(t => new ConfigResource { Type = ResourceType.Topic, Name = t.topicName })
				.ToList();

			var currentConfigs = await admin.DescribeConfigsAsync(resources);

			var toUpdate = new Dictionary<ConfigResource, List<ConfigEntry>>();
			foreach (var t in topicList)
			{
				var resource = new ConfigResource { Type = ResourceType.Topic, Name = t.topicName };
				var desiredValue = t.retentionBytes.ToString();

				var described = currentConfigs.FirstOrDefault(d => d.ConfigResource.Name == t.topicName);
				if (described != null)
				{
					var current = described.Entries.GetValueOrDefault("retention.bytes");
					if (current != null && current.Value == desiredValue)
						continue;
				}

				toUpdate[resource] = new List<ConfigEntry>
				{
					new ConfigEntry { Name = "retention.bytes", Value = desiredValue }
				};
			}

			if (toUpdate.Count == 0)
			{
				logger?.LogInformation("[KafkaFactory] All {TopicCount} topics already have correct retention.bytes, skipping AlterConfigs", topicList.Count);
				return;
			}

			logger?.LogInformation("[KafkaFactory] Updating retention.bytes on {UpdateCount}/{TopicCount} topics", toUpdate.Count, topicList.Count);
			await admin.AlterConfigsAsync(toUpdate);
		}
	}
}