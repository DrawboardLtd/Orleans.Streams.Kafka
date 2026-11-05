using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using System;

namespace Orleans.Streams.Kafka.Core
{
	/// <summary>
	/// Same cache size and logger naming as <see cref="SimpleQueueAdapterCache"/>, but every queue cache is a
	/// <see cref="StaleCursorEvictingQueueCache"/>.
	/// </summary>
	public class StaleCursorEvictingQueueAdapterCache : IQueueAdapterCache
	{
		private readonly int _cacheSize;
		private readonly string _providerName;
		private readonly ILoggerFactory _loggerFactory;
		private readonly TimeSpan _idleEviction;
		private readonly TimeProvider _timeProvider;

		public StaleCursorEvictingQueueAdapterCache(
			SimpleQueueCacheOptions options,
			string providerName,
			ILoggerFactory loggerFactory,
			TimeSpan idleEviction,
			TimeProvider timeProvider = null
		)
		{
			if (idleEviction <= TimeSpan.Zero)
				throw new ArgumentOutOfRangeException(nameof(idleEviction), idleEviction, "Must be greater than zero.");

			_cacheSize = options.CacheSize;
			_providerName = providerName;
			_loggerFactory = loggerFactory;
			_idleEviction = idleEviction;
			_timeProvider = timeProvider;
		}

		public IQueueCache CreateQueueCache(QueueId queueId)
			=> new StaleCursorEvictingQueueCache(
				new SimpleQueueCache(
					_cacheSize,
					_loggerFactory.CreateLogger($"{typeof(SimpleQueueCache).FullName}.{_providerName}.{queueId}")
				),
				_loggerFactory.CreateLogger($"{typeof(StaleCursorEvictingQueueCache).FullName}.{_providerName}.{queueId}"),
				_idleEviction,
				_timeProvider
			);
	}
}
