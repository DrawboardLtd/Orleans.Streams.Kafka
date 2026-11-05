using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Core;
using Xunit;

namespace Orleans.Streams.Kafka.Tests;

public class KafkaAdapterCacheSelectionTests
{
	private static readonly SimpleQueueCacheOptions CacheOptions = new() { CacheSize = 4096 };

	[Fact]
	public void T5_default_is_ten_minutes()
		=> Assert.Equal(TimeSpan.FromMinutes(10), new KafkaStreamOptions().CacheCursorIdleEviction);

	[Fact]
	public void T5_zero_produces_the_plain_SimpleQueueAdapterCache()
	{
		var options = new KafkaStreamOptions { CacheCursorIdleEviction = TimeSpan.Zero };

		var cache = KafkaAdapterFactory.CreateAdapterCache(options, CacheOptions, "kafka", NullLoggerFactory.Instance);

		Assert.IsType<SimpleQueueAdapterCache>(cache);
		Assert.IsType<SimpleQueueCache>(cache.CreateQueueCache(QueueId.GetQueueId("q", 0, 0)));
	}

	[Fact]
	public void T5_non_zero_produces_the_evicting_cache_per_queue()
	{
		var options = new KafkaStreamOptions();

		var cache = KafkaAdapterFactory.CreateAdapterCache(options, CacheOptions, "kafka", NullLoggerFactory.Instance);

		Assert.IsType<StaleCursorEvictingQueueAdapterCache>(cache);
		var first = cache.CreateQueueCache(QueueId.GetQueueId("q", 0, 0));
		var second = cache.CreateQueueCache(QueueId.GetQueueId("q", 1, 0));
		Assert.IsType<StaleCursorEvictingQueueCache>(first);
		Assert.NotSame(first, second);
		Assert.Equal(new SimpleQueueCache(4096, NullLogger.Instance).GetMaxAddCount(), first.GetMaxAddCount());
	}

	[Fact]
	public void T5_negative_is_rejected()
	{
		var options = new KafkaStreamOptions { CacheCursorIdleEviction = TimeSpan.FromSeconds(-1) };

		Assert.Throws<ArgumentOutOfRangeException>(
			() => KafkaAdapterFactory.CreateAdapterCache(options, CacheOptions, "kafka", NullLoggerFactory.Instance)
		);
	}
}
