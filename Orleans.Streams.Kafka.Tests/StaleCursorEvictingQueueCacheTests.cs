using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using Orleans.Streams.Kafka.Core;
using Xunit;
using Xunit.Abstractions;

namespace Orleans.Streams.Kafka.Tests;

public class StaleCursorEvictingQueueCacheTests
{
	// Passed to the cache explicitly; the scenarios do not depend on the option default.
	private static readonly TimeSpan IdleEviction = TimeSpan.FromMinutes(3);
	private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(5);

	private readonly ITestOutputHelper _output;
	private readonly ManualTimeProvider _clock = new();
	private readonly CapturingLoggerProvider _logs = new();

	public StaleCursorEvictingQueueCacheTests(ITestOutputHelper output)
	{
		_output = output;
	}

	private StaleCursorEvictingQueueCache NewEvictingCache(int size)
	{
		var factory = LoggerFactory.Create(b => b.AddProvider(_logs));
		return new StaleCursorEvictingQueueCache(
			new SimpleQueueCache(size, NullLogger.Instance),
			factory.CreateLogger("evicting"),
			IdleEviction,
			_clock
		);
	}

	// ------------------------------------------------------------------ T1

	/// <summary>
	/// Stream 0's consumer is stuck with a set cursor in the oldest bucket, the other 49 consumers drain everything.
	/// The cache reaches 10 buckets (under pressure) and the commit is frozen. After IdleEviction the next tick must purge,
	/// the committed offset must advance and the cache must not be under pressure. Returns what the recovery tick produced.
	/// </summary>
	private sealed record StuckOutcome(IQueueCache Cache, long FrozenAt, int Purged, long Committed, bool UnderPressure);

	private StuckOutcome RunStuckCursorScenario(IQueueCache cache)
	{
		var clock = _clock.GetUtcNow();
		var sim = new PullingAgentSimulation(cache) { BeforeEachTick = () => _clock.Advance(TickInterval) };
		sim.Start();
		sim.Tick(12);

		// Precondition, true for every cache: the stuck cursor froze purge and commit and the cache is under pressure.
		var frozenAt = sim.CommittedOffset;
		Assert.True(cache.IsUnderPressure(), "precondition: the stuck cursor must put the cache under pressure");
		sim.Tick(3);
		Assert.Equal(frozenAt, sim.CommittedOffset);
		Assert.True(cache.IsUnderPressure(), "precondition: still under pressure");
		_output.WriteLine($"before: committed={sim.CommittedOffset} underPressure={cache.IsUnderPressure()}");

		// IdleEviction (3 minutes, set explicitly, not the option default) and 1 second after the stuck cursor's last progress (its creation). The agent refreshed it on every tick in between.
		_clock.Advance(clock + IdleEviction + TimeSpan.FromSeconds(1) - _clock.GetUtcNow());
		var tick = sim.Tick();
		_output.WriteLine($"after {IdleEviction + TimeSpan.FromSeconds(1)}: purged={tick.Purged} committed={sim.CommittedOffset} underPressure={cache.IsUnderPressure()}");

		return new StuckOutcome(cache, frozenAt, tick.Purged, sim.CommittedOffset, cache.IsUnderPressure());
	}

	private static void AssertRecovered(StuckOutcome outcome)
	{
		Assert.True(outcome.Purged > 0, $"TryPurgeFromCache purged {outcome.Purged} items");
		Assert.True(outcome.Committed > outcome.FrozenAt, $"max purged offset stayed at {outcome.Committed}");
		Assert.False(outcome.UnderPressure, "the cache is still under pressure");
	}

	[Fact]
	public void T1_evicting_cache_recovers_from_a_stuck_cursor()
	{
		var outcome = RunStuckCursorScenario(NewEvictingCache(4096));
		var cache = (StaleCursorEvictingQueueCache)outcome.Cache;

		AssertRecovered(outcome);
		Assert.Equal(3682, outcome.Purged);
		Assert.Equal(3681, outcome.Committed);
		Assert.False(outcome.UnderPressure);
		Assert.Equal(1, cache.EvictedCursorCount);
		var warning = Assert.Single(_logs.Warnings);
		Assert.Equal(LogLevel.Warning, warning.Level);
		_output.WriteLine(warning.Message);
		Assert.Contains(new Guid(1, 0, 0, new byte[8]).ToString(), warning.Message);   // stream 0
		Assert.Contains(new EventSequenceToken(0).ToString(), warning.Message);        // last known token: its one delivered message
		Assert.Contains("00:03:01", warning.Message);                                  // idle for
	}

	[Fact]
	public void T1_plain_cache_stays_frozen()
	{
		// The identical scenario on the plain SimpleQueueCache fails its recovery assertions. Set EVICT_T1_SHOW_PLAIN_FAILURE=1 to see the raw failure.
		var outcome = RunStuckCursorScenario(new SimpleQueueCache(4096, NullLogger.Instance));
		if (Environment.GetEnvironmentVariable("EVICT_T1_SHOW_PLAIN_FAILURE") == "1")
		{
			AssertRecovered(outcome);
			return;
		}

		Assert.Equal(0, outcome.Purged);
		Assert.Equal(-1, outcome.Committed);
		Assert.True(outcome.UnderPressure);
		var failure = Assert.ThrowsAny<Xunit.Sdk.XunitException>(() => AssertRecovered(outcome));
		_output.WriteLine("plain SimpleQueueCache failed the recovery assertion: " + failure.Message);
		Assert.Contains("TryPurgeFromCache purged 0 items", failure.Message);
	}

	// ------------------------------------------------------------------ T2

	[Fact]
	public void T2_cursor_re_set_by_refresh_pins_again_and_is_evicted_again()
	{
		var cache = NewEvictingCache(4096);
		var created = _clock.GetUtcNow();
		var sim = new PullingAgentSimulation(cache) { BeforeEachTick = () => _clock.Advance(TickInterval) };
		sim.Start();
		sim.Tick(12);
		_clock.Advance(created + IdleEviction + TimeSpan.FromSeconds(1) - _clock.GetUtcNow());
		sim.Tick();                                   // first eviction; the same tick's Refresh(first token of stream 0) re-sets the cursor
		Assert.Equal(1, cache.EvictedCursorCount);
		var reSetAt = _clock.GetUtcNow();

		// The consumer is still Active and stuck, so the agent keeps refreshing it and nobody calls MoveNext: it pins again.
		sim.Tick(2);                                  // purge catches up to the re-set cursor's bucket, then stops
		var committed = sim.CommittedOffset;
		sim.Tick(13);
		Assert.True(cache.IsUnderPressure(), "the re-set cursor must pin the cache again");
		Assert.Equal(committed, sim.CommittedOffset);
		_output.WriteLine($"re-pinned: committed={sim.CommittedOffset} underPressure={cache.IsUnderPressure()}");

		// Fresh clock from the re-set, whatever Refresh calls came after it: kept just before IdleEviction, evicted at it.
		_clock.Advance(reSetAt + IdleEviction - TimeSpan.FromSeconds(1) - _clock.GetUtcNow());
		sim.Tick();
		Assert.Equal(1, cache.EvictedCursorCount);
		Assert.True(cache.IsUnderPressure());

		_clock.Advance(TimeSpan.FromSeconds(1));
		var tick = sim.Tick();
		Assert.Equal(2, cache.EvictedCursorCount);
		Assert.True(tick.Purged > 0);
		Assert.True(sim.CommittedOffset > committed);
		Assert.False(cache.IsUnderPressure());
		Assert.Equal(2, _logs.Warnings.Count);
	}

	[Fact]
	public void T2_evicted_cursor_behaves_like_a_disposed_one_and_refresh_re_sets_it_to_a_cached_token()
	{
		var cache = NewEvictingCache(100);   // buckets of 10
		var stream = new StreamId(Guid.NewGuid(), "Ns");
		cache.AddToCache(Enumerable.Range(0, 30).Select(i => (IBatchContainer)new TestBatch(stream.Guid, "Ns", i)).ToList());
		var cursor = cache.GetCacheCursor(stream, new EventSequenceToken(5));
		Assert.True(cursor.MoveNext());
		Assert.False(cache.TryPurgeFromCache(out _), "the cursor pins the oldest bucket");

		_clock.Advance(IdleEviction);
		Assert.True(cache.TryPurgeFromCache(out var purged));
		Assert.Equal(30, purged.Count);
		Assert.Equal(1, cache.EvictedCursorCount);

		Assert.False(cursor.MoveNext());
		Assert.Null(cursor.GetCurrent(out var exception));
		Assert.Null(exception);

		// D3: a later Refresh to a token that is still cached sets it again, and it pins again.
		cache.AddToCache(Enumerable.Range(30, 30).Select(i => (IBatchContainer)new TestBatch(stream.Guid, "Ns", i)).ToList());
		cursor.Refresh(new EventSequenceToken(45));
		Assert.True(cache.TryPurgeFromCache(out purged));
		Assert.Equal(10, purged.Count);                                      // tokens 30-39, the bucket older than the cursor's
		Assert.False(cache.TryPurgeFromCache(out _), "the re-set cursor pins the bucket holding token 45");
		Assert.True(cursor.MoveNext());
		Assert.Equal(45, ((TestBatch)cursor.GetCurrent(out _)).Offset);

		_clock.Advance(IdleEviction);
		Assert.True(cache.TryPurgeFromCache(out _));
		Assert.Equal(2, cache.EvictedCursorCount);
	}

	// ------------------------------------------------------------------ T3

	[Fact]
	public void T3_cursor_idle_at_the_head_is_never_evicted_and_delivers_after_refresh()
	{
		var cache = NewEvictingCache(100);
		var a = new StreamId(Guid.NewGuid(), "Ns");
		var b = new StreamId(Guid.NewGuid(), "Ns");
		IBatchContainer Batch(long offset) => new TestBatch(offset % 2 == 0 ? a.Guid : b.Guid, "Ns", offset);
		cache.AddToCache(Enumerable.Range(0, 30).Select(i => Batch(i)).ToList());

		var cursor = cache.GetCacheCursor(a, null);
		var delivered = 0;
		while (cursor.MoveNext())
			delivered++;
		Assert.True(cache.TryPurgeFromCache(out _), "an idle cursor pins nothing");

		_clock.Advance(TimeSpan.FromHours(1));
		for (var i = 0; i < 3; i++)
			cache.TryPurgeFromCache(out _);

		Assert.Equal(0, cache.EvictedCursorCount);
		Assert.Empty(_logs.Warnings);

		// New data for stream a arrives: the agent adds it to the cache and refreshes the cursor with its token.
		var token = new EventSequenceToken(30);
		cache.AddToCache(new List<IBatchContainer> { Batch(30) });
		cursor.Refresh(token);
		Assert.True(cursor.MoveNext());
		Assert.Equal(30, ((TestBatch)cursor.GetCurrent(out _)).Offset);
		Assert.False(cursor.MoveNext());
		Assert.Empty(_logs.Warnings);
		_output.WriteLine($"idle cursor delivered {delivered} messages, then 1 after Refresh; evicted={cache.EvictedCursorCount}");
	}

	// ------------------------------------------------------------------ T4

	[Fact]
	public void T4_cursor_that_keeps_advancing_within_the_window_is_never_evicted()
	{
		var cache = NewEvictingCache(100);
		var a = new StreamId(Guid.NewGuid(), "Ns");
		var b = new StreamId(Guid.NewGuid(), "Ns");
		cache.AddToCache(Enumerable.Range(0, 40).Select(i => (IBatchContainer)new TestBatch(i % 2 == 0 ? a.Guid : b.Guid, "Ns", i)).ToList());
		var cursor = cache.GetCacheCursor(a, new EventSequenceToken(0));

		var offsets = new List<long>();
		for (var i = 0; i < 10; i++)
		{
			_clock.Advance(TimeSpan.FromMinutes(2));   // 20 minutes in total, never 3 between two MoveNexts
			Assert.True(cursor.MoveNext());
			offsets.Add(((TestBatch)cursor.GetCurrent(out _)).Offset);
			cache.TryPurgeFromCache(out _);
			Assert.Equal(0, cache.EvictedCursorCount);
		}

		Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i * 2), offsets);
		Assert.Empty(_logs.Warnings);
	}

	[Fact]
	public void T4_boundary_just_under_idle_eviction_is_kept_and_exactly_idle_eviction_is_evicted()
	{
		var cache = NewEvictingCache(100);
		var a = new StreamId(Guid.NewGuid(), "Ns");
		cache.AddToCache(Enumerable.Range(0, 30).Select(i => (IBatchContainer)new TestBatch(a.Guid, "Ns", i)).ToList());
		var cursor = cache.GetCacheCursor(a, new EventSequenceToken(0));
		Assert.True(cursor.MoveNext());

		_clock.Advance(IdleEviction - TimeSpan.FromMilliseconds(1));
		Assert.False(cache.TryPurgeFromCache(out _));
		Assert.Equal(0, cache.EvictedCursorCount);

		_clock.Advance(TimeSpan.FromMilliseconds(1));
		Assert.True(cache.TryPurgeFromCache(out _));
		Assert.Equal(1, cache.EvictedCursorCount);
	}

	[Fact]
	public void Disposed_cursors_are_no_longer_tracked()
	{
		var cache = NewEvictingCache(100);
		var a = new StreamId(Guid.NewGuid(), "Ns");
		cache.AddToCache(Enumerable.Range(0, 30).Select(i => (IBatchContainer)new TestBatch(a.Guid, "Ns", i)).ToList());
		var cursor = cache.GetCacheCursor(a, new EventSequenceToken(0));
		Assert.Equal(1, cache.TrackedCursorCount);

		cursor.Dispose();
		cursor.Dispose();
		Assert.Equal(0, cache.TrackedCursorCount);

		_clock.Advance(TimeSpan.FromHours(1));
		Assert.True(cache.TryPurgeFromCache(out _));
		Assert.Equal(0, cache.EvictedCursorCount);
		Assert.Empty(_logs.Warnings);
	}
}
