using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Orleans.Streams.Kafka.Tests;

/// <summary>
/// The per-tick loop of PersistentStreamPullingAgent (ReadFromQueue, StartInactiveCursors, RunConsumerCursor) against any IQueueCache:
/// purge, commit the highest purged offset, stop reading while the cache is under pressure, otherwise add at most GetMaxAddCount()
/// messages, Refresh every consumer's cursor with the first token of its stream in the batch, then let every consumer that is not
/// stuck drain with MoveNext. 50 streams, messages spread over them round robin, offset = sequence number.
/// Stream 0 is the stuck consumer: it takes one message and never calls MoveNext again (a delivery that never completes, or a
/// consumer left Active after ClientNotAvailableException).
/// </summary>
internal sealed class PullingAgentSimulation
{
	public const int Streams = 50;
	public const string Namespace = "Ns";

	private readonly IQueueCache _cache;
	private readonly Guid[] _guids = Enumerable.Range(0, Streams).Select(i => new Guid(i + 1, 0, 0, new byte[8])).ToArray();
	private readonly IQueueCacheCursor[] _cursors = new IQueueCacheCursor[Streams];
	private readonly HashSet<int> _stuck = new();
	private long _next;

	public PullingAgentSimulation(IQueueCache cache)
	{
		_cache = cache;
	}

	/// <summary>Runs at the start of every Tick(int), e.g. to let a test clock move between ticks.</summary>
	public Action BeforeEachTick { get; set; }

	public long CommittedOffset { get; private set; } = -1;
	public long NextOffset => _next;
	public IQueueCacheCursor StuckCursor => _cursors[0];
	public Dictionary<int, int> Delivered { get; } = new();

	public IStreamIdentity StreamId(int stream) => new StreamId(_guids[stream], Namespace);

	/// <summary>Creates every consumer's cursor; the stuck one takes its single message and stops.</summary>
	public void Start()
	{
		var first = NewBatch(1);
		_cache.AddToCache(first);
		_cursors[0] = _cache.GetCacheCursor(StreamId(0), first[0].SequenceToken);
		for (var i = 1; i < Streams; i++)
			_cursors[i] = _cache.GetCacheCursor(StreamId(i), null);

		if (!_cursors[0].MoveNext())
			throw new InvalidOperationException("The stuck consumer's first MoveNext must deliver.");
		_stuck.Add(0);
		DrainConsumers();
	}

	public record TickResult(int Purged, bool UnderPressure, int Added);

	public TickResult Tick()
	{
		var purgedCount = 0;
		if (_cache.TryPurgeFromCache(out var purged) && purged.Count > 0)
		{
			purgedCount = purged.Count;
			CommittedOffset = Math.Max(CommittedOffset, purged.Max(p => ((EventSequenceToken)p.SequenceToken).SequenceNumber));
		}

		var pressure = _cache.IsUnderPressure();
		var added = 0;
		if (!pressure)
		{
			var batch = NewBatch(Math.Min(409, _cache.GetMaxAddCount()));
			_cache.AddToCache(batch);
			added = batch.Count;
			foreach (var group in batch.GroupBy(b => Array.IndexOf(_guids, b.StreamGuid)))
				_cursors[group.Key].Refresh(group.First().SequenceToken);
		}

		DrainConsumers();
		return new TickResult(purgedCount, pressure, added);
	}

	public void Tick(int times)
	{
		for (var i = 0; i < times; i++)
		{
			BeforeEachTick?.Invoke();
			Tick();
		}
	}

	private List<IBatchContainer> NewBatch(int count)
	{
		var list = new List<IBatchContainer>(count);
		for (var i = 0; i < count; i++)
		{
			list.Add(new TestBatch(_guids[(int)(_next % Streams)], Namespace, _next));
			_next++;
		}

		return list;
	}

	private void DrainConsumers()
	{
		for (var i = 0; i < Streams; i++)
		{
			if (_stuck.Contains(i))
				continue;
			while (_cursors[i].MoveNext())
				Delivered[i] = Delivered.GetValueOrDefault(i) + 1;
		}
	}
}
