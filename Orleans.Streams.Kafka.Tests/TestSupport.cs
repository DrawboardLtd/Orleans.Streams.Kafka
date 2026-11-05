using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Orleans.Streams.Kafka.Tests;

internal sealed class StreamId : IStreamIdentity
{
	public StreamId(Guid guid, string ns) { Guid = guid; Namespace = ns; }
	public Guid Guid { get; }
	public string Namespace { get; }
}

internal sealed class TestBatch : IBatchContainer
{
	public TestBatch(Guid guid, string ns, long offset)
	{
		StreamGuid = guid;
		StreamNamespace = ns;
		SequenceToken = new EventSequenceToken(offset);
	}

	public Guid StreamGuid { get; }
	public string StreamNamespace { get; }
	public StreamSequenceToken SequenceToken { get; }
	public long Offset => ((EventSequenceToken)SequenceToken).SequenceNumber;

	public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>() => throw new NotImplementedException();
	public bool ImportRequestContext() => false;
	public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
		=> stream.Guid == StreamGuid && stream.Namespace == StreamNamespace;
}

internal sealed class ManualTimeProvider : TimeProvider
{
	private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
	public override DateTimeOffset GetUtcNow() => _now;
	public void Advance(TimeSpan by) => _now += by;
}

internal sealed record LogEntry(LogLevel Level, string Message);

internal sealed class CapturingLoggerProvider : ILoggerProvider
{
	private readonly List<LogEntry> _entries = new();

	public IReadOnlyList<LogEntry> Entries
	{
		get { lock (_entries) return _entries.ToArray(); }
	}

	public IReadOnlyList<LogEntry> Warnings => Entries.Where(e => e.Level >= LogLevel.Warning).ToArray();

	public ILogger CreateLogger(string categoryName) => new CapturingLogger(this);
	public void Dispose() { }

	private void Add(LogEntry entry)
	{
		lock (_entries) _entries.Add(entry);
	}

	private sealed class CapturingLogger : ILogger
	{
		private readonly CapturingLoggerProvider _owner;
		public CapturingLogger(CapturingLoggerProvider owner) => _owner = owner;
		public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;
		public bool IsEnabled(LogLevel logLevel) => true;
		public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
			=> _owner.Add(new LogEntry(logLevel, formatter(state, exception)));
	}

	private sealed class NullScope : IDisposable
	{
		public static readonly NullScope Instance = new();
		public void Dispose() { }
	}
}
