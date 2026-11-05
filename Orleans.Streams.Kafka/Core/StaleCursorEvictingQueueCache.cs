using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using System;
using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Core
{
	/// <summary>
	/// Decorates a <see cref="SimpleQueueCache"/> so that a cursor which pins the oldest cache bucket
	/// without making progress cannot block purge (and so the partition commit) forever.
	/// </summary>
	/// <remarks>
	/// <para>
	/// <see cref="SimpleQueueCache"/> only purges a bucket once no cursor points into it. A cursor left behind by a consumer that
	/// never calls <c>MoveNext</c> again (a delivery that never completes, or a consumer left Active after a
	/// <c>ClientNotAvailableException</c>) pins its bucket for good, the cache reaches 10 buckets and the agent stops reading the queue.
	/// </para>
	/// <para>
	/// The members that say whether a cursor is set (<c>IsSet</c>, <c>SequenceToken</c>) are internal to Orleans, so the decorator
	/// works from the calls the pulling agent makes on each cursor. A cursor is <i>armed</i> after it was created, after a
	/// <c>Refresh</c> that found it idle, and after a <c>MoveNext</c> that returned true; it is <i>idle</i> after a <c>MoveNext</c>
	/// that returned false (the cursor is then unset at the head of the cache) and after an eviction. An armed cursor whose last
	/// arming is at least <c>idleEviction</c> old gets its inner cursor disposed when the cache is purged. Idle cursors are never evicted.
	/// </para>
	/// <para>
	/// After an eviction the cursor still behaves like a disposed <see cref="SimpleQueueCacheCursor"/>: <c>MoveNext</c> returns false and a
	/// later <c>Refresh</c> sets it again, which arms it again, so a leaked consumer is evicted again if it still makes no progress.
	/// Like <see cref="SimpleQueueCache"/> this type is not thread safe; the pulling agent calls it from one activation.
	/// </para>
	/// </remarks>
	public sealed class StaleCursorEvictingQueueCache : IQueueCache
	{
		private readonly SimpleQueueCache _inner;
		private readonly ILogger _logger;
		private readonly TimeSpan _idleEviction;
		private readonly TimeProvider _timeProvider;
		private readonly HashSet<EvictableCursor> _cursors = new HashSet<EvictableCursor>();

		public StaleCursorEvictingQueueCache(
			SimpleQueueCache inner,
			ILogger logger,
			TimeSpan idleEviction,
			TimeProvider timeProvider = null
		)
		{
			if (idleEviction <= TimeSpan.Zero)
				throw new ArgumentOutOfRangeException(nameof(idleEviction), idleEviction, "Must be greater than zero.");

			_inner = inner ?? throw new ArgumentNullException(nameof(inner));
			_logger = logger ?? throw new ArgumentNullException(nameof(logger));
			_idleEviction = idleEviction;
			_timeProvider = timeProvider ?? TimeProvider.System;
		}

		/// <summary>
		/// Number of cursors evicted by this cache so far.
		/// </summary>
		public long EvictedCursorCount { get; private set; }

		/// <summary>
		/// Number of cursors handed out and not yet disposed by the caller.
		/// </summary>
		public int TrackedCursorCount => _cursors.Count;

		public int GetMaxAddCount() => _inner.GetMaxAddCount();

		public void AddToCache(IList<IBatchContainer> messages) => _inner.AddToCache(messages);

		public bool IsUnderPressure() => _inner.IsUnderPressure();

		public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
		{
			EvictStaleCursors();
			return _inner.TryPurgeFromCache(out purgedItems);
		}

		public IQueueCacheCursor GetCacheCursor(IStreamIdentity streamIdentity, StreamSequenceToken token)
		{
			var inner = _inner.GetCacheCursor(streamIdentity, token);
			var cursor = new EvictableCursor(this, streamIdentity, inner, token, _timeProvider.GetUtcNow());
			_cursors.Add(cursor);
			return cursor;
		}

		private void EvictStaleCursors()
		{
			if (_cursors.Count == 0)
				return;

			var now = _timeProvider.GetUtcNow();
			foreach (var cursor in _cursors)
			{
				if (!cursor.TryEvict(now, _idleEviction, out var idleFor))
					continue;

				EvictedCursorCount++;
				_logger.LogWarning(
					"Evicted stale cache cursor for stream {StreamGuid}/{StreamNamespace}: last known sequence token {SequenceToken}, no progress for {IdleFor}",
					cursor.StreamIdentity.Guid,
					cursor.StreamIdentity.Namespace,
					cursor.LastKnownToken?.ToString() ?? "null",
					idleFor
				);
			}
		}

		private void Untrack(EvictableCursor cursor) => _cursors.Remove(cursor);

		private sealed class EvictableCursor : IQueueCacheCursor
		{
			private readonly StaleCursorEvictingQueueCache _owner;
			private readonly IQueueCacheCursor _inner;
			private readonly TimeProvider _timeProvider;
			private bool _armed;
			private DateTimeOffset _armedAt;

			public EvictableCursor(
				StaleCursorEvictingQueueCache owner,
				IStreamIdentity streamIdentity,
				IQueueCacheCursor inner,
				StreamSequenceToken token,
				DateTimeOffset now
			)
			{
				_owner = owner;
				_timeProvider = owner._timeProvider;
				StreamIdentity = streamIdentity;
				_inner = inner;
				LastKnownToken = token;
				_armed = true;
				_armedAt = now;
			}

			public IStreamIdentity StreamIdentity { get; }

			public StreamSequenceToken LastKnownToken { get; private set; }

			public IBatchContainer GetCurrent(out Exception exception) => _inner.GetCurrent(out exception);

			public bool MoveNext()
			{
				var moved = _inner.MoveNext();
				if (moved)
				{
					_armed = true;
					_armedAt = _timeProvider.GetUtcNow();
					LastKnownToken = _inner.GetCurrent(out _)?.SequenceToken ?? LastKnownToken;
				}
				else
				{
					// A false MoveNext leaves the inner cursor unset at the head of the cache (SimpleQueueCacheCursor.MoveNext).
					_armed = false;
				}

				return moved;
			}

			public void Refresh(StreamSequenceToken token)
			{
				_inner.Refresh(token);

				// The pulling agent refreshes every consumer on every batch, including one that is stuck. Only a refresh
				// of an idle cursor starts a clock; a refresh of an armed cursor must not restart it.
				if (_armed)
					return;

				_armed = true;
				_armedAt = _timeProvider.GetUtcNow();
				LastKnownToken = token ?? LastKnownToken;
			}

			public void RecordDeliveryFailure() => _inner.RecordDeliveryFailure();

			public void Dispose()
			{
				_owner.Untrack(this);
				_inner.Dispose();
			}

			public bool TryEvict(DateTimeOffset now, TimeSpan idleEviction, out TimeSpan idleFor)
			{
				idleFor = now - _armedAt;
				if (!_armed || idleFor < idleEviction)
					return false;

				_inner.Dispose();
				_armed = false;
				return true;
			}
		}
	}
}
