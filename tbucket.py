# The author disclaims copyright to this source code. Please see the
# accompanying UNLICENSE file.

"""
tbucket: some stateful classes for "token bucket" style rate limiting.

In a token bucket setup, each token represents some costly action, such as
making one API call or sending one byte of data. We also have a "bucket" of
tokens, which represents how many actions we're allowed to take. The bucket
fills up with tokens according to various rate-limiting semantics. When we're
about to take an action, we remove a token from the bucket. If the bucket is
empty, we must wait for it to refill.

These classes all use a SQLite database to store state. They take a "key"
parameter which identifies the bucket within the database. This way, token
bucket state may be shared between many different processes on a single
machine.

This library is tailored for the case of calling various APIs found in the wild
which have low rate limits. Our goal is to closely model the algorithm behind
an API's rate limiter, so we can make as many calls as possible as early as
possible, without going over the limit.

Note:
    This library treats SQLite's transaction semantics as a white box. In
    general, user-visible functions will initiate a BEGIN IMMEDIATE
    transaction.
"""

import collections
import contextlib
import logging
import os
import random
import threading
import time

import apsw


__all__ = [
    "TokenBucket",
    "ScheduledTokenBucket",
    "TimeSeriesTokenBucket",
]


def log():
    """Gets a module-level logger"""
    return logging.getLogger(__name__)


class TokenBucket(object):
    """
    A "classic" token bucket rate limiter.

    In the classic implementation, the bucket fills at a constant and
    continuous rate, up to a maximum.

    The configuration parameters are `rate` and `period`. The bucket fills at
    `rate / period`, with `rate` being the maximum value.

    The state of the bucket is represented as a (tokens, timestamp) tuple. This
    state gets returned from most functions.

    This class will create a table called "tbf" in the database to store state.
    The "key" attribute gets used as the primary key within the table. It only
    stores one row per bucket, consisting of the most recent (tokens,
    timestamp) state tuple.

    Attributes:
        path: The path to the sqlite database.
        key: A unique key for this bucket within the database.
        rate: The maximum number of tokens.
        period: The time for the bucket to reach the maximum number of tokens.
            The bucket refills at a rate of `rate / period`.
    """

    def __init__(self, path, key, rate, period):
        self.path = path
        self.key = key
        self.rate = float(rate)
        self.period = float(period)

        self._local = threading.local()

    @property
    def db(self):
        """A thread-local apsw.Connection."""
        db = getattr(self._local, "db", None)
        if db is not None:
            return db
        db = apsw.Connection(self.path)
        db.setbusytimeout(5000)
        with db:
            db.cursor().execute(
                "create table if not exists tbf ("
                "  key text primary key,"
                "  tokens float not null,"
                "  last float not null)")
        self._local.db = db
        return db

    @contextlib.contextmanager
    def _begin(self):
        """Returns a context manager for a BEGIN IMMEDIATE transaction."""
        self.db.cursor().execute("begin immediate")
        try:
            yield
        except:
            self.db.cursor().execute("rollback")
            raise
        else:
            self.db.cursor().execute("commit")

    def _set(self, tokens, timestamp=None):
        """Sets the state of the bucket.

        The state will be clamped to valid values before being set.

        Will perform a SAVEPOINT/RELEASE on the database.

        Args:
            tokens: The number of tokens we have/had at the given time.
            timestamp: The time at which we had this number of tokens. If None,
                the current time will be used.

        Returns:
            A (tokens, timestamp) tuple, clamped to valid values.
        """
        with self.db:
            if timestamp is None:
                timestamp = time.time()
            if tokens < 0:
                tokens = 0.0
            if tokens > self.rate:
                tokens = self.rate
            self.db.cursor().execute(
                "insert or replace into tbf (key, tokens, last) "
                "values (?, ?, ?)",
                (self.key, tokens, timestamp))
            return (tokens, timestamp)

    def _update(self, tokens, timestamp, query_time):
        """Update the bucket state for a new time, given a last known state.

        This function doesn't touch the database, and has no side effects.

        Args:
            tokens: The last known number of tokens.
            timestamp: The timestamp of the last known number of tokens.
            query_time: The query time.

        Returns:
            A (tokens, timestamp) state tuple.
        """
        tdelta = query_time - timestamp
        tokens += tdelta * self.rate / self.period
        return tokens, timestamp

    def _peek(self):
        """Get the current bucket state, and update the database.

        Will perform a SAVEPOINT/RELEASE on the database.

        Returns:
            A (tokens, timestamp) tuple, clamped to valid values.
        """
        with self.db:
            row = self.db.cursor().execute(
                "select tokens, last from tbf where key = ?",
                (self.key,)).fetchone()
            now = time.time()
            if not row:
                tokens, timestamp = self.rate, now
            else:
                tokens, timestamp = row
            tokens, timestamp = self._update(tokens, timestamp, now)
            tokens, timestamp = self._set(tokens, now)
            return (tokens, timestamp)

    def try_consume(self, n, leave=None):
        """Try to consume some tokens.

        If there are fewer tokens available than the number requested, this
        function will just signal failure, without waiting for a token.

        This will perform a BEGIN IMMEDIATE transaction on the database while
        querying and updating state.

        Args:
            n: The number of tokens to try to consume.
            leave: A number of tokens. Only succeed if we would have this many
                tokens left over, after n are consumed.

        Returns:
            A (success, tokens, timestamp) tuple.
        """
        if leave is None:
            leave = 0
        with self._begin():
            tokens, timestamp = self._peek()
            if tokens >= n and tokens > leave:
                tokens, timestamp = self._set(tokens - n, timestamp=timestamp)
                log().debug(
                    "%s: Gave %s token(s). %s remaining.",
                    self.key, n, tokens)
                return (True, tokens, timestamp)
            return (False, tokens, timestamp)

    def _estimate(self, tokens, timestamp, n, query_time):
        """Estimate the timestamp at which we would have a number of tokens.

        This function doesn't touch the database, and has no side effects.

        Args:
            tokens: The last known number of tokens.
            timestamp: The timestamp of the last known number of tokens.
            n: The number of tokens we need.
            query_time: The time as of which the query is made.

        Returns:
            The timestamp at which we would have n tokens available.
        """
        return timestamp + (n - tokens) * self.period / self.rate

    def consume(self, n, leave=None):
        """Consume tokens, waiting for them if necessary.

        This will perform a BEGIN IMMEDIATE transaction on the database while
        querying and updating state. The transaction is only used for updating
        state and won't be held while waiting for tokens.

        Args:
            n: The number of tokens to consume.
            leave: A number of tokens. Only successfully consume tokens once we
                would be able to leave this many behind.

        Returns:
            A (tokens, timestamp) tuple.
        """
        assert n > 0
        while True:
            success, tokens, timestamp = self.try_consume(n, leave=leave)
            if success:
                return (tokens, timestamp)
            now = time.time()
            target = self._estimate(tokens, timestamp, n, now)
            if target > now:
                wait = target - now
                log().debug("%s: Waiting %ss for tokens", self.key, wait)
                time.sleep(wait)

    def peek(self):
        """Peek at the current number of tokens, and update the state.

        This will perform a BEGIN IMMEDIATE transaction on the database.

        Returns:
            A (tokens, timestamp) tuple.
        """
        with self._begin():
            return self._peek()

    def set(self, tokens, timestamp=None):
        """Explicitly set the number of tokens.

        This will perform a BEGIN IMMEDIATE transaction on the database.

        Args:
            tokens: The number of tokens.
            timestamp: The time at which the tokens are measured. If None,
                defaults to now.

        Returns:
            A (tokens, timestamp) tuple.
        """
        with self._begin():
            return self._set(tokens, timestamp=timestamp)


class ScheduledTokenBucket(TokenBucket):
    """
    A token bucket which resets to a fixed number of tokens at regular
    intervals.

    The bucket will be filled  whenever `now % period == 0`. There is currently
    no support for any offsets to this schedule.

    When filled, the bucket will be reset to have `rate` tokens.

    The state of the bucket is represented as a (tokens, timestamp) tuple. This
    state gets returned from most functions.

    This class will create a table called "tbf" in the database to store state.
    The "key" attribute gets used as the primary key within the table. It only
    stores one row per bucket, consisting of the most recent (tokens,
    timestamp) state tuple.

    Attributes:
        path: The path to the sqlite database.
        key: A unique key for this bucket within the database.
        rate: The number of tokens the bucket will be reset to.
        period: How often the bucket is reset.
    """

    def __init__(self, path, key, rate, period):
        super(ScheduledTokenBucket, self).__init__(
            path, key, rate, period)

    def _get_last_refill(self, when):
        """Get the last time the bucket refilled, as of a query time.

        Args:
            when: The query time.

        Returns:
            A timestamp representing the last time the bucket was refilled.
        """
        return when - (when % self.period)

    def _get_next_refill(self, when):
        """Get the next time the bucket will refill, as of a query time.

        Args:
            when: The query time.

        Returns:
            A timestamp representing the next time the bucket will refill.
        """
        return self._get_last_refill(when) + self.period

    def _update(self, tokens, timestamp, query_time):
        last_refill = self._get_last_refill(query_time)
        if last_refill > timestamp:
            return (self.rate, last_refill)
        return (tokens, query_time)

    def _estimate(self, tokens, timestamp, n, query_time):
        if tokens >= n:
            return query_time
        return self._get_next_refill(query_time)


class TimeSeriesTokenBucket(TokenBucket):
    """
    A token bucket which tracks the exact timestamps of tokens withdrawn.

    This token bucket implementation enforces that exactly N tokens may be
    consumed in any window of the target size.

    However, this implementation requires tracking more state. The classic
    implementation keeps a single tuple for state, but this implementation must
    track the timestamps of at least the last N tokens.

    The bucket has tokens available whenever fewer than `rate` tokens have been
    consumed in the last `period`.

    This class will create a table called "ts_token_bucket" in the database to
    store state. The "key" attribute gets used as a key in this table. It will
    store one row per token consumed, but by default will only store `rate`
    tokens (see `trim()`).

    Attributes:
        path: The path to the sqlite database.
        key: A unique key for this bucket within the database.
        rate: The number of tokens the bucket will be reset to.
        period: How often the bucket is reset.
    """

    def __init__(self, path, key, rate, period, trim_func=None):
        super(TimeSeriesTokenBucket, self).__init__(path, key, rate, period)
        self.rate = int(self.rate)
        if trim_func is None:
            trim_func = self._trim_default
        self.trim = trim_func

    @property
    def db(self):
        """A thread-local apsw.Connection."""
        db = getattr(self._local, "db", None)
        if db is not None:
            return db
        db = apsw.Connection(self.path)
        db.setbusytimeout(5000)
        with db:
            db.cursor().execute(
                "create table if not exists ts_token_bucket ("
                "  key text not null,"
                "  time float not null)")
            db.cursor().execute(
                "create index if not exists ts_token_bucket_key_time "
                "on ts_token_bucket (key, time)")
        self._local.db = db
        return db

    def _trim_default(self):
        r = self.db.cursor().execute(
            "select max(time) from ts_token_bucket where key = ?",
            (self.key,)).fetchone()
        if r is None or r[0] is None:
            return
        latest = r[0]
        self.db.cursor().execute(
            "delete from ts_token_bucket where key = ? and time < ?",
            (self.key, latest - self.period))

    def _record(self, *times):
        """Record new token timestamps.

        Will perform a SAVEPOINT with immediate INSERT on the database.

        Args:
            *times: A list of timestamps when some tokens were given out.
        """
        if not times:
            return
        with self.db:
            self.db.cursor().executemany(
                "insert into ts_token_bucket (key, time) values (?, ?)",
                [(self.key, t) for t in times])
            self.trim()

    def record(self, *times):
        """Record new token timestamps.

        Will perform a BEGIN IMMEDIATE transaction on the database.

        Args:
            *times: A list of timestamps when some tokens were given out.
        """
        with self._begin():
            return self._record(*times)

    def _mutate(self, mutator, query_time=None):
        """Mutate the set of token timestamps within the most recent period.

        This will get the list of token timestamps that occurred from
        `query_time - period` and `query_time`, pass them to a `mutator`
        function. The `mutator` should return a new set of timestamps, which
        must all be within the same window. This function will then update the
        database so that the returned timestamps will be the only timestamps
        that exist within the window.

        Will perform a SAVEPOINT/RELEASE on the database.

        Args:
            mutator: A function which takes (list_of_timestamps, query_time)
                and returns a new list of timestamps.
            query_time: A target query time. The target window will be from
                `query_time - period` to `query_time`. If None, defaults to
                now. This value will be passed to the mutator function.
        """
        if query_time is None:
            query_time = time.time()
        with self.db:
            _, old_times, query_time = self.peek(query_time=query_time)
            new_times = mutator(old_times, query_time)
            assert all(
                t <= query_time and t >= query_time - self.period
                for t in new_times), new_times
            old_counter = collections.Counter(old_times)
            new_counter = collections.Counter(new_times)
            times_to_add = list((new_counter - old_counter).elements())
            times_to_delete = list((old_counter - new_counter).elements())
            if times_to_add :
                self._record(*times_to_add)
            if times_to_delete:
                self.db.cursor().executemany(
                    "delete from ts_token_bucket where rowid = "
                    "(select rowid from ts_token_bucket "
                    "where key = ? and time = ? limit 1)",
                    [(self.key, t) for t in times_to_delete])
            return (self.rate - len(new_times), new_times, query_time)

    def mutate(self, mutator, query_time=None):
        """Mutate the set of token timestamps recorded in a recent period.

        This will get the list of token timestamps that occurred from
        `query_time - period` and `query_time`, pass them to a `mutator`
        function. The `mutator` should return a new set of timestamps, which
        must all be within the same window. This function will then update the
        database so that the returned timestamps will be the only timestamps
        that exist within the window.

        Will perform a BEGIN IMMEDIATE transaction on the database. The
        transaction will be held while `mutator` is called.

        Args:
            mutator: A function which takes (list_of_timestamps, query_time),
                and returns a new list of timestamps.
            query_time: A target query time. The target window will be from
                `query_time - period` to `query_time`. If None, defaults to
                now. This value will be passed to the mutator function.
        """
        with self._begin():
            return self._mutate(mutator, query_time=query_time)

    def set(self, n, query_time=None, fill=None, prune=None):
        """Updates the set of token timestamps recorded in a recent window such
        that there are exactly n.

        This is useful for the case when you know n tokens are available, but
        don't have an exact history of token timestamps. For example, if you
        get a "call limit exceeded" error from an API which uses this style of
        call tracking, you know that there must be `rate` token timestamps
        should be recorded in the last `period`, leaving 0 available now.

        This function helps make guesses when mutating a recent window in this
        case.

        If it needs to record new token timestamps, by default, it will record
        them all at `query_time`. In other words, the default is to guess that
        some tokens were consumed just now. This is the most conservative
        guess, as it means the consumer will need to wait the longest possible
        time before consuming more.

        If it needs to remove some token timestamps, by default it will remove
        them at random.

        Will perform a BEGIN IMMEDIATE transaction on the database. The
        transaction will be held while `fill` or `prune` are called.

        Args:
            n: The number of tokens that should exist in the recent window of
                `query_time - period` to `query_time`.
            query_time: The end of the window. If None, defaults to now.
            fill: A function to guess when some token timestamps might have
                occurred, if new ones must be recorded. Takes
                (list_of_timestamps, query_time, n) as arguments, and must
                return a new list of timestamps of length n. The new timestamps
                should all be within `query_time - preiod` and `query_time`.
                The default returns `[query_time] * n`.
            prune: A function to guess which timestamps should be deleted, if
                some must be removed. Takes (list_of_timestamps, query_time, n)
                as arguments, and must return a new list of timestamps of
                length n, where each timestamp occurs in the old list at least
                as many times as in the new one. The default returns
                `random.sample(list_of_timestamps, n)`.

        Returns:
            The new list of timestamps in the window.
        """
        assert n >= 0, n
        assert n <= self.rate, n

        if fill is None:
            def fill(times, query_time, n):
                return [query_time] * n

        if prune is None:
            def prune(times, query_time, n):
                return random.sample(times, n)

        def mutator(times, query_time):
            tokens = self.rate - len(times)
            if tokens > n:
                num_to_add = tokens - n
                new = list(fill(times, query_time, num_to_add))
                assert len(new) == num_to_add, new
                assert all(
                    t >= query_time - self.period and t <= query_time
                    for t in new), new
                return list(times) + new
            elif tokens < n:
                num_to_prune = n - tokens
                to_prune = list(prune(times, query_time, num_to_prune))
                assert len(to_prune) == num_to_prune, to_prune
                times_counter = collections.Counter(times)
                times_counter.subtract(collections.Counter(to_prune))
                assert not any(v < 0 for v in times_counter.values()), (
                    to_prune, times)
                return list(times_counter.elements())
            return times

        return self.mutate(mutator, query_time=query_time)

    def peek(self, query_time=None):
        """Peek at the recorded token timestamps in a recent window.

        This will perform a SAVEPOINT/RELEASE on the database.

        Args:
            query_time: The target query time. If None, defaults to now.

        Returns:
            A tuple of (tokens, list_of_timestamps, query_time). The number of
                tokens will always be `rate - len(list_of_timestamps`).
        """
        if query_time is None:
            query_time = time.time()
        with self.db:
            c = self.db.cursor()
            c.execute(
                "select time from ts_token_bucket "
                "where key = ? and time >= ? and time <= ?",
                (self.key, query_time - self.period, query_time))
            times = [r[0] for r in c.fetchall()]
            return (self.rate - len(times), times, query_time)

    def try_consume(self, n, leave=None):
        """Try to consume some tokens.

        If there are fewer tokens available than the number requested, this
        function will just signal failure, without waiting for a token.

        This will perform a BEGIN IMMEDIATE transaction on the database while
        querying and updating state.

        Args:
            n: The number of tokens to try to consume.
            leave: A number of tokens. Only succeed if we would have this many
                tokens left over, after n are consumed.

        Returns:
            A (success, tokens, list_of_timestamps, query_time) tuple.
        """
        assert n > 0, n
        assert n <= self.rate, n
        if leave is None:
            leave = 0
        success = False
        with self._begin():
            _, times, query_time = self.peek()
            tokens = self.rate - len(times)
            if tokens >= n and tokens > leave:
                new_times = [query_time] * n
                self._record(*new_times)
                times += new_times
                tokens -= n
                log().debug(
                    "%s: Gave %s token(s). %s remaining.", self.key, n, tokens)
                success = True
            return (success, self.rate - len(times), times, query_time)

    def _estimate(self, times, query_time, n):
        """Estimate the timestamp at which we would have a number of tokens.

        This function doesn't touch the database, and has no side effects.

        Args:
            times: A list of token timestamps, between `query_time - period`
                and `query_time`.
            query_time: The time as of which the query is made.
            n: The number of tokens we need.

        Returns:
            The timestamp at which we would have n tokens available.
        """
        assert n > 0, n
        assert n <= self.rate, n
        offset = self.rate - n
        if offset >= len(times):
            return query_time
        times = sorted(times, key=lambda t: -t)
        return times[offset] + self.period

    def estimate(self, n, query_time=None):
        """Estimate the timestamp at which we would have a number of tokens.

        This will perform a SAVEPOINT/RELEASE on the database.

        Args:
            n: The number of tokens we need.
            query_time: The time as of which the query is made. If None,
                defaults to now.

        Returns:
            The timestamp at which we would have n tokens available.
        """
        _, times, query_time = self.peek(query_time=query_time)
        return self._estimate(times, query_time, n)

    def consume(self, n, leave=None):
        """Consume tokens, waiting for them if necessary.

        This will perform a BEGIN IMMEDIATE transaction on the database while
        querying and updating state. The transaction is only used for updating
        state and won't be held while waiting for tokens.

        Args:
            n: The number of tokens to consume.
            leave: A number of tokens. Only successfully consume tokens once we
                would be able to leave this many behind.

        Returns:
            A tuple of (tokens, list_of_timestamps, query_Time). The number of
                tokens will always be `rate - len(list_of_timestamps`).
        """
        while True:
            success, _, times, query_time = self.try_consume(n, leave=leave)
            if success:
                return (self.rate - len(times), times, query_time)
            target = self._estimate(times, query_time, n)
            now = time.time()
            if target > now:
                wait = target - now
                log().debug("%s: Waiting %ss for tokens", self.key, wait)
                time.sleep(wait)
