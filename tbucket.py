import collections
import contextlib
import logging
import os
import random
import threading
import time

import apsw


def log():
    return logging.getLogger(__name__)


class TokenBucket(object):

    def __init__(self, path, key, rate, period):
        self.path = path
        self.key = key
        self.rate = float(rate)
        self.period = float(period)

        self._local = threading.local()

    @property
    def db(self):
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
        self.db.cursor().execute("begin immediate")
        try:
            yield
        except:
            self.db.cursor().execute("rollback")
            raise
        else:
            self.db.cursor().execute("commit")

    def _set(self, tokens, last=None):
        with self.db:
            if last is None:
                last = time.time()
            if tokens < 0:
                tokens = 0.0
            if tokens > self.rate:
                tokens = self.rate
            self.db.cursor().execute(
                "insert or replace into tbf (key, tokens, last) "
                "values (?, ?, ?)",
                (self.key, tokens, last))
            return (tokens, last)

    def update(self, tokens, last, as_of):
        tdelta = as_of - last
        tokens += tdelta * self.rate / self.period
        return tokens, last

    def _peek(self):
        with self.db:
            row = self.db.cursor().execute(
                "select tokens, last from tbf where key = ?",
                (self.key,)).fetchone()
            now = time.time()
            if not row:
                tokens, last = self.rate, now
            else:
                tokens, last = row
            tokens, last = self.update(tokens, last, now)
            tokens, last = self._set(tokens, now)
            return (tokens, last)

    def try_consume(self, n, leave=None):
        if leave is None:
            leave = 0
        with self._begin():
            tokens, last = self._peek()
            if tokens >= n and tokens > leave:
                tokens, last = self._set(tokens - n, last=last)
                log().debug(
                    "%s: Gave %s token(s). %s remaining.",
                    self.key, n, tokens)
                return (True, tokens, last)
            return (False, tokens, last)

    def estimate(self, tokens, last, n, as_of):
        return last + (n - tokens) * self.period / self.rate

    def consume(self, n, leave=None):
        assert n > 0
        while True:
            success, tokens, last = self.try_consume(n, leave=leave)
            if success:
                return (tokens, last)
            now = time.time()
            target = self.estimate(tokens, last, n, now)
            if target > now:
                wait = target - now
                log().debug("%s: Waiting %ss for tokens", self.key, wait)
                time.sleep(wait)

    def peek(self):
        with self._begin():
            return self._peek()

    def set(self, tokens, last=None):
        with self._begin():
            return self._set(tokens, last=last)


class ScheduledTokenBucket(TokenBucket):

    def __init__(self, path, key, rate, period):
        super(ScheduledTokenBucket, self).__init__(
            path, key, rate, period)

    def get_last_refill(self, when):
        return when - (when % self.period)

    def get_next_refill(self, when):
        return self.get_last_refill(when) + self.period

    def update(self, tokens, last, as_of):
        last_refill = self.get_last_refill(as_of)
        if last_refill > last:
            return (self.rate, last_refill)
        return (tokens, as_of)

    def estimate(self, tokens, last, n, as_of):
        if tokens >= n:
            return as_of
        return self.get_next_refill(as_of)


class TimeSeriesTokenBucket(TokenBucket):

    def __init__(self, path, key, rate, period, trim_func=None):
        super(TimeSeriesTokenBucket, self).__init__(path, key, rate, period)
        self.rate = int(self.rate)
        if trim_func is None:
            trim_func = self._trim_default
        self.trim = trim_func

    @property
    def db(self):
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
        if not times:
            return
        with self.db:
            self.db.cursor().executemany(
                "insert into ts_token_bucket (key, time) values (?, ?)",
                [(self.key, t) for t in times])
            self.trim()

    def record(self, *times):
        with self._begin():
            return self._record(*times)

    def _mutate(self, mutator, as_of=None):
        if as_of is None:
            as_of = time.time()
        with self.db:
            _, old_times, as_of = self.peek(as_of=as_of)
            new_times = mutator(old_times, as_of)
            assert all(
                t <= as_of and t >= as_of - self.period
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
            return (self.rate - len(new_times), new_times, as_of)

    def mutate(self, mutator, as_of=None):
        with self._begin():
            return self._mutate(mutator, as_of=as_of)

    def set(self, n, as_of=None, fill=None, prune=None):
        assert n >= 0, n
        assert n <= self.rate, n

        if fill is None:
            def fill(times, as_of, n):
                return [as_of] * n

        if prune is None:
            def prune(times, as_of, n):
                return random.sample(times, n)

        def mutator(times, as_of):
            tokens = self.rate - len(times)
            if tokens > n:
                num_to_add = tokens - n
                new = list(fill(times, as_of, num_to_add))
                assert len(new) == num_to_add, new
                assert all(
                    t >= as_of - self.period and t <= as_of for t in new), new
                return list(times) + new
            elif tokens < n:
                num_to_prune = n - tokens
                to_prune = list(prune(times, as_of, num_to_prune))
                assert len(to_prune) == num_to_prune, to_prune
                times_counter = collections.Counter(times)
                times_counter.subtract(collections.Counter(to_prune))
                assert not any(v < 0 for v in times_counter.values()), (
                    to_prune, times)
                return list(times_counter.elements())
            return times

        return self.mutate(mutator, as_of=as_of)

    def peek(self, as_of=None):
        if as_of is None:
            as_of = time.time()
        with self.db:
            c = self.db.cursor()
            c.execute(
                "select time from ts_token_bucket "
                "where key = ? and time >= ? and time <= ?",
                (self.key, as_of - self.period, as_of))
            times = [r[0] for r in c.fetchall()]
            return (self.rate - len(times), times, as_of)

    def try_consume(self, n, leave=None):
        assert n > 0, n
        assert n <= self.rate, n
        if leave is None:
            leave = 0
        success = False
        with self._begin():
            _, times, as_of = self.peek()
            tokens = self.rate - len(times)
            if tokens >= n and tokens > leave:
                new_times = [as_of] * n
                self._record(*new_times)
                times += new_times
                tokens -= n
                log().debug(
                    "%s: Gave %s token(s). %s remaining.", self.key, n, tokens)
                success = True
            return (success, self.rate - len(times), times, as_of)

    def _estimate(self, times, as_of, n):
        assert n > 0, n
        assert n <= self.rate, n
        offset = self.rate - n
        if offset >= len(times):
            return as_of
        times = sorted(times, key=lambda t: -t)
        return times[offset] + self.period

    def estimate(self, n, as_of=None):
        _, times, as_of = self.peek(as_of=as_of)
        return self._estimate(times, as_of, n)

    def consume(self, n, leave=None):
        while True:
            success, _, times, as_of = self.try_consume(n, leave=leave)
            if success:
                return (self.rate - len(times), times, as_of)
            target = self._estimate(times, as_of, n)
            now = time.time()
            if target > now:
                wait = target - now
                log().debug("%s: Waiting %ss for tokens", self.key, wait)
                time.sleep(wait)
