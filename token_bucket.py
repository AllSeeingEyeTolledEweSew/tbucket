import contextlib
import logging
import os
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
