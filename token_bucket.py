import logging
import os
import sqlite3
import threading
import time


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
        db = sqlite3.connect(self.path)
        db.isolation_level = "IMMEDIATE"
        with db:
            db.execute(
                "create table if not exists tbf ("
                "  key text primary key,"
                "  tokens float not null,"
                "  last float not null)")
        self._local.db = db
        return db

    def _set_unlocked(self, tokens, last=None):
        if last is None:
            last = time.time()
        if tokens < 0:
            tokens = 0.0
        if tokens > self.rate:
            tokens = self.rate
        self.db.execute(
            "insert or replace into tbf (key, tokens, last) values (?, ?, ?)",
            (self.key, tokens, last))
        return (tokens, last)

    def update(self, tokens, last, as_of):
        tdelta = as_of - last
        tokens += tdelta * self.rate / self.period
        return tokens, last

    def _peek_unlocked(self):
        row = self.db.execute(
            "select tokens, last from tbf where key = ?",
            (self.key,)).fetchone()
        now = time.time()
        if not row:
            tokens, last = 0.0, now
        else:
            tokens, last = row
        tokens, last = self.update(tokens, last, now)
        tokens, last = self._set_unlocked(tokens, now)
        return (tokens, last)

    def try_consume(self, n):
        with self.db:
            tokens, last = self._peek_unlocked()
            if tokens >= n:
                tokens, last = self._set_unlocked(
                    tokens - n, last=last)
                log().debug(
                    "%s: Gave %s token(s). %s remaining.",
                    self.key, n, tokens)
                return (True, tokens, last)
            return (False, tokens, last)

    def estimate(self, tokens, last, n, as_of):
        return last + (n - tokens) * self.period / self.rate

    def consume(self, n):
        assert n > 0
        while True:
            success, tokens, last = self.try_consume(n)
            if success:
                return (tokens, last)
            now = time.time()
            target = self.estimate(tokens, last, n, now)
            if target > now:
                wait = target - now
                log().debug("%s: Waiting %ss for tokens", self.key, wait)
                time.sleep(wait)

    def peek(self):
        with self.db:
            return self._peek_unlocked()

    def set(self, tokens, last=None):
        with self.db:
            return self._set_unlocked(tokens, last=last)


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
