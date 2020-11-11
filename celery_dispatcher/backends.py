from redis import StrictRedis


class RedisBackend:
    _conn = None

    def __init__(self, url=None):
        self._url = url

    @property
    def conn(self):
        if self._conn is None:
            self._conn = StrictRedis.from_url(self._url)
        return self._conn

    def bulk_push(self, key, values, expire=None):
        with self.conn.pipeline(transaction=False) as p:
            for value in values:
                self.push(key, value)

            if expire:
                p.expire(key, int(expire))

            p.execute()

    def push(self, key, value):
        return self.conn.lpush(key, value)

    def pop(self, key):
        return self.conn.rpop(key)


DEFAULT_BACKEND = RedisBackend
