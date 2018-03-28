import six
import mock
import pytest
from pynamodb.pagination import RateLimiter
import time


PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'


class MockTime():
    def __init__(self):
        self.current_time = 0.0

    def sleep(self, amount):
        self.current_time += amount

    def time(self):
        return self.current_time

    def increment_time(self, amount):
        self.current_time += amount

def test_rate_limiter_exceptions():
    with pytest.raises(ValueError):
        r = RateLimiter(0)

    with pytest.raises(ValueError):
        r = RateLimiter(-1)

    with pytest.raises(ValueError):
        r = RateLimiter(10)
        r.rate_limit = 0

    with pytest.raises(ValueError):
        r = RateLimiter(10)
        r.rate_limit = -1


def test_basic_rate_limiting():
    mock_time = MockTime()
    r = RateLimiter(0.1, mock_time)

    # 100 operations
    for i in range(0, 100):
        r.acquire()
        # Simulates an operation that takes 1 second
        mock_time.increment_time(1)
        r.consume(1)

    # Since at the first acquire doesn't take time, thus we should be expecting (100-1) * 10 seconds = 990 delay
    # plus 1 for the last operation
    assert mock_time.time() == 991.0
