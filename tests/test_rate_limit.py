from py_news.rate_limit import SharedRateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_rate_limiter_waits_to_enforce_rps():
    clock = FakeClock()
    limiter = SharedRateLimiter(
        max_requests_per_second=2.0,
        time_fn=clock.time,
        sleep_fn=clock.sleep,
    )

    limiter.wait_for_slot()
    limiter.wait_for_slot()

    assert len(clock.sleeps) == 1
    assert clock.sleeps[0] == 0.5
