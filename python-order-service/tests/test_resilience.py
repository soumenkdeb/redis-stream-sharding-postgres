"""
Unit tests for resilience.py — CircuitBreaker and with_resilience().

Tests cover every state transition and failure scenario without sleeping:
  - asyncio.sleep is replaced with AsyncMock (instant)
  - time.monotonic is injected via _clock so recovery timeout is controllable

Reading order:
  1. TestCircuitBreakerStates   — state machine transitions
  2. TestWithResilienceRetry    — retry + backoff behaviour
  3. TestWithResilienceTimeout  — per-attempt timeout
  4. TestWithResilienceCircuit  — retry exhaustion → circuit failure counting
  5. TestHalfOpen               — recovery probe logic
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, call, patch
import pytest

from resilience import CircuitBreaker, CircuitBreakerOpenError, with_resilience


# ── helpers ──────────────────────────────────────────────────────────────────

def make_breaker(threshold=3, recovery=30, clock=None):
    """Build a CircuitBreaker with an injectable clock for time travel."""
    return CircuitBreaker(
        "test",
        failure_threshold=threshold,
        recovery_timeout=recovery,
        _clock=clock or (lambda: 0.0),
    )


async def _ok():
    return "ok"


async def _fail():
    raise RuntimeError("boom")


# ── CircuitBreaker state machine ─────────────────────────────────────────────

class TestCircuitBreakerStates:

    def test_initial_state_is_closed(self):
        cb = make_breaker()
        assert cb.state == "closed"
        assert cb.allow_request() is True

    def test_stays_closed_below_threshold(self):
        """Failures below threshold do not open the circuit."""
        cb = make_breaker(threshold=3)
        cb.on_failure()
        cb.on_failure()
        assert cb.state == "closed"
        assert cb.allow_request() is True

    def test_opens_at_threshold(self):
        """Exactly failure_threshold failures opens the circuit."""
        cb = make_breaker(threshold=3)
        cb.on_failure()
        cb.on_failure()
        cb.on_failure()
        assert cb.state == "open"
        assert cb.allow_request() is False

    def test_rejects_calls_when_open(self):
        cb = make_breaker(threshold=1)
        cb.on_failure()
        assert cb.allow_request() is False

    def test_success_resets_failure_count(self):
        """A success after two failures resets the counter — circuit stays closed."""
        cb = make_breaker(threshold=3)
        cb.on_failure()
        cb.on_failure()
        cb.on_success()
        cb.on_failure()  # count restarted — only 1 now
        assert cb.state == "closed"

    def test_reset_clears_state(self):
        cb = make_breaker(threshold=1)
        cb.on_failure()
        assert cb.state == "open"
        cb.reset()
        assert cb.state == "closed"
        assert cb._failure_count == 0


class TestCircuitBreakerRecovery:

    def test_transitions_to_half_open_after_timeout(self):
        """
        After recovery_timeout seconds the circuit moves from OPEN → HALF_OPEN
        and allows one probe request.
        """
        # Simulate: opened at t=0, now at t=31 (> recovery_timeout=30)
        tick = 0.0
        cb = CircuitBreaker("t", failure_threshold=1, recovery_timeout=30,
                            _clock=lambda: tick)
        cb.on_failure()
        assert cb.state == "open"

        tick = 31.0  # advance clock past recovery window
        assert cb.state == "half_open"
        assert cb.allow_request() is True

    def test_half_open_success_closes_circuit(self):
        tick = 0.0
        cb = CircuitBreaker("t", failure_threshold=1, recovery_timeout=30,
                            _clock=lambda: tick)
        cb.on_failure()
        tick = 31.0
        assert cb.state == "half_open"
        cb.on_success()
        assert cb.state == "closed"

    def test_half_open_failure_reopens_circuit(self):
        tick = 0.0
        cb = CircuitBreaker("t", failure_threshold=1, recovery_timeout=30,
                            _clock=lambda: tick)
        cb.on_failure()
        tick = 31.0
        assert cb.state == "half_open"
        cb.on_failure()
        assert cb.state == "open"

    def test_does_not_transition_before_timeout(self):
        tick = 0.0
        cb = CircuitBreaker("t", failure_threshold=1, recovery_timeout=30,
                            _clock=lambda: tick)
        cb.on_failure()
        tick = 15.0  # only halfway through recovery window
        assert cb.state == "open"


# ── with_resilience retry behaviour ──────────────────────────────────────────

class TestWithResilienceRetry:

    @pytest.mark.asyncio
    async def test_success_first_attempt_no_retry(self):
        """No retries needed when first call succeeds."""
        sleep = AsyncMock()
        cb = make_breaker()
        result = await with_resilience(lambda: _ok(), circuit_breaker=cb, _sleep=sleep)
        assert result == "ok"
        sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_retries_on_failure_succeeds_third_attempt(self):
        """
        Fails on attempts 1 and 2, succeeds on attempt 3.
        Circuit breaker must NOT count this as a failure (success ultimately).
        """
        sleep = AsyncMock()
        cb = make_breaker()
        calls = 0

        async def flaky():
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RuntimeError("transient")
            return "ok"

        result = await with_resilience(flaky, circuit_breaker=cb,
                                       max_retries=3, delay=1.0, backoff=2.0,
                                       _sleep=sleep)
        assert result == "ok"
        assert calls == 3
        assert cb.state == "closed"

    @pytest.mark.asyncio
    async def test_exponential_backoff_delays(self):
        """
        Verify sleep is called with delay=1s then delay*backoff=2s between attempts.
        """
        sleep = AsyncMock()
        cb = make_breaker(threshold=99)  # don't open the circuit

        async def always_fail():
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            await with_resilience(always_fail, circuit_breaker=cb,
                                  max_retries=3, delay=1.0, backoff=2.0,
                                  _sleep=sleep)

        # First gap: 1.0s, second gap: 2.0s, no sleep after last attempt
        assert sleep.call_count == 2
        assert sleep.call_args_list[0] == call(1.0)
        assert sleep.call_args_list[1] == call(2.0)

    @pytest.mark.asyncio
    async def test_all_retries_exhausted_raises_last_exception(self):
        """When all attempts fail the original exception propagates."""
        sleep = AsyncMock()
        cb = make_breaker(threshold=99)

        with pytest.raises(RuntimeError, match="original error"):
            await with_resilience(
                lambda: (_ for _ in ()).throw(RuntimeError("original error")),
                circuit_breaker=cb, max_retries=3, _sleep=sleep,
            )

    @pytest.mark.asyncio
    async def test_no_sleep_after_last_attempt(self):
        """Sleep must not be called after the final failed attempt."""
        sleep = AsyncMock()
        cb = make_breaker(threshold=99)
        calls = 0

        async def fail():
            nonlocal calls; calls += 1
            raise RuntimeError("x")

        with pytest.raises(RuntimeError):
            await with_resilience(fail, circuit_breaker=cb,
                                  max_retries=3, _sleep=sleep)

        assert calls == 3
        assert sleep.call_count == 2  # only between attempts, not after last


# ── with_resilience timeout ───────────────────────────────────────────────────

class TestWithResilienceTimeout:

    @pytest.mark.asyncio
    async def test_timeout_triggers_retry(self):
        """
        A hung call is cancelled after `timeout` seconds and the attempt is
        counted as a failure, triggering a retry.
        """
        sleep = AsyncMock()
        cb = make_breaker(threshold=99)
        attempts = 0

        async def hangs_then_ok():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                await asyncio.sleep(999)   # will be cancelled by wait_for
            return "ok"

        result = await with_resilience(
            hangs_then_ok,
            circuit_breaker=cb,
            max_retries=3,
            timeout=0.01,   # 10 ms — fires instantly in tests
            _sleep=sleep,
        )
        assert result == "ok"
        assert attempts == 2

    @pytest.mark.asyncio
    async def test_all_timeouts_exhaust_retries(self):
        """If every attempt times out, with_resilience raises TimeoutError."""
        sleep = AsyncMock()
        cb = make_breaker(threshold=99)

        async def always_hangs():
            await asyncio.sleep(999)

        with pytest.raises(asyncio.TimeoutError):
            await with_resilience(
                always_hangs,
                circuit_breaker=cb,
                max_retries=3,
                timeout=0.01,
                _sleep=sleep,
            )
        # Timeout exhaustion still counts as a CB failure
        assert cb._failure_count == 1


# ── with_resilience ↔ CircuitBreaker integration ──────────────────────────────

class TestWithResilienceCircuit:

    @pytest.mark.asyncio
    async def test_circuit_opens_after_threshold_total_retry_failures(self):
        """
        Each call to with_resilience that exhausts all retries = 1 CB failure.
        After failure_threshold such calls the circuit opens.
        """
        sleep = AsyncMock()
        cb = make_breaker(threshold=3)

        async def always_fail():
            raise RuntimeError("down")

        for _ in range(3):
            with pytest.raises(RuntimeError):
                await with_resilience(always_fail, circuit_breaker=cb,
                                      max_retries=3, _sleep=sleep)

        assert cb.state == "open"

    @pytest.mark.asyncio
    async def test_open_circuit_raises_immediately_without_calling_operation(self):
        """
        When the circuit is open, with_resilience raises CircuitBreakerOpenError
        without executing the coro_factory at all — fail fast.
        """
        sleep = AsyncMock()
        cb = make_breaker(threshold=1)
        cb.on_failure()  # force open
        assert cb.state == "open"

        called = False

        async def should_not_run():
            nonlocal called; called = True
            return "ok"

        with pytest.raises(CircuitBreakerOpenError):
            await with_resilience(should_not_run, circuit_breaker=cb, _sleep=sleep)

        assert called is False
        sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_circuit_closes_after_successful_probe(self):
        """
        HALF_OPEN: one successful probe closes the circuit.
        """
        tick = 0.0
        cb = CircuitBreaker("t", failure_threshold=1, recovery_timeout=30,
                            _clock=lambda: tick)
        cb.on_failure()

        tick = 31.0  # advance past recovery window → HALF_OPEN
        assert cb.state == "half_open"

        result = await with_resilience(lambda: _ok(), circuit_breaker=cb,
                                       _sleep=AsyncMock())
        assert result == "ok"
        assert cb.state == "closed"

    @pytest.mark.asyncio
    async def test_transient_errors_do_not_open_circuit(self):
        """
        Two failures followed by a success must NOT open the circuit
        (threshold=3, each with_resilience call retries internally).
        """
        sleep = AsyncMock()
        cb = make_breaker(threshold=3)
        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise RuntimeError("blip")
            return "recovered"

        result = await with_resilience(flaky, circuit_breaker=cb,
                                       max_retries=3, _sleep=sleep)
        assert result == "recovered"
        assert cb.state == "closed"
        assert cb._failure_count == 0


# ── producer-specific: circuit open → HTTP 503 ───────────────────────────────

class TestProducerCircuitBreakerIntegration:
    """
    End-to-end: open circuit on a shard → POST /orders returns 503.
    Uses FastAPI test client with mocked Redis.
    """

    @pytest.mark.asyncio
    async def test_open_circuit_returns_503(self, mock_redis, sample_order):
        import producer as prod_module
        from producer import app
        from httpx import AsyncClient, ASGITransport

        prod_module.clients = [mock_redis]
        prod_module.redis_breakers = [
            CircuitBreaker("redis-shard-0", failure_threshold=1, recovery_timeout=30)
        ]
        # Force the circuit open
        prod_module.redis_breakers[0].on_failure()

        async with AsyncClient(transport=ASGITransport(app=app),
                               base_url="http://test") as ac:
            response = await ac.post("/orders", json=sample_order)

        assert response.status_code == 503
        assert "circuit" in response.json()["detail"].lower()
        # Redis was never called — fail fast
        mock_redis.xadd.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_succeeds_returns_200(self, mock_redis, sample_order):
        """Redis fails once then succeeds — response is still 200."""
        import producer as prod_module
        from producer import app
        from httpx import AsyncClient, ASGITransport

        call_count = 0

        async def flaky_xadd(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("momentary failure")
            return "1714-0"

        mock_redis.xadd.side_effect = flaky_xadd
        prod_module.clients = [mock_redis]
        prod_module.redis_breakers = [
            CircuitBreaker("redis-shard-0", failure_threshold=3, recovery_timeout=30)
        ]

        with patch("producer.asyncio.sleep", new=AsyncMock()):
            async with AsyncClient(transport=ASGITransport(app=app),
                                   base_url="http://test") as ac:
                response = await ac.post("/orders", json=sample_order)

        assert response.status_code == 200
        assert call_count == 2


# ── consumer-specific: DB circuit open → XACK skipped ────────────────────────

class TestConsumerCircuitBreakerIntegration:

    @pytest.mark.asyncio
    async def test_db_circuit_open_skips_xack(
        self, mock_db_pool, mock_redis, sample_stream_fields
    ):
        """
        When the DB circuit is open, flush_batch must return early — no
        Postgres call and no XACK. Messages stay in the PEL.
        """
        import consumer as cons_module
        from consumer import flush_batch

        pool, conn = mock_db_pool
        cons_module.db_breaker = CircuitBreaker("postgres", failure_threshold=1, recovery_timeout=30)
        cons_module.db_breaker.on_failure()   # force open
        cons_module.redis_breakers = [
            CircuitBreaker("redis-xack-0", failure_threshold=3, recovery_timeout=30)
        ]

        await flush_batch(pool, mock_redis, "orders:stream:0", 0,
                          [("id-1", sample_stream_fields)])

        conn.executemany.assert_not_called()
        mock_redis.xack.assert_not_called()

    @pytest.mark.asyncio
    async def test_db_retries_then_succeeds_xack_called(
        self, mock_db_pool, mock_redis, sample_stream_fields
    ):
        """
        executemany fails once, succeeds on second attempt.
        XACK must be called after eventual success.
        """
        import consumer as cons_module
        from consumer import flush_batch

        pool, conn = mock_db_pool
        cons_module.db_breaker = CircuitBreaker("postgres", failure_threshold=3, recovery_timeout=30)
        cons_module.redis_breakers = [
            CircuitBreaker("redis-xack-0", failure_threshold=3, recovery_timeout=30)
        ]

        call_count = 0

        async def flaky_executemany(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("db blip")

        conn.executemany.side_effect = flaky_executemany

        await flush_batch(pool, mock_redis, "orders:stream:0", 0,
                          [("id-1", sample_stream_fields)],
                          _sleep=AsyncMock())

        assert call_count == 2
        mock_redis.xack.assert_called_once()
