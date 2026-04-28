"""
Resilience primitives for the order pipeline.

Three patterns applied in combination:

  Timeout (10 s)
    Each individual attempt is bounded. Prevents a slow Redis/Postgres
    from holding up a virtual thread / event-loop task indefinitely.

  Retry (max 3, delay 1 s, backoff ×2)
    Transient failures (network blip, momentary overload) are retried
    with exponential backoff: waits 1 s before attempt 2, 2 s before
    attempt 3. ALL three retries must fail before the circuit breaker
    counts it as one failure — avoids opening the circuit on brief blips.

  Circuit Breaker (threshold 3, recovery 30 s)
    After 3 total-retry-exhaustion events the circuit opens and calls
    are rejected immediately (fail-fast). After 30 s one probe is
    allowed (HALF_OPEN). On success the circuit closes; on failure the
    30 s window resets.

    States:
      CLOSED    →  normal operation, all calls pass through
      OPEN      →  failing, calls raise CircuitBreakerOpenError instantly
      HALF_OPEN →  one probe attempt; success → CLOSED, failure → OPEN

Usage:
    breaker = CircuitBreaker("redis-shard-0")

    result = await with_resilience(
        lambda: client.xadd(stream, fields=msg),
        circuit_breaker=breaker,
    )
"""

import asyncio
import logging
import time

log = logging.getLogger(__name__)


class CircuitBreakerOpenError(Exception):
    """Raised when a call is rejected because the circuit is open."""


class CircuitBreaker:
    """
    Three-state circuit breaker (CLOSED / OPEN / HALF_OPEN).

    Parameters
    ----------
    name              : human-readable label for logging
    failure_threshold : consecutive total-retry-exhaustion events that open the circuit
    recovery_timeout  : seconds the circuit stays OPEN before moving to HALF_OPEN
    _clock            : injectable clock for testing (default: time.monotonic)
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 3,
        recovery_timeout: float = 30.0,
        _clock=time.monotonic,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._clock = _clock
        self._failure_count = 0
        self._opened_at: float | None = None
        self._state = "closed"

    @property
    def state(self) -> str:
        """Current state, auto-transitioning OPEN → HALF_OPEN when timeout elapses."""
        if self._state == "open":
            if self._opened_at is not None:
                elapsed = self._clock() - self._opened_at
                if elapsed >= self.recovery_timeout:
                    self._state = "half_open"
                    log.info("CircuitBreaker[%s] → HALF_OPEN (probing)", self.name)
        return self._state

    def allow_request(self) -> bool:
        s = self.state
        # CLOSED: always allow. HALF_OPEN: allow one probe. OPEN: reject.
        return s in ("closed", "half_open")

    def on_success(self) -> None:
        if self._state != "closed":
            log.info("CircuitBreaker[%s] → CLOSED (recovered)", self.name)
        self._failure_count = 0
        self._opened_at = None
        self._state = "closed"

    def on_failure(self) -> None:
        self._failure_count += 1
        if self._state == "half_open" or self._failure_count >= self.failure_threshold:
            self._state = "open"
            self._opened_at = self._clock()
            log.warning(
                "CircuitBreaker[%s] → OPEN after %d failure(s) — recovers in %ds",
                self.name, self._failure_count, int(self.recovery_timeout),
            )

    def reset(self) -> None:
        """Hard reset to CLOSED. Call between tests to avoid state bleed."""
        self._failure_count = 0
        self._opened_at = None
        self._state = "closed"


async def with_resilience(
    coro_factory,
    *,
    circuit_breaker: CircuitBreaker,
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    timeout: float = 10.0,
    label: str = "operation",
    _sleep=asyncio.sleep,         # injectable for testing
) -> object:
    """
    Execute an async operation with timeout, retry, and circuit breaker.

    Parameters
    ----------
    coro_factory    : callable returning a fresh coroutine on each call
    circuit_breaker : CircuitBreaker instance shared across calls
    max_retries     : total attempts (not extra retries) — default 3
    delay           : initial wait between attempts in seconds — default 1.0
    backoff         : multiplier applied to delay after each failure — default 2.0
    timeout         : per-attempt deadline in seconds — default 10.0
    label           : logged prefix for diagnostics
    _sleep          : asyncio.sleep replacement (pass AsyncMock in tests)

    Raises
    ------
    CircuitBreakerOpenError : circuit is open, call rejected without attempting
    last exception          : all retries exhausted
    """
    if not circuit_breaker.allow_request():
        raise CircuitBreakerOpenError(
            f"[{label}] circuit '{circuit_breaker.name}' is OPEN — "
            f"retry after {circuit_breaker.recovery_timeout}s"
        )

    wait = delay
    last_exc: BaseException = RuntimeError("no attempts made")

    for attempt in range(1, max_retries + 1):
        try:
            result = await asyncio.wait_for(coro_factory(), timeout=timeout)
            circuit_breaker.on_success()
            return result

        except (Exception, asyncio.TimeoutError) as exc:
            last_exc = exc
            log.warning(
                "[%s] attempt %d/%d failed (%s: %s)",
                label, attempt, max_retries, type(exc).__name__, exc,
            )
            if attempt < max_retries:
                log.debug("[%s] retrying in %.1fs", label, wait)
                await _sleep(wait)
                wait *= backoff

    # All retries exhausted → one circuit-breaker failure
    circuit_breaker.on_failure()
    raise last_exc
