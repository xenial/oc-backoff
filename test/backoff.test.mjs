import { test } from "node:test"
import assert from "node:assert/strict"
import {
  CircuitBreaker,
  backoffDelay,
  classify,
  outageExpired,
  redispatchDelay,
  retryDelay,
} from "../dist/backoff.js"

const OPTS = { baseDelayMs: 2_000, maxDelayMs: 300_000 }

test("backoffDelay stays within [0, min(cap, base * 2^(attempt-1))]", () => {
  for (let attempt = 1; attempt <= 12; attempt++) {
    const ceiling = Math.min(OPTS.maxDelayMs, OPTS.baseDelayMs * Math.pow(2, attempt - 1))
    for (let i = 0; i < 200; i++) {
      const delay = backoffDelay(attempt, OPTS)
      assert.ok(delay >= 0, `attempt=${attempt} delay=${delay} below 0`)
      assert.ok(delay <= ceiling, `attempt=${attempt} delay=${delay} above ceiling=${ceiling}`)
    }
  }
})

test("backoffDelay growth: attempt 10 ceiling hits the cap", () => {
  const c1 = Math.min(OPTS.maxDelayMs, OPTS.baseDelayMs * Math.pow(2, 0))
  const c10 = Math.min(OPTS.maxDelayMs, OPTS.baseDelayMs * Math.pow(2, 9))
  assert.equal(c1, 2_000)
  assert.equal(c10, OPTS.maxDelayMs)
})

test("backoffDelay uses injected random source", () => {
  const delay = backoffDelay(3, { ...OPTS, random: () => 0.5 })
  assert.equal(delay, Math.floor(0.5 * 8_000))
})

test("classify: 429 is rate-limit", () => {
  assert.equal(classify({ status: 429 }).cls, "rate-limit")
  assert.equal(classify({ data: { statusCode: 429 } }).cls, "rate-limit")
})

test("classify: 5xx is server-error", () => {
  for (const status of [500, 502, 503, 504]) {
    assert.equal(classify({ status }).cls, "server-error")
  }
})

test("classify: auth and invalid-request are terminal", () => {
  for (const status of [400, 401, 403, 404, 422]) {
    assert.equal(classify({ status }).cls, "terminal", `status=${status}`)
  }
})

test("classify: network patterns are network", () => {
  for (const message of ["fetch failed", "socket hang up", "ECONNRESET", "connection reset by peer"]) {
    assert.equal(classify({ message }).cls, "network", message)
  }
})

test("classify: overloaded and quota text", () => {
  assert.equal(classify({ message: "Provider is overloaded" }).cls, "overloaded")
  assert.equal(classify({ status: 429, message: "Weekly usage limit reached" }).cls, "quota")
  assert.equal(classify({ status: 402 }).cls, "quota")
})

test("classify: context overflow is terminal", () => {
  assert.equal(classify({ message: "context overflow: prompt too large" }).cls, "terminal")
})

test("classify: unknown errors are terminal (fail safe)", () => {
  assert.equal(classify({ message: "some random provider complaint" }).cls, "terminal")
  assert.equal(classify(undefined).cls, "terminal")
})

test("classify: isRetryable flag falls back to server-error", () => {
  assert.equal(classify({ data: { isRetryable: true, message: "weird" } }).cls, "server-error")
})

test("retryDelay honors retry-after-ms header within cap", () => {
  const delay = retryDelay({ cls: "rate-limit", retryAfterMs: 45_000 }, 1, OPTS)
  assert.equal(delay, 45_000)
})

test("retryDelay caps transient retry-after at maxDelayMs", () => {
  const delay = retryDelay({ cls: "server-error", retryAfterMs: 3_600_000 }, 1, OPTS)
  assert.equal(delay, OPTS.maxDelayMs)
})

test("retryDelay lets quota retry-after exceed maxDelayMs", () => {
  const delay = retryDelay({ cls: "quota", retryAfterMs: 3_600_000 }, 1, OPTS)
  assert.equal(delay, 3_600_000)
})

test("retryDelay floors retry-after at baseDelayMs", () => {
  const delay = retryDelay({ cls: "rate-limit", retryAfterMs: 100 }, 1, OPTS)
  assert.equal(delay, OPTS.baseDelayMs)
})

test("retryDelay without headers uses jittered backoff; quota rides at the cap", () => {
  const transient = retryDelay({ cls: "network" }, 2, { ...OPTS, random: () => 1 })
  assert.equal(transient, 4_000)
  const quota = retryDelay({ cls: "quota" }, 1, { ...OPTS, random: () => 0 })
  assert.equal(quota, OPTS.maxDelayMs)
})

test("circuit breaker opens after threshold, blocks admission, half-opens after cooldown", () => {
  const circuit = new CircuitBreaker({ failureThreshold: 3, baseCooldownMs: 1_000, maxCooldownMs: 60_000 })
  let now = 1_000_000

  assert.equal(circuit.admit(now), true)
  circuit.recordFailure(now)
  circuit.recordFailure(now)
  assert.equal(circuit.admit(now), true, "still closed before threshold")
  circuit.recordFailure(now)
  assert.equal(circuit.snapshot(now).state, "open")
  assert.equal(circuit.admit(now), false, "open circuit fails fast")
  assert.equal(circuit.admit(now + 999), false, "still open just before cooldown ends")

  now += 1_001
  assert.equal(circuit.snapshot(now).state, "half-open")
  assert.equal(circuit.admit(now), true, "half-open admits a probe")
})

test("circuit breaker failed probe doubles cooldown; success resets", () => {
  const circuit = new CircuitBreaker({ failureThreshold: 1, baseCooldownMs: 1_000, maxCooldownMs: 60_000 })
  let now = 0

  circuit.recordFailure(now)
  assert.equal(circuit.snapshot().cooldownMs, 1_000)

  // Probes are explicit: recordFailure(failedAt) with a timestamp after the
  // cooldown means an admitted request failed. Merely observing the breaker
  // (snapshot/admit) after cooldown does not count as a probe.
  now += 1_001
  assert.equal(circuit.snapshot(now).state, "half-open")
  circuit.recordFailure(now) // probe failed at `now`, after cooldown elapsed
  assert.equal(circuit.snapshot(now).state, "open")
  assert.equal(circuit.snapshot(now).cooldownMs, 2_000)

  now += 2_001
  assert.equal(circuit.snapshot(now).state, "half-open")
  circuit.recordFailure(now) // probe failed again
  assert.equal(circuit.snapshot(now).state, "open")
  assert.equal(circuit.snapshot(now).cooldownMs, 4_000)

  now += 4_001
  circuit.recordSuccess()
  const snap = circuit.snapshot()
  assert.equal(snap.state, "closed")
  assert.equal(snap.consecutiveFailures, 0)
})

test("circuit breaker cooldown is capped", () => {
  const circuit = new CircuitBreaker({ failureThreshold: 1, baseCooldownMs: 10_000, maxCooldownMs: 15_000 })
  let now = 0
  circuit.recordFailure(now)
  for (let i = 0; i < 5; i++) {
    now += circuit.snapshot(now).cooldownMs + 1
    circuit.recordFailure(now) // explicit probe failure after each cooldown
  }
  assert.equal(circuit.snapshot(now).cooldownMs, 15_000)
})

test("circuit breaker ignores duplicate failure reports during cooldown", () => {
  const circuit = new CircuitBreaker({ failureThreshold: 1, baseCooldownMs: 5_000, maxCooldownMs: 60_000 })
  const now = 1_000_000
  circuit.recordFailure(now)
  // Five sibling sessions all report the same outage seconds later:
  // none of these is a probe, so the cooldown must not double.
  for (let i = 1; i <= 5; i++) circuit.recordFailure(now + i * 100)
  assert.equal(circuit.snapshot(now).cooldownMs, 5_000)
  assert.equal(circuit.snapshot(now).state, "open")
})

test("circuit cooldown is anchored at the first failure, not slid forward by stragglers", () => {
  // Three concurrent sessions exhaust their retries against the same dead
  // provider seconds apart. If openedAt were anchored at the LAST report,
  // each straggler would push the probe point out; a steady trickle of
  // sibling sessions could delay the probe arbitrarily. The window must be
  // anchored at the FIRST failure of the run so the probe fires on schedule.
  const circuit = new CircuitBreaker({ failureThreshold: 3, baseCooldownMs: 5_000, maxCooldownMs: 60_000 })
  const t0 = 1_000_000

  circuit.recordFailure(t0) // session A exhausts at t0
  circuit.recordFailure(t0 + 3_000) // session B exhausts 3s later
  circuit.recordFailure(t0 + 4_900) // session C exhausts 4.9s later, opening the circuit

  assert.equal(circuit.snapshot(t0 + 4_900).state, "open")
  // Probe is due 5s after the FIRST failure, not 5s after the last report.
  assert.equal(circuit.admit(t0 + 5_001), true, "probe due at firstFailure + cooldown")
  assert.equal(circuit.snapshot(t0 + 5_001).state, "half-open")
  // And it must not have slid to lastReport + cooldown (t0 + 9_900).
  assert.equal(circuit.admit(t0 + 9_899), true, "window must not slide to the last report")
})

test("circuit retryAfterMs reports remaining open time", () => {
  const circuit = new CircuitBreaker({ failureThreshold: 1, baseCooldownMs: 5_000, maxCooldownMs: 5_000 })
  const now = 100_000
  circuit.recordFailure(now)
  assert.equal(circuit.retryAfterMs(now + 2_000), 3_000)
  assert.equal(circuit.retryAfterMs(now + 5_001), undefined, "half-open: probe allowed")
})

test("redispatchDelay grows geometrically and caps", () => {
  const opts = { baseDelayMs: 60_000, maxRedispatchDelayMs: 300_000, random: () => 1 }
  assert.equal(redispatchDelay(1, opts), 120_000)
  assert.equal(redispatchDelay(2, opts), 240_000)
  assert.equal(redispatchDelay(3, opts), 300_000)
  assert.equal(redispatchDelay(10, opts), 300_000)
})

test("outageExpired enforces the tolerance window", () => {
  const parkedAt = 1_000_000
  const tolerance = 43_200_000
  assert.equal(outageExpired(parkedAt, tolerance, parkedAt + tolerance - 1), false)
  assert.equal(outageExpired(parkedAt, tolerance, parkedAt + tolerance), true)
})
