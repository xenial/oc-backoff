/**
 * Pure backoff, classification, and circuit-breaker logic for opencode-backoff.
 *
 * This module has no dependency on the OpenCode plugin API so it can be unit
 * tested directly with node:test.
 */

export interface BackoffOptions {
  /** First retry delay in milliseconds. Default 2000. */
  baseDelayMs: number
  /** Hard ceiling for any single retry delay. Default 300_000 (5 min). */
  maxDelayMs: number
  /** Random source, injectable for tests. Defaults to Math.random. */
  random?: () => number
}

export const DEFAULT_BASE_DELAY_MS = 2_000
export const DEFAULT_MAX_DELAY_MS = 300_000
export const DEFAULT_MAX_REDISPATCH_DELAY_MS = 1_800_000 // 30 min
export const DEFAULT_OUTAGE_TOLERANCE_MS = 43_200_000 // 12 h
export const DEFAULT_PROBE_MODEL_COUNT = 1

/**
 * Full-jitter exponential backoff: uniform random in [0, min(cap, base * 2^n)].
 *
 * Full jitter (rather than equal jitter or no jitter) prevents a fleet of
 * parallel agents from synchronizing their retries after a shared rate limit.
 */
export function backoffDelay(attempt: number, opts: BackoffOptions): number {
  const random = opts.random ?? Math.random
  const ceiling = Math.min(opts.maxDelayMs, opts.baseDelayMs * Math.pow(2, Math.max(0, attempt - 1)))
  return Math.floor(random() * ceiling)
}

export type ErrorClass =
  | "rate-limit"
  | "server-error"
  | "network"
  | "overloaded"
  | "quota" // long-window account/usage limits: worth waiting out, not worth hammering
  | "terminal"

export interface ClassifiedError {
  cls: ErrorClass
  /** Provider-directed wait from Retry-After / retry-after-ms headers, if any. */
  retryAfterMs?: number
}

interface ErrorLike {
  status?: number
  statusCode?: number
  type?: string
  message?: string
  data?: {
    statusCode?: number
    message?: string
    responseBody?: string
    responseHeaders?: Record<string, string>
    isRetryable?: boolean
  }
}

const NETWORK_PATTERNS = [
  "econnreset",
  "econnrefused",
  "econnaborted",
  "etimedout",
  "fetch failed",
  "failed to fetch",
  "socket hang up",
  "network error",
  "connection reset",
  "connection refused",
  "connection timeout",
]

const RATE_LIMIT_PATTERNS = ["rate limit", "too many requests", "too_many_requests", "rate increased too quickly"]
const OVERLOAD_PATTERNS = ["overloaded", "exhausted", "unavailable", "capacity"]
const QUOTA_PATTERNS = ["usage limit", "usagelimterror", "quota exceeded", "monthly quota", "weekly limit", "freeusagelimterror"]

function normalize(error: unknown): ErrorLike {
  if (error && typeof error === "object") return error as ErrorLike
  return { message: String(error) }
}

function statusOf(error: ErrorLike): number | undefined {
  return error.status ?? error.statusCode ?? error.data?.statusCode
}

function textOf(error: ErrorLike): string {
  return [error.message, error.type, error.data?.message, error.data?.responseBody]
    .filter((part): part is string => typeof part === "string")
    .join("\n")
    .toLowerCase()
}

function retryAfterOf(error: ErrorLike): number | undefined {
  const headers = error.data?.responseHeaders
  if (!headers) return undefined
  const ms = headers["retry-after-ms"]
  if (ms !== undefined) {
    const parsed = Number.parseFloat(ms)
    if (!Number.isNaN(parsed) && parsed >= 0) return parsed
  }
  const seconds = headers["retry-after"]
  if (seconds !== undefined) {
    const parsedSeconds = Number.parseFloat(seconds)
    if (!Number.isNaN(parsedSeconds) && parsedSeconds >= 0) return Math.ceil(parsedSeconds * 1000)
    const asDate = Date.parse(seconds)
    if (!Number.isNaN(asDate)) {
      const delta = asDate - Date.now()
      if (delta > 0) return Math.ceil(delta)
    }
  }
  return undefined
}

/**
 * Classify a provider failure into a retry policy bucket.
 *
 * - terminal: auth failures, invalid requests, missing models, context
 *   overflow. Never retry; masking these turns a real bug into a hang.
 * - quota: long-window account/usage limits. Retryable within the outage
 *   tolerance window but always at the provider-directed wait or the max
 *   delay, never at the fast end of the backoff curve.
 * - rate-limit / server-error / network / overloaded: transient; retry with
 *   jittered exponential backoff.
 */
export function classify(rawError: unknown): ClassifiedError {
  const error = normalize(rawError)
  const status = statusOf(error)
  const text = textOf(error)
  const retryAfterMs = retryAfterOf(error)

  if (text.includes("context") && text.includes("overflow")) return { cls: "terminal" }
  if (status !== undefined) {
    if (status === 401 || status === 403) return { cls: "terminal" }
    if (status === 400 || status === 404 || status === 422) return { cls: "terminal" }
    if (status === 402) return { cls: "quota", retryAfterMs }
    if (status === 429) {
      const cls: ErrorClass = QUOTA_PATTERNS.some((p) => text.includes(p)) ? "quota" : "rate-limit"
      return { cls, retryAfterMs }
    }
    if (status >= 500) return { cls: "server-error", retryAfterMs }
  }

  if (QUOTA_PATTERNS.some((p) => text.includes(p))) return { cls: "quota", retryAfterMs }
  if (RATE_LIMIT_PATTERNS.some((p) => text.includes(p))) return { cls: "rate-limit", retryAfterMs }
  if (NETWORK_PATTERNS.some((p) => text.includes(p))) return { cls: "network", retryAfterMs }
  if (OVERLOAD_PATTERNS.some((p) => text.includes(p))) return { cls: "overloaded", retryAfterMs }

  if (error.data?.isRetryable === true) return { cls: "server-error", retryAfterMs }

  return { cls: "terminal" }
}

/**
 * Decide the delay for a retry attempt, honoring provider-directed waits.
 *
 * Provider Retry-After values are authoritative but floored at the base delay
 * and capped at maxDelayMs for transient classes. Quota-class waits are
 * allowed to exceed maxDelayMs because a usage window resetting in 40 minutes
 * is real information, not noise.
 */
export function retryDelay(classified: ClassifiedError, attempt: number, opts: BackoffOptions): number {
  if (classified.retryAfterMs !== undefined) {
    if (classified.cls === "quota") {
      return Math.max(opts.baseDelayMs, classified.retryAfterMs)
    }
    return Math.max(opts.baseDelayMs, Math.min(opts.maxDelayMs, classified.retryAfterMs))
  }
  const computed = backoffDelay(attempt, opts)
  return classified.cls === "quota" ? Math.max(computed, opts.maxDelayMs) : computed
}

export type CircuitState = "closed" | "open" | "half-open"

export interface CircuitSnapshot {
  state: CircuitState
  consecutiveFailures: number
  openedAt?: number
  cooldownMs?: number
}

export interface CircuitOptions {
  /** Consecutive exhausted retry sequences before the circuit opens. Default 3. */
  failureThreshold: number
  /** First cooldown after opening. Default 900_000 (15 min). */
  baseCooldownMs: number
  /** Cooldown ceiling. Default 7_200_000 (2 h). */
  maxCooldownMs: number
  random?: () => number
}

export const DEFAULT_CIRCUIT: Omit<CircuitOptions, "random"> = {
  failureThreshold: 3,
  baseCooldownMs: 900_000,
  maxCooldownMs: 7_200_000,
}

/**
 * Per-provider circuit breaker. Open after `failureThreshold` consecutive
 * exhausted retry sequences; while open, fail fast instead of burning every
 * session's attempt budget against a dead provider. Cooldown doubles on each
 * consecutive failed probe, up to maxCooldownMs.
 */
export class CircuitBreaker {
  private state: CircuitState = "closed"
  private consecutiveFailures = 0
  private openedAt = 0
  private cooldownMs: number

  constructor(private readonly opts: CircuitOptions = DEFAULT_CIRCUIT) {
    this.cooldownMs = opts.baseCooldownMs
  }

  snapshot(now: number = Date.now()): CircuitSnapshot {
    // Pure observation: a state is only ever changed by recordFailure /
    // recordSuccess, never by being looked at. An open circuit whose
    // cooldown has elapsed reports "half-open" here without transitioning.
    const state: CircuitState =
      this.state === "open" && now >= this.openedAt + this.cooldownMs ? "half-open" : this.state
    return {
      state,
      consecutiveFailures: this.consecutiveFailures,
      openedAt: this.state === "closed" ? undefined : this.openedAt,
      cooldownMs: this.state === "closed" ? undefined : this.cooldownMs,
    }
  }

  /** Whether a new request should be admitted right now. */
  admit(now: number = Date.now()): boolean {
    return this.state !== "open" || now >= this.openedAt + this.cooldownMs
  }

  /** Milliseconds until a probe is allowed, if the circuit is open. */
  retryAfterMs(now: number = Date.now()): number | undefined {
    if (this.admit(now)) return undefined
    return Math.max(0, this.openedAt + this.cooldownMs - now)
  }

  /**
   * Record that a full retry sequence was exhausted against this provider.
   * Pass the failure timestamp explicitly when it is known; observing the
   * breaker between the failure and this call does not count as a probe.
   */
  recordFailure(failedAt?: number): void {
    if (this.state === "open") {
      if (failedAt === undefined) failedAt = Date.now()
      if (failedAt < this.openedAt + this.cooldownMs) {
        // Still cooling down: not a probe, just another report of the same outage.
        return
      }
      // A request was admitted after cooldown and failed: probe failed.
      // Re-open with doubled cooldown.
      this.consecutiveFailures += 1
      this.openedAt = failedAt
      this.cooldownMs = Math.min(this.opts.maxCooldownMs, this.cooldownMs * 2)
      return
    }
    this.consecutiveFailures += 1
    if (this.consecutiveFailures >= this.opts.failureThreshold) {
      this.state = "open"
      this.openedAt = failedAt ?? Date.now()
    }
  }

  /** Record a successful request; fully resets the breaker. */
  recordSuccess(): void {
    this.state = "closed"
    this.consecutiveFailures = 0
    this.cooldownMs = this.opts.baseCooldownMs
    this.openedAt = 0
  }

}

/**
 * Delay before re-dispatching a parked prompt after an exhaustion event.
 * Slower curve than in-request retries: the provider has already proven it
 * cannot serve this session, so each re-dispatch is a fresh probe.
 */
export function redispatchDelay(
  generation: number,
  opts: { baseDelayMs: number; maxRedispatchDelayMs: number; random?: () => number },
): number {
  const random = opts.random ?? Math.random
  const ceiling = Math.min(opts.maxRedispatchDelayMs, opts.baseDelayMs * Math.pow(2, Math.max(0, generation)))
  return Math.floor(random() * ceiling)
}

/** Whether a parked session has exceeded the outage tolerance window. */
export function outageExpired(parkedAt: number, toleranceMs: number, now: number = Date.now()): boolean {
  return now - parkedAt >= toleranceMs
}
