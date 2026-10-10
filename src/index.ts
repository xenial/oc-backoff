import { Plugin } from "@opencode/plugin"
import {
  CircuitBreaker,
  DEFAULT_BASE_DELAY_MS,
  DEFAULT_CIRCUIT,
  DEFAULT_MAX_DELAY_MS,
  DEFAULT_MAX_REDISPATCH_DELAY_MS,
  DEFAULT_OUTAGE_TOLERANCE_MS,
  classify,
  outageExpired,
  redispatchDelay,
  retryAfterFromHeaders,
  retryDelay,
  type CircuitOptions,
  type ClassifiedError,
} from "./backoff.js"

interface BackoffPluginOptions {
  baseDelayMs: number
  maxDelayMs: number
  circuit: CircuitOptions
  watchdog: {
    enabled: boolean
    maxRedispatchDelayMs: number
    outageToleranceMs: number
  }
}

interface ProviderRequest {
  providerID: string
  retryAfterAt?: number
  blocked: boolean
}

interface ParkedSession {
  sessionID: string
  providerID: string
  parkedAt: number
  generation: number
  nextDispatchAt: number
  dispatching: boolean
  controller: AbortController
  timer?: ReturnType<typeof setTimeout>
}

const MAX_TIMER_MS = 2_147_483_647
const REDISPATCH_MARKER = "oc-backoff.redispatch"

function recordOption(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}
}

function numberOption(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? Math.min(Number.MAX_SAFE_INTEGER, value)
    : fallback
}

function parseOptions(raw: unknown): BackoffPluginOptions {
  const opts = recordOption(raw)
  const circuit = recordOption(opts.circuit)
  const watchdog = recordOption(opts.watchdog)
  return {
    baseDelayMs: numberOption(opts.baseDelayMs, DEFAULT_BASE_DELAY_MS),
    maxDelayMs: numberOption(opts.maxDelayMs, DEFAULT_MAX_DELAY_MS),
    circuit: {
      failureThreshold: Math.ceil(numberOption(circuit.failureThreshold, DEFAULT_CIRCUIT.failureThreshold)),
      baseCooldownMs: numberOption(circuit.baseCooldownMs, DEFAULT_CIRCUIT.baseCooldownMs),
      maxCooldownMs: numberOption(circuit.maxCooldownMs, DEFAULT_CIRCUIT.maxCooldownMs),
    },
    watchdog: {
      enabled: watchdog.enabled !== false,
      maxRedispatchDelayMs: numberOption(watchdog.maxRedispatchDelayMs, DEFAULT_MAX_REDISPATCH_DELAY_MS),
      outageToleranceMs: numberOption(watchdog.outageToleranceMs, DEFAULT_OUTAGE_TOLERANCE_MS),
    },
  }
}

export default Plugin.define({
  id: "oc-backoff",
  async setup(ctx) {
    const opts = parseOptions(ctx.options)
    const circuits = new Map<string, CircuitBreaker>()
    const parked = new Map<string, ParkedSession>()
    const requests = new Map<string, ProviderRequest>()
    const expired = new Set<string>()
    const controller = new AbortController()
    const log = (message: string) => console.error(`[oc-backoff] ${message}`)

    function circuitFor(providerID: string): CircuitBreaker {
      let circuit = circuits.get(providerID)
      if (!circuit) {
        circuit = new CircuitBreaker(opts.circuit)
        circuits.set(providerID, circuit)
      }
      return circuit
    }

    function cancel(sessionID: string): void {
      const entry = parked.get(sessionID)
      if (!entry) return
      parked.delete(sessionID)
      clearTimeout(entry.timer)
      entry.controller.abort()
      circuitFor(entry.providerID).releaseProbe(sessionID)
    }

    function policy(error: unknown, sessionID: string): ClassifiedError {
      const classified = classify(error)
      const retryAfterAt = requests.get(sessionID)?.retryAfterAt
      if (retryAfterAt !== undefined) classified.retryAfterMs = Math.max(0, retryAfterAt - Date.now())
      return classified
    }

    // The structured retry error omits headers. Capture them at the HTTP
    // boundary, and track execution requests even if no retry hook ever fires.
    const modelHook = await ctx.session.hook("model.request", (event) => {
      if (event.kind !== "primary" && event.kind !== "compaction") return
      requests.set(event.sessionID, { providerID: event.model.providerID, blocked: false })
    })
    const responseHook = await ctx.session.hook("http.response", (event) => {
      if (event.kind !== "primary" && event.kind !== "compaction") return
      const request = requests.get(event.sessionID)
      if (!request || request.providerID !== event.model.providerID) return
      const wait = retryAfterFromHeaders(event.response.headers)
      request.retryAfterAt = wait === undefined ? undefined : Date.now() + wait
    })
    const promptHook = await ctx.session.hook("prompt", (event) => {
      if (event.metadata?.[REDISPATCH_MARKER] === true) return
      cancel(event.sessionID)
      expired.delete(event.sessionID)
      requests.delete(event.sessionID)
    })
    const retryHook = await ctx.session.hook("retry", (event) => {
      const providerID = event.model.providerID
      let request = requests.get(event.sessionID)
      if (!request || request.providerID !== providerID) {
        request = { providerID, blocked: false }
        requests.set(event.sessionID, request)
      }
      const classified = policy(event.error, event.sessionID)
      if (classified.cls === "terminal") {
        event.decision = { retry: false }
        log(`terminal error for provider=${providerID}: not retrying`)
        return
      }

      const circuit = circuitFor(providerID)
      if (!circuit.acquireProbe(event.sessionID)) {
        request.blocked = true
        event.decision = { retry: false }
        log(`circuit open for provider=${providerID}: not retrying`)
        return
      }

      // OpenCode counts the initial physical request as attempt 1, so the
      // first retry hook (attempt 2) uses the base backoff ceiling.
      const delay = retryDelay(classified, event.attempt - 1, opts)
      const proposed = event.decision.retry ? event.decision.delay : undefined
      event.decision = { retry: true, delay }
      log(`provider=${providerID} attempt=${event.attempt} class=${classified.cls} delay=${Math.round(delay / 1000)}s (was ${proposed ?? "none"}ms)`)
    })

    function active(entry: ParkedSession): boolean {
      return !controller.signal.aborted && parked.get(entry.sessionID) === entry
    }

    function arm(entry: ParkedSession): void {
      if (!active(entry)) return
      clearTimeout(entry.timer)
      const deadline = entry.parkedAt + opts.watchdog.outageToleranceMs
      const wakeAt = entry.dispatching ? deadline : Math.min(deadline, entry.nextDispatchAt)
      // Long provider waits must not overflow setTimeout and become 1 ms.
      entry.timer = setTimeout(() => void dispatch(entry), Math.max(1, Math.min(MAX_TIMER_MS, wakeAt - Date.now())))
    }

    function schedule(entry: ParkedSession, classified: ClassifiedError): void {
      entry.dispatching = false
      const jitter = redispatchDelay(entry.generation, {
        baseDelayMs: opts.circuit.baseCooldownMs,
        maxRedispatchDelayMs: opts.watchdog.maxRedispatchDelayMs,
      })
      const minimum = Math.min(opts.baseDelayMs, opts.watchdog.maxRedispatchDelayMs)
      const directed = classified.retryAfterMs !== undefined || classified.cls === "quota"
        ? retryDelay(classified, 1, opts)
        : 0
      entry.nextDispatchAt = Date.now() + Math.max(minimum, jitter, directed)
      arm(entry)
    }

    async function dispatch(entry: ParkedSession): Promise<void> {
      if (!active(entry)) return
      if (outageExpired(entry.parkedAt, opts.watchdog.outageToleranceMs)) {
        expired.add(entry.sessionID)
        cancel(entry.sessionID)
        log(`session=${entry.sessionID} exceeded outage tolerance; giving up`)
        return
      }
      if (entry.dispatching || Date.now() < entry.nextDispatchAt) {
        arm(entry)
        return
      }
      const circuit = circuitFor(entry.providerID)
      if (!circuit.acquireProbe(entry.sessionID)) {
        // A held half-open probe has no cooldown deadline: poll until its
        // lifecycle event releases it. Every wake also enforces tolerance.
        entry.nextDispatchAt = Date.now() + (circuit.retryAfterMs() ?? 1_000)
        arm(entry)
        return
      }

      entry.dispatching = true
      const generation = entry.generation
      arm(entry)
      try {
        await ctx.session.prompt({
          sessionID: entry.sessionID,
          text: "Resume the task from where the provider failure interrupted it. Avoid repeating completed work.",
          metadata: { [REDISPATCH_MARKER]: true },
          delivery: "queue",
        }, { signal: entry.controller.signal })
        // Admission and wait() are not execution outcomes. Keep this entry
        // parked until execution.succeeded/failed/interrupted arrives.
        if (active(entry)) log(`re-dispatched session=${entry.sessionID} generation=${generation}; awaiting execution outcome`)
      } catch (error) {
        if (!active(entry) || !entry.dispatching || generation !== entry.generation) return
        circuit.releaseProbe(entry.sessionID)
        const classified = classify(error)
        if (classified.cls === "terminal") {
          cancel(entry.sessionID)
          log(`session=${entry.sessionID} prompt admission failed; giving up: ${String(error)}`)
          return
        }
        // An API transport failure is not evidence the inference provider is
        // down. Back off admission without incrementing its circuit counter.
        entry.generation += 1
        schedule(entry, classified)
        log(`session=${entry.sessionID} prompt admission failed; backing off`)
      }
    }

    // Always observe lifecycle events: the circuit remains functional when
    // watchdog redispatch is disabled. A failed resumed execution is counted
    // here exactly once, rather than inferred from prompt()/wait().
    async function consumeEvents(): Promise<void> {
      while (!controller.signal.aborted) {
        try {
          for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
            if (controller.signal.aborted) return
            if (event.type === "session.compaction.ended") {
              const providerID = event.data.model?.providerID
              if (providerID) circuitFor(providerID).recordSuccess()
              continue
            }
            if (event.type === "session.execution.started") {
              const entry = parked.get(event.data.sessionID)
              if (entry && !entry.dispatching) cancel(entry.sessionID)
              continue
            }
            if (event.type === "session.model.selected" || event.type === "session.agent.selected" || event.type === "session.deleted" || event.type === "session.execution.interrupted") {
              const sessionID = event.data.sessionID
              cancel(sessionID)
              const providerID = requests.get(sessionID)?.providerID
              if (providerID) circuitFor(providerID).releaseProbe(sessionID)
              requests.delete(sessionID)
              if (event.type !== "session.execution.interrupted") expired.delete(sessionID)
              continue
            }
            if (event.type === "session.step.ended" || event.type === "session.execution.succeeded") {
              const sessionID = event.data.sessionID
              const providerID = requests.get(sessionID)?.providerID ?? parked.get(sessionID)?.providerID
              if (providerID) circuitFor(providerID).recordSuccess()
              if (event.type === "session.execution.succeeded") {
                cancel(sessionID)
                requests.delete(sessionID)
                expired.delete(sessionID)
              }
              continue
            }
            if (event.type !== "session.execution.failed") continue
            const sessionID = event.data.sessionID
            if (expired.has(sessionID)) {
              requests.delete(sessionID)
              continue
            }
            const entry = parked.get(sessionID)
            if (entry && !entry.dispatching) continue
            const classified = policy(event.data.error, sessionID)
            const request = requests.get(sessionID)
            const providerID = request?.providerID ?? entry?.providerID
            if (classified.cls === "terminal") {
              cancel(sessionID)
              if (providerID) circuitFor(providerID).releaseProbe(sessionID)
              requests.delete(sessionID)
              continue
            }
            // Never merge unattributed failures into a shared "unknown"
            // provider circuit. Model requests supply reliable attribution.
            if (!providerID) {
              log(`session=${sessionID} transient failure has no provider attribution; leaving it alone`)
              continue
            }
            const circuit = circuitFor(providerID)
            if (!request?.blocked) {
              const before = circuit.snapshot()
              circuit.recordFailure(Date.now(), sessionID)
              const after = circuit.snapshot()
              if (before.state !== after.state || before.cooldownMs !== after.cooldownMs) {
                log(`provider=${providerID} circuit=${after.state} cooldown=${after.cooldownMs ?? 0}ms`)
              }
            }
            circuit.releaseProbe(sessionID)
            if (!opts.watchdog.enabled) {
              requests.delete(sessionID)
              continue
            }
            if (entry) {
              if (entry.providerID !== providerID) {
                circuitFor(entry.providerID).releaseProbe(sessionID)
                entry.providerID = providerID
              }
              entry.generation += 1
              schedule(entry, classified)
            } else {
              const parkedEntry: ParkedSession = {
                sessionID,
                providerID,
                parkedAt: Date.now(),
                generation: 0,
                nextDispatchAt: 0,
                dispatching: false,
                controller: new AbortController(),
              }
              parked.set(sessionID, parkedEntry)
              schedule(parkedEntry, classified)
              log(`parked session=${sessionID} provider=${providerID}; waiting to probe recovery`)
            }
          }
          if (!controller.signal.aborted) log("event stream ended; reconnecting")
        } catch (error) {
          if (!controller.signal.aborted) log(`event stream failed; reconnecting: ${String(error)}`)
        }
        if (controller.signal.aborted) return
        await new Promise<void>((resolve) => {
          const done = () => {
            clearTimeout(timer)
            controller.signal.removeEventListener("abort", done)
            resolve()
          }
          const timer = setTimeout(done, 1_000)
          controller.signal.addEventListener("abort", done, { once: true })
        })
      }
    }

    void consumeEvents()
    return async () => {
      controller.abort()
      for (const sessionID of parked.keys()) cancel(sessionID)
      requests.clear()
      expired.clear()
      await Promise.all([modelHook, responseHook, promptHook, retryHook].map((hook) => hook.dispose()))
    }
  },
})
