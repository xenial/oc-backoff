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
  retryDelay,
  type CircuitOptions,
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

interface ParkedSession {
  sessionID: string
  providerID: string
  parkedAt: number
  generation: number
  timer: ReturnType<typeof setTimeout>
}

function numberOption(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : fallback
}

function parseOptions(raw: unknown): BackoffPluginOptions {
  const opts = (raw ?? {}) as Record<string, unknown>
  const circuit = (opts.circuit ?? {}) as Record<string, unknown>
  const watchdog = (opts.watchdog ?? {}) as Record<string, unknown>
  return {
    baseDelayMs: numberOption(opts.baseDelayMs, DEFAULT_BASE_DELAY_MS),
    maxDelayMs: numberOption(opts.maxDelayMs, DEFAULT_MAX_DELAY_MS),
    circuit: {
      failureThreshold: numberOption(circuit.failureThreshold, DEFAULT_CIRCUIT.failureThreshold),
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
    // Last provider a session retried against. session.execution.failed does
    // not carry the model ref, so the retry hook's identity is the best
    // available attribution for a parked session.
    const lastProvider = new Map<string, string>()
    const providerFor = (sessionID: string): string | undefined => lastProvider.get(sessionID)
    const log = (message: string) => console.error(`[oc-backoff] ${message}`)

    function circuitFor(providerID: string): CircuitBreaker {
      let circuit = circuits.get(providerID)
      if (!circuit) {
        circuit = new CircuitBreaker(opts.circuit)
        circuits.set(providerID, circuit)
      }
      return circuit
    }

    // Layer 1: shape the retry policy inside OpenCode's attempt budget.
    // OpenCode classifies the failure and proposes a decision; we replace the
    // delay with full-jitter exponential backoff and veto retries that are
    // either terminal (auth/invalid-request) or directed at an open circuit.
    const retryHook = await ctx.session.hook("retry", (event) => {
      const providerID = event.model.providerID
      lastProvider.set(event.sessionID, providerID)
      const classified = classify(event.error)

      if (classified.cls === "terminal") {
        event.decision = { retry: false }
        log(`terminal error for provider=${providerID}: not retrying`)
        return
      }

      const circuit = circuitFor(providerID)
      if (!circuit.admit()) {
        const wait = circuit.retryAfterMs()
        log(`circuit open for provider=${providerID}: failing fast (probe in ~${Math.round((wait ?? 0) / 1000)}s)`)
        event.decision = { retry: false }
        return
      }

      const delay = retryDelay(classified, event.attempt, opts)
      const proposed = event.decision.retry ? event.decision.delay : undefined
      log(
        `provider=${providerID} attempt=${event.attempt} class=${classified.cls} ` +
          `delay=${Math.round(delay / 1000)}s (was ${proposed === undefined ? "none" : `${Math.round(proposed / 1000)}s`})`,
      )
      event.decision = { retry: true, delay }
    })

    async function park(sessionID: string, providerID: string): Promise<void> {
      if (parked.has(sessionID)) return
      const entry: ParkedSession = {
        sessionID,
        providerID,
        parkedAt: Date.now(),
        generation: 0,
        timer: setTimeout(() => void reparkOrDispatch(entry), 0),
      }
      parked.set(sessionID, entry)
      log(`parked session=${sessionID} provider=${providerID}; waiting for provider recovery`)
    }

    async function reparkOrDispatch(entry: ParkedSession): Promise<void> {
      if (outageExpired(entry.parkedAt, opts.watchdog.outageToleranceMs)) {
        parked.delete(entry.sessionID)
        log(`session=${entry.sessionID} exceeded outage tolerance; giving up`)
        return
      }

      const circuit = circuitFor(entry.providerID)
      if (!circuit.admit()) {
        const wait = circuit.retryAfterMs() ?? opts.circuit.baseCooldownMs
        entry.timer = setTimeout(() => void reparkOrDispatch(entry), wait + 1_000)
        return
      }

      try {
        await ctx.session.prompt({
          sessionID: entry.sessionID,
          text: "The provider outage appears to have passed. Resume the task you were working on.",
        })
        // prompt() resolves once the message is queued, not when the model
        // finishes. Wait for the run to settle so a still-down provider
        // surfaces here as a rejection instead of an immediate fake success
        // (which would unpark, reset the circuit, and spam re-dispatches).
        await ctx.session.wait({ sessionID: entry.sessionID })
        circuit.recordSuccess()
        parked.delete(entry.sessionID)
        log(`re-dispatched session=${entry.sessionID} after ${entry.generation} park generation(s)`)
      } catch (error) {
        const classified = classify(error)
        if (classified.cls === "terminal") {
          parked.delete(entry.sessionID)
          log(`session=${entry.sessionID} re-dispatch hit terminal error; giving up: ${String(error)}`)
          return
        }
        circuit.recordFailure()
        entry.generation += 1
        const delay = redispatchDelay(entry.generation, {
          baseDelayMs: opts.circuit.baseCooldownMs,
          maxRedispatchDelayMs: opts.watchdog.maxRedispatchDelayMs,
        })
        log(
          `session=${entry.sessionID} re-dispatch failed (class=${classified.cls}); ` +
            `next attempt in ~${Math.round(delay / 60_000)}min`,
        )
        entry.timer = setTimeout(() => void reparkOrDispatch(entry), delay)
      }
    }

    // Layer 2: watchdog. When a session errors out after exhausting retries on
    // a transient failure, park it and re-dispatch with exponential backoff
    // until the provider recovers or the outage tolerance (default 12 h) is
    // exceeded. This covers outages far longer than OpenCode's attempt budget.
    const controller = new AbortController()
    if (opts.watchdog.enabled) {
      void (async () => {
        try {
          for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
            if (event.type !== "session.execution.failed") continue
            const sessionID = event.data.sessionID
            if (parked.has(sessionID)) continue
            const classified = classify(event.data.error)
            if (classified.cls === "terminal") continue
            const providerID = providerFor(sessionID) ?? "unknown"
            circuitFor(providerID).recordFailure()
            await park(sessionID, providerID)
          }
        } catch (error) {
          if (!controller.signal.aborted) log(`event stream ended unexpectedly: ${String(error)}`)
        }
      })()
    }

    return () => {
      controller.abort()
      void retryHook.dispose()
      for (const entry of parked.values()) clearTimeout(entry.timer)
      parked.clear()
    }
  },
})
