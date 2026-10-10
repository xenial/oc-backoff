import { test } from "node:test"
import assert from "node:assert/strict"
import { setImmediate } from "node:timers/promises"
import plugin from "../dist/index.js"

const TRANSIENT = { type: "provider.internal", message: "upstream failed", status: 503 }
const TERMINAL = { type: "provider.auth", message: "bad credentials", status: 401 }

function eventStream(signal) {
  const queued = []
  let pending
  let ended = false
  const stream = {
    [Symbol.asyncIterator]() { return this },
    next() {
      if (queued.length) return Promise.resolve({ value: queued.shift(), done: false })
      if (ended) return Promise.resolve({ done: true })
      return new Promise((resolve, reject) => { pending = { resolve, reject } })
    },
    return() {
      ended = true
      pending?.resolve({ done: true })
      pending = undefined
      signal.removeEventListener("abort", abort)
      return Promise.resolve({ done: true })
    },
    push(event) {
      if (pending) {
        pending.resolve({ value: event, done: false })
        pending = undefined
      } else queued.push(event)
    },
    fail(error) {
      pending?.reject(error)
      pending = undefined
      ended = true
    },
  }
  const abort = () => { void stream.return() }
  signal.addEventListener("abort", abort, { once: true })
  return stream
}

async function harness(t, options = {}) {
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: 1_000_000 })
  t.mock.method(Math, "random", () => 0.5)
  const logs = []
  t.mock.method(console, "error", (message) => logs.push(message))
  const hooks = new Map()
  const streams = []
  const prompts = []
  const disposed = []
  let promptImpl
  const ctx = {
    options: {
      baseDelayMs: 10,
      maxDelayMs: 100,
      circuit: { failureThreshold: 3, baseCooldownMs: 1_000, maxCooldownMs: 8_000 },
      watchdog: { enabled: true, maxRedispatchDelayMs: 2_000, outageToleranceMs: 10_000 },
      ...options,
    },
    session: {
      async hook(name, handler) {
        hooks.set(name, handler)
        return { async dispose() { disposed.push(name) } }
      },
      async prompt(input, requestOptions) {
        prompts.push({ ...input, signal: requestOptions.signal })
        await hooks.get("prompt")({ sessionID: input.sessionID, metadata: input.metadata })
        if (promptImpl) return promptImpl(input, requestOptions)
        await api.request(input.sessionID)
        await api.emit("session.execution.started", input.sessionID)
        return { id: `message-${prompts.length}` }
      },
      async wait() { throw new Error("wait() cannot determine execution success") },
    },
    event: {
      subscribe({ signal }) {
        const stream = eventStream(signal)
        streams.push(stream)
        return stream
      },
    },
  }
  const cleanup = await plugin.setup(ctx)
  t.after(async () => { await cleanup(); await setImmediate() })
  const api = {
    prompts, streams, logs, disposed, cleanup,
    setPrompt(fn) { promptImpl = fn },
    async tick(ms) { t.mock.timers.tick(ms); await setImmediate() },
    async emit(type, sessionID = "s1", extra = {}) {
      streams.at(-1).push({ type, data: { sessionID, ...extra } })
      await setImmediate()
    },
    async request(sessionID = "s1", providerID = "p1", kind = "primary") {
      await hooks.get("model.request")({ sessionID, model: { providerID, id: "model" }, kind })
    },
    async response(headers, sessionID = "s1", providerID = "p1", kind = "primary") {
      await hooks.get("http.response")({ sessionID, model: { providerID }, kind, response: new Response(null, { status: 429, headers }) })
    },
    async retry(sessionID = "s1", providerID = "p1", error = TRANSIENT, attempt = 2) {
      const event = { sessionID, model: { providerID }, error, attempt, decision: { retry: true, delay: 30 } }
      await hooks.get("retry")(event)
      return event.decision
    },
    async fail(sessionID = "s1", error = TRANSIENT) {
      await api.emit("session.execution.failed", sessionID, { error })
    },
    async userPrompt(sessionID = "s1") { await hooks.get("prompt")({ sessionID }) },
  }
  return api
}

test("retry hook uses the base ceiling for SDK physical attempt 2", async (t) => {
  const h = await harness(t)
  assert.deepEqual(await h.retry(), { retry: true, delay: 5 })
  assert.deepEqual(await h.retry("s1", "p1", TRANSIENT, 3), { retry: true, delay: 10 })
  assert.deepEqual(await h.retry("s1", "p1", TERMINAL), { retry: false })
})

test("HTTP headers survive the structured error boundary; auxiliary responses do not overwrite them", async (t) => {
  const h = await harness(t)
  await h.request()
  await h.response({ "Retry-After-Ms": "80" })
  await h.request("s1", "title-provider", "title")
  await h.response({ "Retry-After-Ms": "999" }, "s1", "title-provider", "title")
  assert.deepEqual(await h.retry(), { retry: true, delay: 80 })
  await h.tick(20)
  assert.deepEqual(await h.retry(), { retry: true, delay: 60 })
  await h.request()
  assert.deepEqual(await h.retry(), { retry: true, delay: 5 }, "fresh request clears old response headers")
})

test("watchdog backs off the first redispatch and waits for a lifecycle success", async (t) => {
  const h = await harness(t)
  await h.request()
  await h.fail()
  await h.tick(499)
  assert.equal(h.prompts.length, 0)
  await h.tick(1)
  assert.equal(h.prompts.length, 1)
  await h.tick(1_000)
  assert.equal(h.prompts.length, 1, "queued prompt is not a completed execution")
  await h.emit("session.execution.succeeded")
  await h.tick(20_000)
  assert.equal(h.prompts.length, 1)
})

test("failed resumed executions keep the original outage deadline", async (t) => {
  const h = await harness(t, { watchdog: { maxRedispatchDelayMs: 2_000, outageToleranceMs: 2_000 } })
  await h.request()
  await h.fail()
  await h.tick(500)
  await h.fail()
  await h.tick(999)
  assert.equal(h.prompts.length, 1)
  await h.tick(1)
  assert.equal(h.prompts.length, 2)
  await h.tick(500)
  await h.fail()
  await h.tick(20_000)
  assert.equal(h.prompts.length, 2, "late failure cannot restart an expired outage window")
  assert.ok(h.logs.some((line) => line.includes("exceeded outage tolerance")))
})

test("terminal resumed failure stops the watchdog", async (t) => {
  const h = await harness(t)
  await h.request()
  await h.fail()
  await h.tick(500)
  await h.fail("s1", TERMINAL)
  await h.tick(20_000)
  assert.equal(h.prompts.length, 1)
})

test("circuit tracks failures with watchdog disabled and isolates providers", async (t) => {
  const h = await harness(t, { watchdog: { enabled: false } })
  for (let i = 0; i < 3; i++) {
    await h.request(`s${i}`)
    await h.fail(`s${i}`)
  }
  assert.deepEqual(await h.retry("another"), { retry: false })
  assert.equal((await h.retry("other-provider", "p2")).retry, true)
  await h.tick(20_000)
  assert.equal(h.prompts.length, 0)
})

test("ordinary successful provider steps reset consecutive failures", async (t) => {
  const h = await harness(t, { watchdog: { enabled: false } })
  for (let i = 0; i < 2; i++) {
    await h.request()
    await h.fail()
  }
  await h.request()
  await h.emit("session.step.ended")
  await h.emit("session.execution.succeeded")
  await h.request()
  await h.fail()
  assert.equal((await h.retry("another")).retry, true)
})

test("only one watchdog session probes a half-open provider", async (t) => {
  const h = await harness(t, { circuit: { failureThreshold: 1, baseCooldownMs: 1_000, maxCooldownMs: 8_000 } })
  await h.request("s1")
  await h.fail("s1")
  await h.request("s2")
  await h.fail("s2")
  await h.tick(500)
  assert.equal(h.prompts.length, 0)
  await h.tick(500)
  assert.equal(h.prompts.length, 1)
  assert.deepEqual(await h.retry("sibling"), { retry: false })
  assert.equal((await h.retry("s1")).retry, true, "the probe may use its own retry sequence")
  await h.fail("s1")
  await h.tick(1_000)
  assert.equal(h.prompts.length, 1, "failed probe doubles cooldown")
  await h.tick(1_000)
  assert.equal(h.prompts.length, 2)
})

for (const reason of ["user-prompt", "session.execution.interrupted", "session.deleted", "session.model.selected", "session.agent.selected", "session.execution.started"]) {
  test(`pending redispatch is cancelled by ${reason}`, async (t) => {
    const h = await harness(t)
    await h.request()
    await h.fail()
    if (reason === "user-prompt") await h.userPrompt()
    else await h.emit(reason)
    await h.tick(20_000)
    assert.equal(h.prompts.length, 0)
  })
}

test("cleanup aborts in-flight prompt admission and prevents late rescheduling", async (t) => {
  const h = await harness(t)
  let rejectPrompt
  h.setPrompt(() => new Promise((_, reject) => { rejectPrompt = reject }))
  await h.request()
  await h.fail()
  await h.tick(500)
  await h.cleanup()
  assert.equal(h.prompts[0].signal.aborted, true)
  rejectPrompt(new Error("fetch failed"))
  await setImmediate()
  await h.tick(20_000)
  assert.equal(h.prompts.length, 1)
  assert.deepEqual(h.disposed.sort(), ["http.response", "model.request", "prompt", "retry"])
})

test("quota reset headers delay watchdog probes beyond the transient cap", async (t) => {
  const h = await harness(t)
  await h.request()
  await h.response({ "Retry-After-Ms": "3000" })
  await h.fail("s1", { type: "provider.quota", status: 429, message: "limit reached" })
  await h.tick(2_999)
  assert.equal(h.prompts.length, 0)
  await h.tick(1)
  assert.equal(h.prompts.length, 1)
})

test("outage deadline wins over a longer circuit cooldown", async (t) => {
  const h = await harness(t, {
    circuit: { failureThreshold: 1, baseCooldownMs: 30_000, maxCooldownMs: 30_000 },
    watchdog: { maxRedispatchDelayMs: 1_000, outageToleranceMs: 2_000 },
  })
  await h.request()
  await h.fail()
  await h.tick(500)
  await h.tick(1_500)
  assert.ok(h.logs.some((line) => line.includes("exceeded outage tolerance")))
  assert.equal(h.prompts.length, 0)
})

test("long watchdog timers do not overflow into immediate redispatch", async (t) => {
  const h = await harness(t, {
    circuit: { failureThreshold: 3, baseCooldownMs: 6_000_000_000, maxCooldownMs: 6_000_000_000 },
    watchdog: { maxRedispatchDelayMs: 6_000_000_000, outageToleranceMs: 8_000_000_000 },
  })
  await h.request()
  await h.fail()
  await h.tick(2_147_483_647)
  assert.equal(h.prompts.length, 0)
  await h.tick(852_516_353)
  assert.equal(h.prompts.length, 1)
})

test("transient API admission failures back off without opening the provider circuit", async (t) => {
  const h = await harness(t)
  h.setPrompt(async () => { throw new Error("fetch failed") })
  await h.request()
  await h.fail()
  await h.tick(500)
  await h.tick(1_000)
  await h.tick(1_000)
  assert.equal(h.prompts.length, 3)
  assert.equal((await h.retry("another")).retry, true)
})

test("event stream failure reconnects instead of disabling the watchdog", async (t) => {
  const h = await harness(t)
  h.streams[0].fail(new Error("connection reset"))
  await setImmediate()
  await h.tick(1_000)
  assert.equal(h.streams.length, 2)
  await h.request()
  await h.fail()
  await h.tick(500)
  assert.equal(h.prompts.length, 1)
})

test("unattributed transient failures are not pooled into an unknown provider", async (t) => {
  const h = await harness(t)
  await h.fail()
  await h.tick(20_000)
  assert.equal(h.prompts.length, 0)
})

test("compaction failures are attributed without a retry hook", async (t) => {
  const h = await harness(t, {
    circuit: { failureThreshold: 1, baseCooldownMs: 1_000, maxCooldownMs: 8_000 },
    watchdog: { enabled: false },
  })
  await h.request("s1", "primary")
  await h.request("s1", "compaction-provider", "compaction")
  await h.fail()
  assert.deepEqual(await h.retry("another", "compaction-provider"), { retry: false })
  assert.equal((await h.retry("another-primary", "primary")).retry, true)
})

test("redispatch follows a new failing provider and releases the old probe", async (t) => {
  const h = await harness(t, { circuit: { failureThreshold: 1, baseCooldownMs: 1_000, maxCooldownMs: 8_000 } })
  await h.request()
  await h.fail()
  await h.tick(500)
  await h.tick(500)
  await h.request("s1", "p2", "compaction")
  await h.fail()
  assert.equal((await h.retry("another", "p1")).retry, true, "p1 probe reservation was released")
  await h.tick(1_000)
  assert.equal(h.prompts.length, 2, "next dispatch follows p2's shorter cooldown")
})

test("successful compaction resets its own provider circuit", async (t) => {
  const h = await harness(t, {
    circuit: { failureThreshold: 1, baseCooldownMs: 1_000, maxCooldownMs: 8_000 },
    watchdog: { enabled: false },
  })
  await h.request("s1", "compaction-provider", "compaction")
  await h.fail()
  await h.emit("session.compaction.ended", "s2", { model: { providerID: "compaction-provider", id: "model" } })
  assert.equal((await h.retry("another", "compaction-provider")).retry, true)
})
