"use strict";

import { GeneratorResponse, ProtocolIo, RunOpts } from "./types.js";
import { AsyncLock } from "@tursodatabase/database-common";

interface TrackPromise<T> {
    promise: Promise<T>,
    finished: boolean
}

function trackPromise<T>(p: Promise<T>): TrackPromise<T> {
    let status: any = { promise: null, finished: false };
    status.promise = p.finally(() => status.finished = true);
    return status;
}

function timeoutMs(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
}

function normalizeUrl(url: string): string {
    return url.replace(/^(libsql|turso):\/\//, 'https://');
}

async function process(opts: RunOpts, io: ProtocolIo, request: any) {
    const requestType = request.request();
    const completion = request.completion();
    if (requestType.type == 'Http') {
        let url: string | null = requestType.url;
        if (typeof opts.url == "function") {
            url = opts.url();
        } else {
            url = opts.url;
        }
        if (url == null) {
            completion.poison(`url is empty - sync is paused`);
            return;
        }
        url = normalizeUrl(url);
        try {
            let headers = typeof opts.headers === "function" ? await opts.headers() : opts.headers;
            if (requestType.headers != null && requestType.headers.length > 0) {
                headers = { ...headers };
                for (let header of requestType.headers) {
                    headers[header[0]] = header[1];
                }
            }
            const fetchImpl = opts.fetch ?? fetch;
            const response = await fetchImpl(`${url}${requestType.path}`, {
                method: requestType.method,
                headers: headers,
                body: requestType.body ?? null,
            });
            completion.status(response.status);
            const reader = response.body?.getReader();
            if (reader == null) {
                throw new Error("reader is null");
            }
            while (true) {
                const { done, value } = await reader.read();
                if (done) {
                    completion.done();
                    break;
                }
                completion.pushBuffer(value);
            }
        } catch (error) {
            completion.poison(`fetch error: ${error}`);
        }
    } else if (requestType.type == 'FullRead') {
        try {
            const metadata = await io.read(requestType.path);
            if (metadata != null) {
                completion.pushBuffer(metadata);
            }
            completion.done();
        } catch (error) {
            completion.poison(`metadata read error: ${error}`);
        }
    } else if (requestType.type == 'FullWrite') {
        try {
            await io.write(requestType.path, requestType.content);
            completion.done();
        } catch (error) {
            completion.poison(`metadata write error: ${error}`);
        }
    } else if (requestType.type == 'Transform') {
        if (opts.transform == null) {
            completion.poison("transform is not set");
            return;
        }
        const results = [];
        for (const mutation of requestType.mutations) {
            const result = opts.transform(mutation);
            if (result == null) {
                results.push({ type: 'Keep' });
            } else if (result.operation == 'skip') {
                results.push({ type: 'Skip' });
            } else if (result.operation == 'rewrite') {
                results.push({ type: 'Rewrite', stmt: result.stmt });
            } else {
                completion.poison("unexpected transform operation");
                return;
            }
        }
        completion.pushTransform(results);
        completion.done();
    }
}

/**
 * Configuration for {@link retryFetch}.
 */
export interface RetryFetchOpts {
    /** total number of attempts (including the first try). must be >= 1. default: 3 */
    attempts?: number;
    /** delay before the second attempt, in milliseconds. default: 500 */
    delayMs?: number;
    /** multiplier applied to {@link delayMs} after each failed attempt. default: 2 */
    backoff?: number;
    /** underlying fetch implementation to retry around. default: globalThis.fetch */
    fetch?: typeof fetch;
}

/**
 * Wraps a fetch implementation in a retry/backoff loop. Plug into
 * {@link DatabaseOpts.fetch} to give the sync engine resilient HTTP transport.
 *
 * Retries on:
 *   - thrown network errors (DNS, connection reset, AbortError, etc.)
 *   - 5xx server responses
 *   - 429 rate-limit responses
 *
 * Does NOT retry on:
 *   - 2xx, 3xx, or other 4xx responses (auth/bad-request — won't fix itself)
 *
 * Defaults: 3 attempts (initial + 2 retries), 500ms initial delay, 2x backoff
 * (so delays are 500ms, 1000ms between attempts).
 *
 * @example
 * ```ts
 * import { connect } from '@tursodatabase/sync';
 * import { retryFetch } from '@tursodatabase/sync-common';
 *
 * const db = await connect({
 *   path: 'local.db',
 *   url: 'libsql://...',
 *   fetch: retryFetch(),                              // defaults
 *   // fetch: retryFetch({ attempts: 5, delayMs: 1000 }),
 * });
 * ```
 */
export function retryFetch(opts: RetryFetchOpts = {}): typeof fetch {
    const attempts = opts.attempts ?? 3;
    const baseDelay = opts.delayMs ?? 500;
    const backoff = opts.backoff ?? 2;
    const underlying: typeof fetch = opts.fetch ?? ((input, init) => fetch(input, init));
    if (!Number.isFinite(attempts) || attempts < 1) {
        throw new Error(`retryFetch: attempts must be a finite integer >= 1, got ${attempts}`);
    }
    return async (input: RequestInfo | URL, init?: RequestInit) => {
        let lastError: unknown = null;
        let lastResponse: Response | null = null;
        let delay = baseDelay;
        for (let i = 0; i < attempts; i++) {
            try {
                const response = await underlying(input, init);
                if (response.status < 500 && response.status !== 429) {
                    return response;
                }
                lastResponse = response;
                lastError = null;
            } catch (error) {
                lastError = error;
                lastResponse = null;
            }
            if (i + 1 < attempts) {
                await timeoutMs(delay);
                delay *= backoff;
            }
        }
        if (lastResponse != null) {
            return lastResponse;
        }
        throw lastError ?? new Error('retryFetch: exhausted with no response');
    };
}

export function memoryIO(): ProtocolIo {
    let values = new Map();
    return {
        async read(path: string): Promise<Buffer | Uint8Array | null> {
            return values.get(path);
        },
        async write(path: string, data: Buffer | Uint8Array): Promise<void> {
            values.set(path, data);
        }
    }
};

export interface Runner {
    wait(): Promise<void>;
}

export function runner(opts: RunOpts, io: ProtocolIo, engine: any): Runner {
    let tasks: TrackPromise<any>[] = [];
    return {
        async wait() {
            for (let request = engine.protocolIo(); request != null; request = engine.protocolIo()) {
                tasks.push(trackPromise(process(opts, io, request)));
            }
            const tasksRace = tasks.length == 0 ? Promise.resolve() : Promise.race([timeoutMs(opts.preemptionMs), ...tasks.map(t => t.promise)]);
            await Promise.all([engine.ioLoopAsync(), tasksRace]);

            tasks = tasks.filter(t => !t.finished);

            engine.protocolIoStep();
        },
    }
}

export async function run(runner: Runner, generator: any, lock?: AsyncLock): Promise<any> {
    // When `lock` is provided, hold it while a native call is in flight:
    // `resumeAsync` and the io loop inside `runner.wait()` execute sync-engine
    // core work on the napi async worker, and on browser wasm the main thread
    // panics if one of its own native calls contends on a core lock meanwhile.
    // Callers pass the connection's exec lock so statement execution and
    // sync-engine steps interleave without ever overlapping. The lock is
    // released between steps, so waiting on a long-poll does not starve
    // main-thread statement execution.
    while (true) {
        if (lock != null) {
            await lock.acquire();
        }
        let response: GeneratorResponse;
        try {
            response = await generator.resumeAsync(null);
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
        const { type, ...rest } = response;
        if (type == 'Done') {
            return null;
        }
        if (type == 'SyncEngineStats') {
            return rest;
        }
        if (type == 'SyncEngineChanges') {
            //@ts-ignore
            return rest.changes;
        }
        if (lock != null) {
            await lock.acquire();
        }
        try {
            await runner.wait();
        } finally {
            if (lock != null) {
                lock.release();
            }
        }
    }
}

export class SyncEngineGuards {
    waitLock: AsyncLock;
    pushLock: AsyncLock;
    pullLock: AsyncLock;
    checkpointLock: AsyncLock;
    constructor() {
        this.waitLock = new AsyncLock();
        this.pushLock = new AsyncLock();
        this.pullLock = new AsyncLock();
        this.checkpointLock = new AsyncLock();
    }
    async wait(f: () => Promise<any>): Promise<any> {
        try {
            await this.waitLock.acquire();
            return await f();
        } finally {
            this.waitLock.release();
        }
    }
    async push(f: () => Promise<void>) {
        try {
            await this.pushLock.acquire();
            await this.pullLock.acquire();
            await this.checkpointLock.acquire();
            return await f();
        } finally {
            this.pushLock.release();
            this.pullLock.release();
            this.checkpointLock.release();
        }
    }
    async apply(f: () => Promise<void>) {
        try {
            await this.waitLock.acquire();
            await this.pushLock.acquire();
            await this.pullLock.acquire();
            await this.checkpointLock.acquire();
            return await f();
        } finally {
            this.waitLock.release();
            this.pushLock.release();
            this.pullLock.release();
            this.checkpointLock.release();
        }
    }
    async checkpoint(f: () => Promise<void>) {
        try {
            await this.waitLock.acquire();
            await this.pushLock.acquire();
            await this.pullLock.acquire();
            await this.checkpointLock.acquire();
            return await f();
        } finally {
            this.waitLock.release();
            this.pushLock.release();
            this.pullLock.release();
            this.checkpointLock.release();
        }
    }
}