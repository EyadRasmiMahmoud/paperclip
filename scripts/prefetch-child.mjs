// Background downloads for verification scripts: start a child early with its
// output in a log file, join it where the result is needed, and stop it before
// the scratch directory it writes into is removed. No shell, no inherited
// terminal, and a failure is reported where the caller awaits it.
//
// A child may start processes of its own: the bundled-dependency installs are
// a Node wrapper blocked in `npm install`. Signalling only the wrapper would
// leave npm writing into the scratch root while it is removed, so on POSIX
// each child leads its own process group, and stopping signals and waits for
// the whole group. The parent's own terminal signals are forwarded to the
// groups while any child runs, so Ctrl-C still stops everything.
import { spawn } from 'node:child_process';
import { closeSync, openSync, readFileSync, statSync } from 'node:fs';

const GROUPS = process.platform !== 'win32';
const FORWARDED_SIGNALS = ['SIGINT', 'SIGTERM', 'SIGHUP'];
const running = child => child.pid !== undefined && child.exitCode === null && child.signalCode === null;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// Signals the child's whole process group (POSIX) or the child alone.
const signalTree = (child, signal) => {
  if (child.pid === undefined) return;
  if (!GROUPS) { if (running(child)) child.kill(signal); return; }
  try { process.kill(-child.pid, signal); } catch (error) { if (error.code !== 'ESRCH') throw error; }
};
// True while any member of the child's group exists. On a platform without
// groups only the child itself is known.
const treeAlive = child => {
  if (child.pid === undefined) return false;
  if (!GROUPS) return running(child);
  try { process.kill(-child.pid, 0); return true; } catch (error) { return error.code !== 'ESRCH'; }
};

// Terminal signals reach the parent's group only. While a prefetch runs, the
// same signal is passed on to its group and the parent then exits the way it
// would have without the handler.
const live = new Set();
const forward = signal => {
  for (const child of live) signalTree(child, signal);
  for (const name of FORWARDED_SIGNALS) process.removeListener(name, forward);
  process.kill(process.pid, signal);
};
const track = child => {
  if (!GROUPS) return;
  if (live.size === 0) for (const name of FORWARDED_SIGNALS) process.on(name, forward);
  live.add(child);
};
const untrack = child => {
  live.delete(child);
  if (live.size === 0) for (const name of FORWARDED_SIGNALS) process.removeListener(name, forward);
};

// Returns { name, child, done, durationMs }. `done` resolves on exit code 0
// and otherwise rejects with the tail of the log; the rejection is observed
// here so an unawaited failure never surfaces as an unhandled rejection.
export function startPrefetch({ name, cmd, args, cwd, env, logPath }) {
  const startedAt = Date.now();
  const log = openSync(logPath, 'w');
  let child;
  try {
    child = spawn(cmd, args, { cwd, env, stdio: ['ignore', log, log], detached: GROUPS });
  } finally {
    closeSync(log); // the child holds its own descriptor
  }
  if (child.pid !== undefined) track(child);
  const handle = { name, child, done: null, durationMs: null };
  let settled = false;
  handle.done = new Promise((settle, reject) => {
    const finish = failure => {
      if (settled) return;
      settled = true;
      untrack(child);
      // The caller may be blocked in synchronous work when the child exits, so
      // this event can arrive long after the fact. When the child wrote
      // anything, its last write is the end time; both downloads this serves
      // print their final status line as they complete. A silent child falls
      // back to the time the exit was observed.
      let finishedAt = Date.now();
      try {
        const { mtimeMs, size } = statSync(logPath);
        if (size > 0) finishedAt = Math.min(finishedAt, mtimeMs);
      } catch {}
      handle.durationMs = Math.max(0, Math.round(finishedAt - startedAt));
      if (failure === null) return settle();
      let output = '';
      try { output = readFileSync(logPath, 'utf8').trim().slice(-2000); } catch {}
      reject(new Error(`${name} prefetch failed (${failure}): ${output}`));
    };
    child.once('error', error => finish(error.message));
    child.once('exit', (code, signal) => finish(code === 0 ? null : (signal ?? `exit ${code}`)));
  });
  handle.done.catch(() => {});
  return handle;
}

// Stops every prefetch that is still running, together with any process it
// started, and resolves only once all of them have exited, so the caller can
// remove their working directory without racing a write. A tree that ignores
// SIGTERM is killed after `graceMs`; one that still exists `graceMs` after
// that is given up on so cleanup cannot hang.
export async function stopPrefetches(prefetches, { graceMs = 5000 } = {}) {
  await Promise.all(prefetches.map(async ({ child, done }) => {
    const started = Date.now();
    let termed = false, killed = false;
    while (treeAlive(child)) {
      const elapsed = Date.now() - started;
      if (!termed) { signalTree(child, 'SIGTERM'); termed = true; }
      else if (!killed && elapsed >= graceMs) { signalTree(child, 'SIGKILL'); killed = true; }
      else if (killed && elapsed >= 2 * graceMs) break;
      await sleep(25);
    }
    try { await done; } catch {}
  }));
}
