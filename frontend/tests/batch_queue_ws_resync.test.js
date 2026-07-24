/**
 * Global WebSocket reconnect must restore both queue snapshots.
 *
 * A CPU-heavy backend stage can delay or drop progress events. When the
 * connection comes back, relying only on future WebSocket messages leaves the
 * batch UI with stale "pending" items until the whole project completes.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function extractFunction(src, name) {
  const start = src.indexOf(`function ${name}(`);
  if (start === -1) throw new Error(`Function not found: ${name}`);
  const braceStart = src.indexOf('{', start);
  let depth = 0;
  for (let i = braceStart; i < src.length; i += 1) {
    if (src[i] === '{') depth += 1;
    if (src[i] === '}') {
      depth -= 1;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  throw new Error(`Unbalanced function: ${name}`);
}

let context;

beforeEach(() => {
  const source = fs.readFileSync(
    path.join(__dirname, '..', 'static', 'js', 'app.js'),
    'utf8',
  );
  const connectGlobalWS = extractFunction(source, 'connectGlobalWS');
  const bootstrap = `
    let wsMode = null;
    let wsGlobal = null;
    const wsConnected = { value: false };
    const calls = [];
    const location = { protocol: 'https:', host: 'audit.example' };
    const closeProjectWS = () => calls.push('close-project');
    const closeGlobalWS = () => calls.push('close-global');
    const fetchPrepareQueue = () => calls.push('prepare');
    const refreshBatchQueue = () => calls.push('batch');
    const handleWSMessage = () => {};
    const setTimeout = () => {};
    const console = { error: () => {} };
    class WebSocket {
      static OPEN = 1;
      constructor(url) {
        this.url = url;
        this.readyState = 0;
        globalThis.socket = this;
      }
    }
    ${connectGlobalWS}
    globalThis.run = connectGlobalWS;
    globalThis.calls = calls;
    globalThis.wsConnected = wsConnected;
  `;
  context = vm.createContext({ globalThis: {} });
  vm.runInContext(bootstrap, context);
});

describe('connectGlobalWS queue recovery', () => {
  it('reloads prepare and batch queues when the socket opens', () => {
    context.globalThis.run();
    context.globalThis.socket.onopen();

    expect(context.globalThis.calls).toContain('prepare');
    expect(context.globalThis.calls).toContain('batch');
    expect(context.globalThis.wsConnected.value).toBe(true);
  });
});
