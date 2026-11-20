import { DurableObject } from 'cloudflare:workers';

const JSON_HEADERS = { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' };
const ADDRESS_RE = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

function json(data, status = 200, extra = {}) {
  return new Response(JSON.stringify(data), { status, headers: { ...JSON_HEADERS, ...extra } });
}
function corsHeaders(request, env) {
  const origin = request.headers.get('Origin');
  const allowed = (env.ALLOWED_ORIGINS || '').split(',').map(x => x.trim()).filter(Boolean);
  if (!origin) return {};
  if (allowed.length && !allowed.includes(origin)) return {};
  return { 'access-control-allow-origin': origin, 'access-control-allow-methods': 'GET,POST,PUT,OPTIONS', 'access-control-allow-headers': 'content-type,authorization', 'vary': 'Origin' };
}
function authorized(request, env) {
  const expected = env.ADMIN_TOKEN;
  const supplied = (request.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
  return Boolean(expected && supplied && supplied === expected);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = corsHeaders(request, env);
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });

    if (url.pathname === '/api/health') {
      const id = env.PUMP_RADAR_HUB.idFromName('main');
      return env.PUMP_RADAR_HUB.get(id).fetch('https://hub.internal/health');
    }

    if (url.pathname === '/api/ws') {
      if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') return new Response('WebSocket upgrade required', { status: 426 });
      const id = env.PUMP_RADAR_HUB.idFromName('main');
      return env.PUMP_RADAR_HUB.get(id).fetch('https://hub.internal/client', request);
    }

    if (url.pathname.startsWith('/api/admin/')) {
      if (!authorized(request, env)) return json({ error: 'Unauthorized' }, 401, cors);
      if (!['GET', 'PUT'].includes(request.method)) return json({ error: 'Method not allowed' }, 405, cors);
      if (url.pathname !== '/api/admin/watchlists') return json({ error: 'Not found' }, 404, cors);
      const id = env.PUMP_RADAR_HUB.idFromName('main');
      const init = request.method === 'PUT' ? { method: 'PUT', headers: { 'content-type': 'application/json' }, body: await request.text() } : { method: 'GET' };
      const result = await env.PUMP_RADAR_HUB.get(id).fetch('https://hub.internal/watchlists', init);
      return new Response(result.body, { status: result.status, headers: { ...JSON_HEADERS, ...cors } });
    }

    return env.ASSETS.fetch(request);
  }
};

export class PumpRadarHub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
    this.upstream = null;
    this.retryMs = 2000;
    this.connecting = false;
    this.lastUpstreamMessageAt = null;
    this.upstreamState = 'starting';
  }

  async getLists() {
    return (await this.ctx.storage.get('watchlists')) || { mints: [], wallets: [] };
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/health') {
      const lists = await this.getLists();
      return json({ ok: true, upstream: this.upstreamState, lastMessageAt: this.lastUpstreamMessageAt, watchedMints: lists.mints.length, watchedWallets: lists.wallets.length });
    }
    if (url.pathname === '/watchlists') {
      if (request.method === 'GET') return json(await this.getLists());
      if (request.method === 'PUT') {
        let input;
        try { input = await request.json(); } catch { return json({ error: 'Invalid JSON' }, 400); }
        const mints = Array.isArray(input.mints) ? [...new Set(input.mints.map(String).map(x => x.trim()).filter(x => ADDRESS_RE.test(x)))].slice(0, 100) : [];
        const wallets = Array.isArray(input.wallets) ? [...new Set(input.wallets.map(String).map(x => x.trim()).filter(x => ADDRESS_RE.test(x)))].slice(0, 100) : [];
        if ((input.mints || []).length !== mints.length || (input.wallets || []).length !== wallets.length) return json({ error: 'One or more addresses are invalid. Use Solana base58 public addresses.' }, 400);
        const previous = await this.getLists();
        const next = { mints, wallets };
        await this.ctx.storage.put('watchlists', next);
        await this.applySubscriptions(previous, next);
        return json({ ok: true, watchlists: next });
      }
    }
    if (url.pathname === '/client') {
      if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') return new Response('WebSocket upgrade required', { status: 426 });
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server);
      server.serializeAttachment({ connectedAt: Date.now() });
      const lists = await this.getLists();
      server.send(JSON.stringify({ type: 'status', upstream: this.upstreamState, watchlists: { mints: lists.mints.length, wallets: lists.wallets.length } }));
      await this.ensureUpstream();
      return new Response(null, { status: 101, webSocket: client });
    }
    return new Response('Not found', { status: 404 });
  }

  async ensureUpstream() {
    if (this.connecting || (this.upstream && this.upstream.readyState <= WebSocket.OPEN)) return;
    if (!this.env.PUMPPORTAL_API_KEY) { this.upstreamState = 'missing_api_key'; this.broadcast({ type: 'status', upstream: this.upstreamState }); return; }
    this.connecting = true;
    this.upstreamState = 'connecting';
    this.broadcast({ type: 'status', upstream: this.upstreamState });
    try {
      const endpoint = `wss://pumpportal.fun/api/data?api-key=${encodeURIComponent(this.env.PUMPPORTAL_API_KEY)}`;
      const ws = new WebSocket(endpoint);
      this.upstream = ws;
      ws.addEventListener('open', async () => {
        this.retryMs = 2000;
        this.connecting = false;
        this.upstreamState = 'live';
        ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
        ws.send(JSON.stringify({ method: 'subscribeMigration' }));
        const lists = await this.getLists();
        if (lists.mints.length) this.sendKeys(ws, 'subscribeTokenTrade', lists.mints);
        if (lists.wallets.length) this.sendKeys(ws, 'subscribeAccountTrade', lists.wallets);
        this.broadcast({ type: 'status', upstream: this.upstreamState, watchlists: { mints: lists.mints.length, wallets: lists.wallets.length } });
      });
      ws.addEventListener('message', event => {
        this.lastUpstreamMessageAt = Date.now();
        let data;
        try { data = JSON.parse(typeof event.data === 'string' ? event.data : ''); } catch { return; }
        if (!data || typeof data !== 'object') return;
        const type = this.classify(data);
        this.broadcast({ type, receivedAt: Date.now(), data });
      });
      ws.addEventListener('error', () => {
        this.upstreamState = 'error';
        this.broadcast({ type: 'status', upstream: this.upstreamState });
      });
      ws.addEventListener('close', () => {
        this.connecting = false;
        this.upstream = null;
        this.upstreamState = 'reconnecting';
        this.broadcast({ type: 'status', upstream: this.upstreamState });
        this.ctx.storage.setAlarm(Date.now() + this.retryMs);
        this.retryMs = Math.min(this.retryMs * 2, 30000);
      });
      await this.ctx.storage.setAlarm(Date.now() + 10 * 60 * 1000);
    } catch (error) {
      this.connecting = false;
      this.upstreamState = 'error';
      this.broadcast({ type: 'status', upstream: this.upstreamState, message: String(error).slice(0, 180) });
      await this.ctx.storage.setAlarm(Date.now() + this.retryMs);
      this.retryMs = Math.min(this.retryMs * 2, 30000);
    }
  }

  classify(data) {
  const txType = String(data.txType || '').toLowerCase();

  if (txType === 'create' || (data.name && data.symbol && data.mint && !txType)) {
    return 'launch';
  }

  if (txType === 'migrate' || (data.pool && data.mint && data.migration)) {
    return 'migration';
  }

  if (txType === 'buy') return 'buy';
  if (txType === 'sell') return 'sell';

  return 'event';
  }

  sendKeys(ws, method, keys) {
    for (let i = 0; i < keys.length; i += 5000) ws.send(JSON.stringify({ method, keys: keys.slice(i, i + 5000) }));
  }

  async applySubscriptions(previous, next) {
    const ws = this.upstream;
    if (!ws || ws.readyState !== WebSocket.OPEN) { await this.ensureUpstream(); return; }
    const removedMints = previous.mints.filter(x => !next.mints.includes(x));
    const addedMints = next.mints.filter(x => !previous.mints.includes(x));
    const removedWallets = previous.wallets.filter(x => !next.wallets.includes(x));
    const addedWallets = next.wallets.filter(x => !previous.wallets.includes(x));
    if (removedMints.length) this.sendKeys(ws, 'unsubscribeTokenTrade', removedMints);
    if (addedMints.length) this.sendKeys(ws, 'subscribeTokenTrade', addedMints);
    if (removedWallets.length) this.sendKeys(ws, 'unsubscribeAccountTrade', removedWallets);
    if (addedWallets.length) this.sendKeys(ws, 'subscribeAccountTrade', addedWallets);
    this.broadcast({ type: 'status', upstream: this.upstreamState, watchlists: { mints: next.mints.length, wallets: next.wallets.length } });
  }

  broadcast(payload) {
    const message = JSON.stringify(payload);
    for (const client of this.ctx.getWebSockets()) {
      try { client.send(message); } catch { try { client.close(1011, 'send failed'); } catch {} }
    }
  }

  async alarm() {
    if (!this.upstream || this.upstream.readyState > WebSocket.OPEN) await this.ensureUpstream();
    await this.ctx.storage.setAlarm(Date.now() + 10 * 60 * 1000);
  }

  async webSocketMessage(ws, message) {
    // Dashboard sockets are read-only. Configuration changes require the admin-token HTTP API.
    if (String(message) === 'ping') ws.send(JSON.stringify({ type: 'pong', at: Date.now() }));
  }

  async webSocketClose(ws, code, reason) {
    if (code >= 1000 && code < 5000 && ![1004, 1005, 1006, 1015].includes(code)) ws.close(code, reason);
    else ws.close();
  }
}
