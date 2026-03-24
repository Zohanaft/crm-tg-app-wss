const uWS = require('uWebSockets.js');

const PORT = Number(process.env.PORT || 3002);
const BACKEND_INTERNAL_URL = (process.env.BACKEND_INTERNAL_URL || 'http://crm-tg-app-backend:3000').replace(/\/$/, '');
const WSS_SHARED_SECRET = process.env.WSS_SHARED_SECRET || '';
const MAX_BODY_SIZE = 1024 * 1024;

const app = uWS.App();
const roomConnections = new Map();

function roomName(workspaceId) {
  return `workspace:${workspaceId}`;
}

function safeJsonParse(input) {
  try {
    return JSON.parse(input);
  } catch {
    return null;
  }
}

function sendJson(res, status, payload) {
  res.writeStatus(status);
  res.writeHeader('content-type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(payload));
}

function collectJsonBody(res, done) {
  let aborted = false;
  let body = '';
  res.onAborted(() => {
    aborted = true;
  });
  res.onData((chunk, isLast) => {
    if (aborted) return;
    body += Buffer.from(chunk).toString('utf8');
    if (body.length > MAX_BODY_SIZE) {
      sendJson(res, '413 Payload Too Large', { ok: false });
      return;
    }
    if (!isLast) return;
    const parsed = body ? safeJsonParse(body) : {};
    if (parsed === null) {
      sendJson(res, '400 Bad Request', { ok: false, error: 'Invalid JSON' });
      return;
    }
    done(parsed);
  });
}

async function authorizeToken(accessToken) {
  if (!accessToken) return null;
  const response = await fetch(`${BACKEND_INTERNAL_URL}/workspace/wss/auth`, {
    method: 'GET',
    headers: {
      authorization: `Bearer ${accessToken}`,
    },
  });
  if (!response.ok) return null;
  return response.json();
}

function touchRoom(workspaceId, socketId) {
  const room = roomName(workspaceId);
  if (!roomConnections.has(room)) {
    roomConnections.set(room, new Set());
  }
  roomConnections.get(room).add(socketId);
  return room;
}

function releaseRoom(room, socketId) {
  if (!roomConnections.has(room)) return;
  const sockets = roomConnections.get(room);
  sockets.delete(socketId);
  if (sockets.size === 0) {
    roomConnections.delete(room);
  }
}

app.ws('/api/wss', {
  idleTimeout: 32,
  maxPayloadLength: 16 * 1024,
  compression: uWS.DISABLED,
  open: async (ws) => {
    ws.userData.rooms = new Set();
    const auth = await authorizeToken(ws.userData.token);
    if (!auth || !Array.isArray(auth.workspaceIds)) {
      ws.end(4001, 'unauthorized');
      return;
    }

    const workspaceIds = ws.userData.workspaceId
      ? auth.workspaceIds.filter((id) => id === ws.userData.workspaceId)
      : auth.workspaceIds;

    workspaceIds.forEach((workspaceId) => {
      const room = touchRoom(workspaceId, ws.userData.socketId);
      ws.subscribe(room);
      ws.userData.rooms.add(room);
    });

    ws.send(
      JSON.stringify({
        type: 'wss:ready',
        workspaceIds,
      }),
    );
  },
  message: (ws, message) => {
    const text = Buffer.from(message).toString('utf8');
    const data = safeJsonParse(text);
    if (!data || typeof data !== 'object') return;

    if (data.type === 'workspace:join' && typeof data.workspaceId === 'string') {
      const room = touchRoom(data.workspaceId, ws.userData.socketId);
      ws.subscribe(room);
      ws.userData.rooms.add(room);
      ws.send(JSON.stringify({ type: 'workspace:joined', workspaceId: data.workspaceId }));
      return;
    }

    if (data.type === 'workspace:leave' && typeof data.workspaceId === 'string') {
      const room = roomName(data.workspaceId);
      ws.unsubscribe(room);
      ws.userData.rooms.delete(room);
      releaseRoom(room, ws.userData.socketId);
      ws.send(JSON.stringify({ type: 'workspace:left', workspaceId: data.workspaceId }));
    }
  },
  close: (ws) => {
    if (!ws.userData.rooms) return;
    ws.userData.rooms.forEach((room) => {
      releaseRoom(room, ws.userData.socketId);
    });
  },
  upgrade: (res, req, context) => {
    const query = new URLSearchParams(req.getQuery() || '');
    const token = query.get('token') || '';
    const workspaceId = query.get('workspaceId') || '';
    const socketId = `${Date.now()}-${Math.random().toString(36).slice(2)}`;

    res.upgrade(
      {
        token,
        workspaceId,
        socketId,
      },
      req.getHeader('sec-websocket-key'),
      req.getHeader('sec-websocket-protocol'),
      req.getHeader('sec-websocket-extensions'),
      context,
    );
  },
});

app.post('/internal/events/client-start', (res, req) => {
  const secret = req.getHeader('x-wss-shared-secret');
  if (WSS_SHARED_SECRET && secret !== WSS_SHARED_SECRET) {
    sendJson(res, '401 Unauthorized', { ok: false });
    return;
  }

  collectJsonBody(res, (payload) => {
    const workspaceIds = Array.isArray(payload.workspaceIds) ? payload.workspaceIds : [];
    workspaceIds.forEach((workspaceId) => {
      const room = roomName(workspaceId);
      if (!roomConnections.has(room)) return;
      app.publish(
        room,
        JSON.stringify({
          type: 'client:start',
          payload,
        }),
      );
    });
    sendJson(res, '200 OK', { ok: true });
  });
});

app.post('/api/wss/telegram/webhook/:secret', (res, req) => {
  const secret = req.getParameter(0);
  collectJsonBody(res, async (payload) => {
    try {
      const response = await fetch(`${BACKEND_INTERNAL_URL}/telegram/webhook/${encodeURIComponent(secret)}`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
        },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        sendJson(res, '502 Bad Gateway', { ok: false });
        return;
      }
      const data = await response.json();
      sendJson(res, '200 OK', data);
    } catch {
      sendJson(res, '502 Bad Gateway', { ok: false });
    }
  });
});

app.get('/healthz', (res) => {
  sendJson(res, '200 OK', { ok: true, rooms: roomConnections.size });
});

app.listen('0.0.0.0', PORT, (token) => {
  if (token) {
    // eslint-disable-next-line no-console
    console.log(`[wss] listening on ${PORT}`);
    return;
  }
  // eslint-disable-next-line no-console
  console.error('[wss] failed to listen');
});
