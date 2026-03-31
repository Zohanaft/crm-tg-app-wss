const uWS = require('uWebSockets.js');

const PORT = Number(process.env.PORT || 3002);
const BACKEND_INTERNAL_URL = (process.env.BACKEND_INTERNAL_URL || 'http://crm-tg-app-backend:3000').replace(/\/$/, '');
const WSS_SHARED_SECRET = process.env.WSS_SHARED_SECRET || '';
const MAX_BODY_SIZE = 1024 * 1024;

const app = uWS.App();
const roomConnections = new Map();

/** Room lifecycle logs (disable: WSS_ROOM_LOGS=0). Prefix wss-room — grep-friendly on Windows. */
const ROOM_LOGS = process.env.WSS_ROOM_LOGS !== '0';

function roomLog(msg, extra) {
  if (!ROOM_LOGS) return;
  if (extra !== undefined) {
    // eslint-disable-next-line no-console
    console.log(`[wss-room] ${msg}`, extra);
  } else {
    // eslint-disable-next-line no-console
    console.log(`[wss-room] ${msg}`);
  }
}

/** Always logged: handshake / open / auth (so you see traffic even if room trace is off). */
function wssTrace(msg, extra) {
  if (extra !== undefined) {
    // eslint-disable-next-line no-console
    console.log(`[wss] ${msg}`, extra);
  } else {
    // eslint-disable-next-line no-console
    console.log(`[wss] ${msg}`);
  }
}

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

async function authorizeWithCookie(accessCookie) {
  if (!accessCookie) return null;
  const response = await fetch(`${BACKEND_INTERNAL_URL}/workspace/wss/auth`, {
    method: 'GET',
    headers: {
      // Cookies are HttpOnly on the main domain; browser sends them automatically during WS handshake.
      // We forward them to the backend so JwtStrategy can read `req.cookies.access_token`.
      cookie: accessCookie,
    },
  });
  if (!response.ok) return null;
  return response.json();
}

function touchRoom(workspaceId, socketId) {
  const room = roomName(workspaceId);
  const isNewRoom = !roomConnections.has(room);
  if (isNewRoom) {
    roomConnections.set(room, new Set());
    roomLog('room created (first socket)', { room, workspaceId, socketId });
  }
  roomConnections.get(room).add(socketId);
  const members = roomConnections.get(room).size;
  roomLog('socket joined room', { room, socketId, membersInRoom: members });
  return room;
}

function releaseRoom(room, socketId) {
  if (!roomConnections.has(room)) return;
  const sockets = roomConnections.get(room);
  sockets.delete(socketId);
  const left = sockets.size;
  roomLog('socket left room', { room, socketId, membersInRoom: left });
  if (left === 0) {
    roomConnections.delete(room);
    roomLog('room removed (empty)', { room });
  }
}

app.ws('/api/wss', {
  idleTimeout: 32,
  maxPayloadLength: 16 * 1024,
  compression: uWS.DISABLED,
  open: async (ws) => {
    const ud = ws.getUserData();
    wssTrace('open start', { socketId: ud.socketId });
    ud.rooms = new Set();
    const auth = await authorizeWithCookie(ud.cookie);
    if (!auth || !Array.isArray(auth.workspaceIds)) {
      wssTrace('open auth failed (unauthorized)', { socketId: ud.socketId });
      ws.end(4001, 'unauthorized');
      return;
    }

    ud.allowedWorkspaceIds = new Set(auth.workspaceIds);
    const workspaceIds = ud.workspaceId
      ? auth.workspaceIds.filter((id) => id === ud.workspaceId)
      : auth.workspaceIds;

    if (ud.workspaceId && !ud.allowedWorkspaceIds.has(ud.workspaceId)) {
      wssTrace('open workspace_forbidden', {
        socketId: ud.socketId,
        workspaceId: ud.workspaceId,
      });
      ws.end(4002, 'workspace_forbidden');
      return;
    }

    wssTrace('open auth ok', { socketId: ud.socketId, workspaceCount: workspaceIds.length });
    roomLog('open subscribe workspaces', {
      socketId: ud.socketId,
      filterWorkspaceId: ud.workspaceId || null,
      workspaceIds,
      count: workspaceIds.length,
    });

    workspaceIds.forEach((workspaceId) => {
      const room = touchRoom(workspaceId, ud.socketId);
      ws.subscribe(room);
      ud.rooms.add(room);
    });

    ws.send(
      JSON.stringify({
        type: 'wss:ready',
        workspaceIds,
      }),
    );
  },
  message: (ws, message) => {
    const ud = ws.getUserData();
    const text = Buffer.from(message).toString('utf8');
    const data = safeJsonParse(text);
    if (!data || typeof data !== 'object') return;

    if (data.type === 'workspace:join' && typeof data.workspaceId === 'string') {
      if (ud.allowedWorkspaceIds && !ud.allowedWorkspaceIds.has(data.workspaceId)) {
        roomLog('workspace:join denied', {
          socketId: ud.socketId,
          workspaceId: data.workspaceId,
        });
        ws.end(4401, 'workspace_forbidden');
        return;
      }
      roomLog('msg workspace:join', { socketId: ud.socketId, workspaceId: data.workspaceId });
      const room = touchRoom(data.workspaceId, ud.socketId);
      ws.subscribe(room);
      ud.rooms.add(room);
      ws.send(JSON.stringify({ type: 'workspace:joined', workspaceId: data.workspaceId }));
      return;
    }

    if (data.type === 'workspace:leave' && typeof data.workspaceId === 'string') {
      const room = roomName(data.workspaceId);
      roomLog('msg workspace:leave', { socketId: ud.socketId, workspaceId: data.workspaceId });
      ws.unsubscribe(room);
      ud.rooms.delete(room);
      releaseRoom(room, ud.socketId);
      ws.send(JSON.stringify({ type: 'workspace:left', workspaceId: data.workspaceId }));
    }
  },
  close: (ws) => {
    const ud = ws.getUserData();
    if (!ud.rooms) return;
    roomLog('ws close cleanup', {
      socketId: ud.socketId,
      rooms: ud.rooms.size,
    });
    ud.rooms.forEach((room) => {
      releaseRoom(room, ud.socketId);
    });
  },
  upgrade: (res, req, context) => {
    const query = new URLSearchParams(req.getQuery() || '');
    const workspaceId = query.get('workspaceId') || '';
    const socketId = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const cookie = req.getHeader('cookie') || '';

    wssTrace('upgrade', {
      socketId,
      workspaceId: workspaceId || null,
      hasCookie: Boolean(cookie && cookie.length > 0),
    });

    res.upgrade(
      {
        cookie,
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

function internalEventsAuth(res, req) {
  const secret = req.getHeader('x-wss-shared-secret');
  if (WSS_SHARED_SECRET && secret !== WSS_SHARED_SECRET) {
    sendJson(res, '401 Unauthorized', { ok: false });
    return false;
  }
  return true;
}

app.post('/internal/events/action-created', (res, req) => {
  if (!internalEventsAuth(res, req)) return;

  collectJsonBody(res, (payload) => {
    const workspaceIds = Array.isArray(payload.workspaceIds) ? payload.workspaceIds : [];
    const action = payload.action && typeof payload.action === 'object' ? payload.action : {};
    const ts = new Date().toISOString();
    workspaceIds.forEach((workspaceId) => {
      const room = roomName(workspaceId);
      if (!roomConnections.has(room)) return;
      app.publish(
        room,
        JSON.stringify({
          type: 'action:created',
          ts,
          payload: { action },
        }),
      );
    });
    sendJson(res, '200 OK', { ok: true });
  });
});

app.post('/internal/events/workspace-member-joined', (res, req) => {
  if (!internalEventsAuth(res, req)) return;

  collectJsonBody(res, (payload) => {
    const workspaceId = typeof payload.workspaceId === 'string' ? payload.workspaceId : '';
    if (!workspaceId) {
      sendJson(res, '400 Bad Request', { ok: false, error: 'workspaceId required' });
      return;
    }
    const room = roomName(workspaceId);
    if (roomConnections.has(room)) {
      const ts = new Date().toISOString();
      app.publish(
        room,
        JSON.stringify({
          type: 'workspace:member_joined',
          ts,
          payload,
        }),
      );
    }
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
    // eslint-disable-next-line no-console
    console.log('[wss-room] config', {
      roomTraceEnabled: ROOM_LOGS,
      WSS_ROOM_LOGS: process.env.WSS_ROOM_LOGS ?? '(unset)',
      hint: 'grep: wss-room OR [wss] upgrade|open',
    });
    return;
  }
  // eslint-disable-next-line no-console
  console.error('[wss] failed to listen');
});
