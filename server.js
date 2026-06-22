/**
 * 吹牛5骰 - 多人联机服务器 v2
 *   - SQLite 持久化（房间 / 玩家 / 战绩 / 皮肤）
 *   - 多人房间 2~6 人（2v2 团战 / 大乱斗淘汰）
 *   - 断线重连（token 机制，3 分钟窗口）
 *   - 皮肤同步
 *   - 无 Redis 依赖，纯 SQLite + 内存 Map
 *
 * 启动：node server.js [--port 8080]
 */
const http = require('http');
const path = require('path');
const fs   = require('fs');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const Database = require('better-sqlite3');

const PORT = parseInt(process.argv[process.argv.indexOf('--port') + 1]) || 8080;

// ══════════════════════════════════════════════════════════════
// SQLite 初始化
// ══════════════════════════════════════════════════════════════
const DB_PATH = path.join(__dirname, 'liars_dice.db');
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS players (
    id TEXT PRIMARY KEY,
    token TEXT UNIQUE NOT NULL,
    nickname TEXT NOT NULL DEFAULT '匿名玩家',
    skin TEXT NOT NULL DEFAULT 'default',
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    total_games INTEGER DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    last_seen INTEGER NOT NULL DEFAULT (unixepoch())
  );

  CREATE TABLE IF NOT EXISTS rooms (
    code TEXT PRIMARY KEY,
    mode TEXT NOT NULL DEFAULT 'normal',
    max_players INTEGER NOT NULL DEFAULT 2,
    phase TEXT NOT NULL DEFAULT 'waiting',
    config TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    last_activity INTEGER NOT NULL DEFAULT (unixepoch())
  );

  CREATE TABLE IF NOT EXISTS room_players (
    room_code TEXT NOT NULL REFERENCES rooms(code) ON DELETE CASCADE,
    player_id TEXT NOT NULL REFERENCES players(id),
    team INTEGER NOT NULL DEFAULT 0,
    alive INTEGER NOT NULL DEFAULT 1,
    joined_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (room_code, player_id)
  );

  CREATE TABLE IF NOT EXISTS reconnect_tokens (
    player_id TEXT PRIMARY KEY REFERENCES players(id),
    room_code TEXT NOT NULL,
    token TEXT UNIQUE NOT NULL,
    expires_at INTEGER NOT NULL,
    game_state TEXT
  );
`);

// 预编译语句
const stmts = {
  player_upsert: db.prepare(`INSERT INTO players (id, token, nickname, skin, last_seen) VALUES (?,?,?,?, unixepoch()) ON CONFLICT(id) DO UPDATE SET token=excluded.token, nickname=excluded.nickname, last_seen=unixepoch()`),
  player_update_stats: db.prepare(`UPDATE players SET wins=wins+?, losses=losses+?, total_games=total_games+?, last_seen=unixepoch() WHERE id=?`),
  player_get: db.prepare(`SELECT * FROM players WHERE id=?`),
  player_by_token: db.prepare(`SELECT * FROM players WHERE token=?`),
  room_insert: db.prepare(`INSERT OR IGNORE INTO rooms (code, mode, max_players, config, created_at, last_activity) VALUES (?,?,?,?,unixepoch(),unixepoch())`),
  room_update: db.prepare(`UPDATE rooms SET phase=?, last_activity=unixepoch() WHERE code=?`),
  room_delete: db.prepare(`DELETE FROM rooms WHERE code=?`),
  room_get: db.prepare(`SELECT * FROM rooms WHERE code=?`),
  room_player_insert: db.prepare(`INSERT OR IGNORE INTO room_players (room_code, player_id, team) VALUES (?,?,?)`),
  room_player_delete: db.prepare(`DELETE FROM room_players WHERE room_code=? AND player_id=?`),
  room_players_get: db.prepare(`SELECT p.*, rp.team, rp.alive FROM players p JOIN room_players rp ON p.id=rp.player_id WHERE rp.room_code=?`),
  room_player_count: db.prepare(`SELECT COUNT(*) as cnt FROM room_players WHERE room_code=?`),
  room_player_alive_count: db.prepare(`SELECT COUNT(*) as cnt FROM room_players WHERE room_code=? AND alive=1`),
  reconnect_upsert: db.prepare(`INSERT OR REPLACE INTO reconnect_tokens (player_id, room_code, token, expires_at, game_state) VALUES (?,?,?,?,?)`),
  reconnect_get: db.prepare(`SELECT * FROM reconnect_tokens WHERE token=?`),
  reconnect_delete: db.prepare(`DELETE FROM reconnect_tokens WHERE player_id=?`),
  cleanup_rooms: db.prepare(`DELETE FROM rooms WHERE last_activity < ? AND phase != 'playing'`),
  cleanup_reconnect: db.prepare(`DELETE FROM reconnect_tokens WHERE expires_at < unixepoch()`),
};

function generateId() { return crypto.randomBytes(12).toString('hex'); }
function generateToken() { return crypto.randomBytes(24).toString('base64url'); }
function now() { return Math.floor(Date.now() / 1000); }

// ══════════════════════════════════════════════════════════════
// 内存活跃状态（快速消息路由）
// ══════════════════════════════════════════════════════════════
// activeRooms: { code -> { players: Map<playerId, ws>, mode, maxPlayers, ... } }
const activeRooms = new Map();
// playerSockets: { ws -> playerId } 快速反向查找
const playerSocketMap = new WeakMap();

function getActiveRoom(code) { return activeRooms.get(code); }

function getActivePlayers(code) {
  const r = activeRooms.get(code);
  if (!r) return [];
  return [...r.players.entries()].map(([id, ws]) => ({ id, ws, alive: ws.readyState === 1 }));
}

// ══════════════════════════════════════════════════════════════
// 广播 & 定向消息
// ══════════════════════════════════════════════════════════════
function sendToRoom(code, msg, excludeWs) {
  const r = activeRooms.get(code);
  if (!r) return;
  for (const [, ws] of r.players) {
    if (ws !== excludeWs && ws.readyState === 1) ws.send(msg);
  }
}

// ══════════════════════════════════════════════════════════════
// 房间管理
// ══════════════════════════════════════════════════════════════
function createRoom(code, mode, maxPlayers, config) {
  stmts.room_insert.run(code, mode, maxPlayers, JSON.stringify(config || {}));
  activeRooms.set(code, {
    players: new Map(),
    teams: new Map(),
    mode,
    maxPlayers,
    config: config || {},
    phase: 'waiting',
    currentBid: { qty: 0, pip: 0, bidder: null },
  });
  return activeRooms.get(code);
}

function addPlayerToRoom(code, playerId, ws, team) {
  const room = activeRooms.get(code);
  if (!room) return false;
  room.players.set(playerId, ws);
  room.teams.set(playerId, team || 0);
  playerSocketMap.set(ws, playerId);
  stmts.room_player_insert.run(code, playerId, team || 0);
  return true;
}

function removePlayerFromRoom(code, playerId) {
  const room = activeRooms.get(code);
  if (!room) return;
  const ws = room.players.get(playerId);
  if (ws) playerSocketMap.delete(ws);
  room.players.delete(playerId);
  room.teams.delete(playerId);
  stmts.room_player_delete.run(code, playerId);
}

// ══════════════════════════════════════════════════════════════
// HTTP 服务
// ══════════════════════════════════════════════════════════════
const httpServer = http.createServer((req, res) => {
  let filePath = req.url === '/' ? '/liars_dice_1.html' : req.url;
  filePath = path.join(__dirname, filePath.split('?')[0]);

  if (!fs.existsSync(filePath)) { res.writeHead(404); res.end('Not found'); return; }

  const ext = path.extname(filePath);
  const mime = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml' };
  res.writeHead(200, { 'Content-Type': mime[ext] || 'text/plain' });
  fs.createReadStream(filePath).pipe(res);
});

// ══════════════════════════════════════════════════════════════
// WebSocket 服务
// ══════════════════════════════════════════════════════════════
const wss = new WebSocketServer({ server: httpServer, maxPayload: 8192 });
let totalConnections = 0;

wss.on('connection', (ws, req) => {
  totalConnections++;
  ws.playerId = null;
  ws.roomCode = null;
  ws.isAlive = true;
  ws.lastPong = now();

  ws.on('pong', () => { ws.isAlive = true; ws.lastPong = now(); });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ── RECONNECT: 断线重连 ──────────────────────────
    if (msg.type === 'RECONNECT') {
      const row = stmts.reconnect_get.get(msg.token);
      if (!row || row.expires_at < now()) {
        ws.send(JSON.stringify({ type: 'ERROR', reason: 'RECONNECT_FAILED', message: '重连令牌已过期' }));
        return;
      }
      // 恢复玩家状态
      ws.playerId = row.player_id;
      ws.roomCode = row.room_code;
      const room = activeRooms.get(row.room_code);
      if (!room) {
        ws.send(JSON.stringify({ type: 'ERROR', reason: 'ROOM_GONE', message: '房间已不存在' }));
        return;
      }
      // 替换旧连接
      const oldWs = room.players.get(row.player_id);
      if (oldWs && oldWs !== ws) {
        try { oldWs.close(); } catch(e) {}
        playerSocketMap.delete(oldWs);
      }
      room.players.set(row.player_id, ws);
      playerSocketMap.set(ws, row.player_id);

      const player = stmts.player_get.get(row.player_id);
      ws.send(JSON.stringify({
        type: 'RECONNECTED',
        playerId: row.player_id,
        nickname: player?.nickname || '匿名玩家',
        skin: player?.skin || 'default',
        roomCode: row.room_code,
        gameState: row.game_state ? JSON.parse(row.game_state) : null,
        team: room.teams.get(row.player_id) || 0,
        mode: room.mode,
      }));
      stmts.reconnect_delete.run(row.player_id);
      console.log(`[重连] ${player?.nickname} 回到房间 ${row.room_code}`);
      return;
    }

    // ── CREATE: 创建房间 ──────────────────────────
    if (msg.type === 'CREATE') {
      const code = msg.code || String(Math.floor(100000 + Math.random() * 900000));
      const mode = msg.mode || 'normal';
      const maxPlayers = Math.min(Math.max(msg.maxPlayers || 2, 2), 6);
      const config = msg.config || {};

      if (activeRooms.has(code)) {
        ws.send(JSON.stringify({ type: 'ERROR', reason: 'ROOM_EXISTS' }));
        return;
      }

      const playerId = generateId();
      const token = generateToken();
      const nickname = (msg.nickname || '匿名玩家').substring(0, 12);
      const skin = msg.skin || 'default';

      stmts.player_upsert.run(playerId, token, nickname, skin);
      const room = createRoom(code, mode, maxPlayers, config);
      addPlayerToRoom(code, playerId, ws, 0);
      ws.playerId = playerId;
      ws.roomCode = code;

      ws.send(JSON.stringify({
        type: 'CREATED',
        code, playerId, token, nickname, skin,
        mode, maxPlayers, team: room.teams.get(playerId),
        ttl: 300000,
      }));
      console.log(`[房间] ${nickname} 创建 ${code} (${mode}, ${maxPlayers}人)`);
      return;
    }

    // ── JOIN: 加入房间 ──────────────────────────
    if (msg.type === 'JOIN') {
      const code = msg.code;
      let room = activeRooms.get(code);

      if (!room) {
        const dbRoom = stmts.room_get.get(code);
        if (!dbRoom) { ws.send(JSON.stringify({ type: 'ERROR', reason: 'ROOM_NOT_FOUND' })); return; }
        // 从 DB 恢复房间
        room = createRoom(code, dbRoom.mode, dbRoom.max_players, JSON.parse(dbRoom.config || '{}'));
      }

      const aliveCount = [...room.players.values()].filter(w => w.readyState === 1).length;
      if (aliveCount >= room.maxPlayers) {
        ws.send(JSON.stringify({ type: 'ERROR', reason: 'ROOM_FULL' }));
        return;
      }

      const playerId = generateId();
      const token = generateToken();
      const nickname = (msg.nickname || '匿名玩家').substring(0, 12);
      const skin = msg.skin || 'default';

      stmts.player_upsert.run(playerId, token, nickname, skin);
      const team = msg.team || 0;
      addPlayerToRoom(code, playerId, ws, team);
      ws.playerId = playerId;
      ws.roomCode = code;

      ws.send(JSON.stringify({
        type: 'JOINED',
        code, playerId, token, nickname, skin,
        mode: room.mode, maxPlayers: room.maxPlayers,
        team: room.teams.get(playerId),
        playerCount: aliveCount + 1,
      }));

      // 通知房间其他玩家
      sendToRoom(code, JSON.stringify({
        type: 'PLAYER_JOINED',
        playerId, nickname, skin,
        team: room.teams.get(playerId),
        playerCount: aliveCount + 1,
      }), ws);

      // 满员自动开始
      const newCount = [...room.players.values()].filter(w => w.readyState === 1).length;
      if (newCount >= room.maxPlayers) {
        room.phase = 'playing';
        stmts.room_update.run('playing', code);
        sendToRoom(code, JSON.stringify({ type: 'START' }));
        startGame(code);
      }

      console.log(`[房间] ${nickname} 加入 ${code} (${newCount}/${room.maxPlayers})`);
      return;
    }

    // ── 对局消息转发（1v1 简单中继） ──────────
    const code = ws.roomCode;
    if (!code) return;
    const room = activeRooms.get(code);
    if (!room) return;
    stmts.room_update.run(room.phase, code);

    // NEXT_ROUND / RESTART_MATCH / OPEN / TIMEOUT 触发 phase 切换
    if (msg.type === 'NEXT_ROUND' || msg.type === 'RESTART_MATCH') {
      room.phase = 'playing';
      room.currentBid = { qty: 0, pip: 0, bidder: null };
      stmts.room_update.run('playing', code);
    }
    // OPEN/TIMEOUT：只在 playing 状态接受，防止重复结算
    if ((msg.type === 'OPEN' || msg.type === 'TIMEOUT') && room.phase === 'playing') {
      room.phase = 'ended';
      stmts.room_update.run('ended', code);
    }

    // 默认：发给房间所有人（除发送者）
    sendToRoom(code, raw.toString(), ws);
  });

  ws.on('close', () => {
    totalConnections--;
    const code = ws.roomCode;
    const playerId = ws.playerId;

    if (code && playerId) {
      const room = activeRooms.get(code);
      if (room) {
        // 对局中断线：通知对手强制退出
        if (room.phase === 'playing' || room.phase === 'rolling') {
          room.phase = 'ended';
          stmts.room_update.run('ended', code);
          sendToRoom(code, JSON.stringify({
            type: 'ROOM_CLOSED',
            reason: 'OPPONENT_LEFT',
            message: '对手已断开连接，对局结束',
            playerId,
          }), ws);
        } else if (room.phase === 'waiting') {
          // 等待中断线：通知对方，清理房间
          sendToRoom(code, JSON.stringify({
            type: 'ROOM_CLOSED',
            reason: 'HOST_LEFT',
            message: '房主已离开，房间关闭',
            playerId,
          }), ws);
          // 直接移除房间
          activeRooms.delete(code);
          stmts.room_delete.run(code);
        } else if (room.phase === 'ended') {
          // 对局已结束（正常结束/超时判负后）：通知对方可返回主菜单
          sendToRoom(code, JSON.stringify({
            type: 'ROOM_CLOSED',
            reason: 'OPPONENT_LEFT',
            message: '对手已离开（对局已结束），可返回主菜单',
            playerId,
          }), ws);
        } else {
          sendToRoom(code, JSON.stringify({
            type: 'PLAYER_DISCONNECTED',
            playerId,
            willReconnect: true,
          }), ws);
        }

        // 创建重连令牌（3分钟有效期）
        const token = generateToken();
        const expiresAt = now() + 180;
        const gameState = JSON.stringify({
          phase: room.phase,
          currentBid: room.currentBid,
          timestamp: now(),
        });
        stmts.reconnect_upsert.run(playerId, code, token, expiresAt, gameState);

        console.log(`[断线] ${playerId} 离开 ${code}（${room.phase}），重连令牌有效期 3 分钟`);
      }
    }
    ws.playerId = null;
    ws.roomCode = null;
  });

  ws.on('error', (err) => console.error('[WS 错误]', err.message));
});

// ══════════════════════════════════════════════════════════════
// 游戏开始 & 回合管理
// ══════════════════════════════════════════════════════════════
function startGame(code) {
  const room = activeRooms.get(code);
  if (!room) return;
  room.phase = 'playing';
  room.currentBid = { qty: 0, pip: 0, bidder: null };

  const players = getActivePlayers(code);
  const playerIds = players.map(p => p.id);

  players.forEach(p => {
    p.ws.send(JSON.stringify({
      type: 'GAME_START',
      playerId: p.id,
      players: players.map(pp => ({
        id: pp.id,
        nickname: stmts.player_get.get(pp.id)?.nickname || '?',
        skin: stmts.player_get.get(pp.id)?.skin || 'default',
      })),
    }));
  });

  console.log(`[游戏] ${code} 开始！${players.length} 名玩家`);
}

// ══════════════════════════════════════════════════════════════
// 心跳
// ══════════════════════════════════════════════════════════════
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// ══════════════════════════════════════════════════════════════
// 定时清理
// ══════════════════════════════════════════════════════════════
setInterval(() => {
  // 清理过期重连令牌
  stmts.cleanup_reconnect.run();

  // 清理空房间 / 过期等待房间（10 分钟无活动）
  const cutoff = now() - 600;
  stmts.cleanup_rooms.run(cutoff);
  for (const [code, room] of activeRooms) {
    const aliveCount = [...room.players.values()].filter(w => w.readyState === 1).length;
    if (aliveCount === 0) {
      activeRooms.delete(code);
      console.log(`[清理] 房间 ${code} 无活跃玩家`);
    }
  }
}, 60000);

// ══════════════════════════════════════════════════════════════
// 优雅关闭
// ══════════════════════════════════════════════════════════════
function shutdown(signal) {
  console.log(`\n[关闭] 收到 ${signal}`);
  wss.close(() => console.log('[关闭] WS 已停止'));
  db.close();
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 3000);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

// ══════════════════════════════════════════════════════════════
// 启动
// ══════════════════════════════════════════════════════════════
httpServer.listen(PORT, '0.0.0.0', () => {
  const nets = require('os').networkInterfaces();
  const ips = [];
  for (const iface of Object.values(nets)) {
    for (const alias of iface) {
      if (alias.family === 'IPv4' && !alias.internal) ips.push(alias.address);
    }
  }

  console.log('\n========================================');
  console.log('  🎲 吹牛5骰 多人服务器 v2');
  console.log('========================================');
  console.log(`  端口:       ${PORT}`);
  console.log(`  数据库:     ${DB_PATH}`);
  console.log(`  模式:       1v1 对战 (AI / 局域网)`);
  console.log(`  断线重连:   3分钟窗口`);
  console.log(`  皮肤同步:   已启用`);
  console.log(`  本机访问:   http://localhost:${PORT}`);
  ips.forEach(ip => console.log(`  局域网:     http://${ip}:${PORT}`));
  console.log('========================================\n');
});
