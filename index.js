import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';

const app = express();
app.use(cors());

const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

const PORT = process.env.PORT || 3001;

// State management
const onlineUsers = new Map(); // socketId -> { id, alias, mode, inCallWith }
const matchQueue = []; // array of socketIds waiting for a match
const activeCalls = new Map(); // callId -> { user1: socketId, user2: socketId, startTime }
const reservedUsernames = new Map(); // lowercase alias -> socketId

function broadcastStats() {
  const onlineCount = onlineUsers.size;
  const inCallCount = activeCalls.size * 2;
  const roomCounts = {
    geyik: 0,
    dertlesme: 0,
    gece: 0,
    dil: 0,
    oyun: 0,
    itiraf: 0
  };
  for (const user of onlineUsers.values()) {
    const m = user.mode || 'geyik';
    if (roomCounts[m] !== undefined) {
      roomCounts[m]++;
    }
  }
  io.emit('stats_update', {
    onlineCount,
    inCallCount,
    roomCounts
  });
}

io.on('connection', (socket) => {
  console.log(`[+] Yeni baglanti: ${socket.id}`);

  // Check if username is available (Strict uniqueness)
  socket.on('check_username', (data, callback) => {
    const rawAlias = (data && data.alias ? data.alias : '').trim();
    const clean = rawAlias.toLowerCase();
    if (!clean || clean.length < 3) {
      if (typeof callback === 'function') callback({ available: false, error: 'Kullanıcı adı en az 3 karakter olmalıdır.' });
      return;
    }
    const currentOwner = reservedUsernames.get(clean);
    const isTaken = currentOwner && currentOwner !== socket.id;
    if (typeof callback === 'function') callback({ available: !isTaken, alias: rawAlias });
  });

const BANNED_WORDS = ['amk', 'aq', 'oç', 'oc', 'orospu', 'piç', 'pic', 'yarrak', 'yarak', 'sik', 'siktir', 'amcık', 'göt', 'got', 'ibne', 'pezevenk', 'kahpe'];
function containsBanned(str) {
  if (!str) return false;
  const lower = str.toLowerCase();
  return BANNED_WORDS.some(w => lower.includes(w));
}

  // 1. User registers alias upon socket connection
  socket.on('register_user', (userData, callback) => {
    let desiredAlias = (userData && userData.alias ? userData.alias : '').trim();
    if (!desiredAlias || containsBanned(desiredAlias)) {
      desiredAlias = `Anonim Fısıltı #${Math.floor(1000 + Math.random() * 9000)}`;
    }
    const clean = desiredAlias.toLowerCase();
    const currentOwner = reservedUsernames.get(clean);

    if (currentOwner && currentOwner !== socket.id) {
      // Username is already taken, ensure uniqueness by appending a unique discriminator #XXXX
      const base = desiredAlias.includes('#') ? desiredAlias.split('#')[0].trim() : desiredAlias;
      desiredAlias = `${base} #${Math.floor(1000 + Math.random() * 9000)}`;
    }

    reservedUsernames.set(desiredAlias.toLowerCase(), socket.id);

    onlineUsers.set(socket.id, {
      socketId: socket.id,
      alias: desiredAlias,
      avatarId: (userData && userData.avatarId) ? userData.avatarId : 'boy_std_1',
      gender: (userData && userData.gender) ? userData.gender : 'erkek',
      inCallWith: null,
      mode: 'geyik'
    });
    broadcastStats();
    if (typeof callback === 'function') callback({ success: true, alias: desiredAlias });
  });

  // 1.1. Change active chat room mode
  socket.on('set_mode', (mode) => {
    const user = onlineUsers.get(socket.id);
    if (user) {
      user.mode = mode || 'geyik';
      broadcastStats();
    }
  });

  // 2. User joins matchmaking queue
  socket.on('join_queue', (data) => {
    const user = onlineUsers.get(socket.id);
    if (!user) return;
    user.mode = data.mode || 'geyik';
    user.targetGender = data.targetGender || 'all';

    // Remove if already in queue
    const existingIdx = matchQueue.indexOf(socket.id);
    if (existingIdx !== -1) matchQueue.splice(existingIdx, 1);

    // Look for a compatible peer in queue
    let matchIdx = -1;
    for (let i = 0; i < matchQueue.length; i++) {
      const candidateId = matchQueue[i];
      if (candidateId === socket.id) continue;
      const candidate = onlineUsers.get(candidateId);
      if (!candidate) continue;

      // 1. Strict Room Mode Separation (Dil Pratiği only matches Dil Pratiği, Gece only Gece, etc.)
      const modeMatches = user.mode === candidate.mode;

      // 2. Strict Bidirectional Gender Matching
      const userWantsCandidate = !user.targetGender || user.targetGender === 'all' || user.targetGender === candidate.gender;
      const candidateWantsUser = !candidate.targetGender || candidate.targetGender === 'all' || candidate.targetGender === user.gender;

      if (modeMatches && userWantsCandidate && candidateWantsUser) {
        matchIdx = i;
        break;
      }
    }

    if (matchIdx !== -1) {
      const partnerSocketId = matchQueue.splice(matchIdx, 1)[0];
      const partner = onlineUsers.get(partnerSocketId);

      if (partner && partnerSocketId !== socket.id) {
        // MATCH FOUND!
        const callId = `call_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
        user.inCallWith = partnerSocketId;
        partner.inCallWith = socket.id;

        activeCalls.set(callId, {
          user1: socket.id,
          user2: partnerSocketId,
          startTime: Date.now()
        });

        // Notify user 1 (initiator / caller)
        io.to(socket.id).emit('match_found', {
          callId,
          partnerAlias: partner.alias,
          partnerAvatarId: partner.avatarId || 'girl_std_1',
          isInitiator: true
        });

        // Notify user 2 (receiver / callee)
        io.to(partnerSocketId).emit('match_found', {
          callId,
          partnerAlias: user.alias,
          partnerAvatarId: user.avatarId || 'boy_std_1',
          isInitiator: false
        });

        broadcastStats();
        console.log(`[★] Eslesme Saglandi: ${user.alias} <---> ${partner.alias}`);
        return;
      }
    }

    // No peer available yet, wait in queue
    matchQueue.push(socket.id);
    socket.emit('queue_waiting');
  });

  // 3. Leave queue
  socket.on('leave_queue', () => {
    const idx = matchQueue.indexOf(socket.id);
    if (idx !== -1) {
      matchQueue.splice(idx, 1);
    }
  });

  // 4. WebRTC Signaling Relays
  socket.on('webrtc_offer', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('webrtc_offer', {
        sdp: data.sdp,
        sender: socket.id
      });
    }
  });

  socket.on('webrtc_answer', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('webrtc_answer', {
        sdp: data.sdp,
        sender: socket.id
      });
    }
  });

  socket.on('webrtc_ice_candidate', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('webrtc_ice_candidate', {
        candidate: data.candidate
      });
    }
  });

  // 5. In-call Events: Gifts
  socket.on('send_gift', (giftData) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('gift_received', giftData);
    }
  });

  // 5.0. Extend Call Duration (BlindID Feature)
  socket.on('extend_call', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('call_extended', {
        addedSeconds: data.addedSeconds || 120,
        byAlias: data.byAlias || user.alias
      });
      console.log(`[⏳ SÜRE UZATILDI] ${user.alias} çağrıyı ${data.addedSeconds || 120} sn uzattı.`);
    }
  });

  // 5.1. Security Violation Report
  socket.on('report_user', (data) => {
    const reporter = onlineUsers.get(socket.id);
    const reporterName = reporter ? reporter.alias : socket.id;
    console.warn(`[🚨 GÜVENLİK İHLALİ] ${reporterName} -> ${data.targetAlias} kullanıcısını şikayet etti. Neden: ${data.reason}`);
  });

  // 5.2. Direct Message Relay (Text & 24h Expiring Voice Message)
  socket.on('send_direct_message', (data) => {
    // data: { toAlias, fromAlias, text, audioData, audioDuration, timestamp }
    for (const [sId, u] of onlineUsers.entries()) {
      if (u.alias === data.toAlias) {
        io.to(sId).emit('direct_message_received', data);
        console.log(`[💬 MESAJ İLETİLDİ] ${data.fromAlias} -> ${data.toAlias}: ${data.text ? data.text.substring(0, 20) : 'Sesli Mesaj'}`);
        break;
      }
    }
  });

  // 5.3. Icebreaker Card Synchronizer (Both sides see the exact same question simultaneously)
  socket.on('send_icebreaker', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('icebreaker_received', data);
      console.log(`[🎲 BUZKIRAN GÖNDERİLDİ] ${user.alias} -> karşı tarafa soru gönderdi: "${data.question?.substring(0, 25)}..."`);
    }
  });

  // 5.4. Live In-Call Reaction Soundboard Relay
  socket.on('send_sound_reaction', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('sound_reaction_received', data);
      console.log(`[🔊 SES EFEKTİ TEPKİSİ] ${user.alias} -> ${data.reactionId}`);
    }
  });

  // 5.5. Mini Game Duel Relay ("Bu mu, Şu mu?" Hızlı Tercih Oyunu)
  socket.on('start_duel', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('duel_started', data);
      console.log(`[🎮 DÜELLO BAŞLADI] ${user.alias} düello başlattı. Soru ID: ${data.duelId}`);
    }
  });

  socket.on('submit_duel_choice', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('partner_duel_choice', data);
      console.log(`[🎮 DÜELLO CEVABI] ${user.alias} seçim yaptı: ${data.choice}`);
    }
  });

  // 5.6. Şişe Çevirmece (Spin the Bottle - Doğruluk / Cesaret) Relay
  socket.on('spin_bottle', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('bottle_spun', data);
      console.log(`[🍾 ŞİŞE ÇEVRİLDİ] ${user.alias} çevirdi -> Hedef: ${data.target}`);
    }
  });

  // 5.7. Taş Kağıt Makas (Rock Paper Scissors) Relay
  socket.on('start_rps', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('rps_started', data);
      console.log(`[✂️ TAŞ KAĞIT MAKAS BAŞLADI] ${user.alias} düellosu`);
    }
  });

  socket.on('submit_rps_choice', (data) => {
    const user = onlineUsers.get(socket.id);
    if (user && user.inCallWith) {
      io.to(user.inCallWith).emit('partner_rps_choice', data);
      console.log(`[✂️ TAŞ KAĞIT MAKAS CEVAP] ${user.alias} seçim yaptı`);
    }
  });

  // 6. Direct Friend Nudge
  socket.on('send_nudge', (data) => {
    for (const [sId, u] of onlineUsers.entries()) {
      if (u.alias === data.targetAlias) {
        io.to(sId).emit('nudge_received', { fromAlias: data.fromAlias });
        break;
      }
    }
  });

  // 6.1. Direct Friend Call Request (Ringing Notification)
  socket.on('call_friend', (data) => {
    // data: { fromAlias, targetAlias, callerAvatarId }
    for (const [sId, u] of onlineUsers.entries()) {
      if (u.alias === data.targetAlias) {
        io.to(sId).emit('incoming_friend_call', {
          callerSocketId: socket.id,
          callerAlias: data.fromAlias,
          callerAvatarId: data.callerAvatarId,
          callId: `friend_call_${Date.now()}`
        });
        break;
      }
    }
  });

  // 6.2. Accept Friend Call
  socket.on('accept_friend_call', (data) => {
    const caller = onlineUsers.get(data.callerSocketId);
    const receiver = onlineUsers.get(socket.id);
    if (caller && receiver) {
      caller.inCallWith = socket.id;
      receiver.inCallWith = data.callerSocketId;
      activeCalls.set(data.callId, {
        user1: data.callerSocketId,
        user2: socket.id,
        startTime: Date.now()
      });

      // Connect both into 15-minute call
      io.to(data.callerSocketId).emit('friend_call_accepted', {
        callId: data.callId,
        partnerAlias: receiver.alias,
        isInitiator: true
      });

      socket.emit('friend_call_accepted', {
        callId: data.callId,
        partnerAlias: caller.alias,
        isInitiator: false
      });

      broadcastStats();
    }
  });

  // 6.3. Reject Friend Call
  socket.on('reject_friend_call', (data) => {
    if (data && data.callerSocketId) {
      io.to(data.callerSocketId).emit('friend_call_rejected', {
        fromAlias: onlineUsers.get(socket.id)?.alias || 'Arkadaşın'
      });
    }
  });

  // 6.4. Send Friend Request Relay
  socket.on('send_friend_request', (data) => {
    for (const [sId, u] of onlineUsers.entries()) {
      if (u.alias === data.toAlias) {
        io.to(sId).emit('friend_request_received', {
          id: `req_${Date.now()}`,
          senderAlias: data.fromAlias,
          senderAvatarId: data.fromAvatarId || 'boy_std_1',
          senderBio: data.fromBio || 'Fısıltı kullanıcısı',
          timestamp: Date.now()
        });
        console.log(`[👥 ARKADAŞLIK İSTEĞİ] ${data.fromAlias} -> ${data.toAlias}`);
        break;
      }
    }
  });

  // 6.5. Accept Friend Request Relay
  socket.on('accept_friend_request', (data) => {
    for (const [sId, u] of onlineUsers.entries()) {
      if (u.alias === data.toAlias) {
        io.to(sId).emit('friend_request_accepted', {
          partnerAlias: data.fromAlias,
          partnerAvatarId: data.fromAvatarId || 'boy_std_1',
          partnerBio: data.fromBio || 'Fısıltı arkadaşı'
        });
        console.log(`[👥 İSTEK KABUL EDİLDİ] ${data.fromAlias} -> ${data.toAlias}`);
        break;
      }
    }
  });

  // 7. Explicit End Call / Hang Up
  socket.on('end_call', () => {
    handleCallTermination(socket, 'hung_up');
  });

  // 8. Skip Call (Hang up and find next)
  socket.on('skip_call', () => {
    handleCallTermination(socket, 'skipped');
  });

  // Helper function to cleanly terminate call
  function handleCallTermination(closingSocket, reason) {
    const user = onlineUsers.get(closingSocket.id);
    if (user && user.inCallWith) {
      const partnerSocketId = user.inCallWith;
      const partner = onlineUsers.get(partnerSocketId);

      // Clean activeCalls
      for (const [callId, call] of activeCalls.entries()) {
        if (call.user1 === closingSocket.id || call.user2 === closingSocket.id) {
          activeCalls.delete(callId);
          break;
        }
      }

      user.inCallWith = null;
      if (partner) {
        partner.inCallWith = null;
        // Notify partner that call ended -> triggers disconnect tone!
        io.to(partnerSocketId).emit('call_ended', { reason });
      }

      closingSocket.emit('call_ended', { reason });
      broadcastStats();
    }
  }

  // 9. Disconnect cleanup
  socket.on('disconnect', () => {
    console.log(`[-] Baglanti koptu: ${socket.id}`);
    handleCallTermination(socket, 'disconnected');

    const idx = matchQueue.indexOf(socket.id);
    if (idx !== -1) matchQueue.splice(idx, 1);

    onlineUsers.delete(socket.id);
    broadcastStats();
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    onlineCount: onlineUsers.size,
    inCallCount: activeCalls.size * 2
  });
});

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`===============================================`);
  console.log(`  FISILTI Sinyal Sunucusu Aktif! Port: ${PORT}`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`===============================================`);
});
