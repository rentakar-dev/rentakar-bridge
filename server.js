/**
 * ╔══════════════════════════════════════════════════════╗
 * ║    RentaKar GPS — MQTT ↔ Firebase Bridge Server     ║
 * ║    Node.js + mqtt.js + Firebase Admin SDK           ║
 * ╚══════════════════════════════════════════════════════╝
 *
 * Fungsi:
 *  1. Subscribe ke semua topic MQTT dari ESP32
 *  2. Simpan data ke Firebase Realtime Database
 *  3. Forward command dari Firebase ke MQTT (ESP32)
 *
 * Deploy: Railway / Render / VPS / Raspberry Pi
 */

'use strict';

const mqtt   = require('mqtt');
const admin  = require('firebase-admin');
const dotenv = require('dotenv');
dotenv.config();

// ─────────────────────────────────────────────
// KONFIGURASI (dari .env)
// ─────────────────────────────────────────────
const MQTT_BROKER = process.env.MQTT_BROKER;       // hivemq cloud url
const MQTT_USER   = process.env.MQTT_USER;
const MQTT_PASS   = process.env.MQTT_PASS;
const MQTT_PORT   = parseInt(process.env.MQTT_PORT || '8883');

const FB_DB_URL   = process.env.FIREBASE_DB_URL;   // https://xxx.firebaseio.com
const FB_KEY_PATH = process.env.FIREBASE_KEY_PATH || './firebase-service-account.json';

const MAX_GPS_HISTORY    = 500;  // titik riwayat GPS per kendaraan
const MAX_ALERT_HISTORY  = 100;  // alert history

// ─────────────────────────────────────────────
// FIREBASE ADMIN INIT
// ─────────────────────────────────────────────
let serviceAccount;
try {
  serviceAccount = require(FB_KEY_PATH);
} catch {
  // Bisa juga dari env variable (untuk Railway/Render)
  serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON || '{}');
}

admin.initializeApp({
  databaseURL: FB_DB_URL
});

const db = admin.database();
console.log('✓ Firebase Admin SDK terhubung');

// ─────────────────────────────────────────────
// MQTT CLIENT
// ─────────────────────────────────────────────
const mqttOptions = {
  port: MQTT_PORT,
  protocol: 'mqtts',       // TLS
  username: MQTT_USER,
  password: MQTT_PASS,
  clientId: `Bridge_RentaKar_${Date.now()}`,
  clean: true,
  reconnectPeriod: 5000,
  connectTimeout: 30000,
  rejectUnauthorized: false // set true di produksi dengan CA cert
};

const client = mqtt.connect(`mqtts://${MQTT_BROKER}`, mqttOptions);

client.on('connect', () => {
  console.log('✓ MQTT terhubung ke broker:', MQTT_BROKER);

  // Subscribe ke semua kendaraan (wildcard)
  const topics = [
    'rentakar/+/gps',
    'rentakar/+/sensor',
    'rentakar/+/status',
    'rentakar/+/alert'
  ];

  client.subscribe(topics, { qos: 1 }, (err) => {
    if (err) {
      console.error('✗ Subscribe error:', err);
    } else {
      console.log('✓ Subscribed ke topics:', topics);
    }
  });
});

client.on('error', (err) => console.error('MQTT Error:', err));
client.on('reconnect', () => console.log('⟳ Reconnecting MQTT...'));
client.on('offline', () => console.log('⚠ MQTT offline'));

// ─────────────────────────────────────────────
// MESSAGE HANDLER
// ─────────────────────────────────────────────
client.on('message', async (topic, payload) => {
  let data;
  try {
    data = JSON.parse(payload.toString());
  } catch {
    console.warn('Payload bukan JSON:', payload.toString());
    return;
  }

  // Extract vehicle ID dan tipe dari topic
  // Format: rentakar/{vehicleId}/{type}
  const parts = topic.split('/');
  if (parts.length < 3) return;
  const vehicleId = parts[1];
  const msgType   = parts[2];

  const tsKey = Date.now(); // timestamp sebagai key

  try {
    switch (msgType) {

      // ── GPS ──
      case 'gps':
        await handleGPS(vehicleId, tsKey, data);
        break;

      // ── SENSOR ──
      case 'sensor':
        await handleSensor(vehicleId, tsKey, data);
        break;

      // ── STATUS ──
      case 'status':
        await handleStatus(vehicleId, data);
        break;

      // ── ALERT ──
      case 'alert':
        await handleAlert(vehicleId, tsKey, data);
        break;
    }
  } catch (err) {
    console.error(`Error handling ${topic}:`, err.message);
  }
});

// ─────────────────────────────────────────────
// HANDLERS
// ─────────────────────────────────────────────

async function handleGPS(vid, ts, data) {
  // Update posisi terbaru
  await db.ref(`vehicles/${vid}/gps/latest`).set({
    ...data,
    ts,
    serverTs: admin.database.ServerValue.TIMESTAMP
  });

  // Simpan ke riwayat (path dengan timestamp sebagai key)
  await db.ref(`vehicles/${vid}/gps/history/${ts}`).set({
    lat: data.lat,
    lon: data.lon,
    spd: data.spd,
    ts
  });

  // Bersihkan riwayat jika terlalu banyak
  await pruneHistory(`vehicles/${vid}/gps/history`, MAX_GPS_HISTORY);

  // Update dashboard metrics
  await db.ref(`vehicles/${vid}/stats`).update({
    lastSeen: ts,
    currentSpeed: data.spd,
    satellites: data.sat,
    odometer: data.odo
  });

  console.log(`GPS [${vid}]: lat=${data.lat} lon=${data.lon} spd=${data.spd} km/h`);
}

async function handleSensor(vid, ts, data) {
  await db.ref(`vehicles/${vid}/sensor/latest`).set({
    ...data,
    ts,
    serverTs: admin.database.ServerValue.TIMESTAMP
  });

  await db.ref(`vehicles/${vid}/sensor/history/${ts}`).set({
    temp: data.temp,
    volt: data.volt,
    rpm: data.rpm,
    fuel: data.fuel,
    ts
  });

  await pruneHistory(`vehicles/${vid}/sensor/history`, 200);

  console.log(`SENSOR [${vid}]: temp=${data.temp}°C volt=${data.volt}V rpm=${data.rpm} fuel=${data.fuel}%`);
}

async function handleStatus(vid, data) {
  await db.ref(`vehicles/${vid}/status`).set({
    ...data,
    online: true,
    lastSeen: admin.database.ServerValue.TIMESTAMP
  });
  console.log(`STATUS [${vid}]: online rssi=${data.wifi_rssi}dBm`);
}

async function handleAlert(vid, ts, data) {
  await db.ref(`vehicles/${vid}/alerts/${ts}`).set({
    ...data,
    ts,
    read: false
  });

  await pruneHistory(`vehicles/${vid}/alerts`, MAX_ALERT_HISTORY);

  // Increment unread alert counter
  await db.ref(`vehicles/${vid}/stats/unreadAlerts`).transaction((curr) => {
    return (curr || 0) + 1;
  });

  console.log(`🚨 ALERT [${vid}] ${data.type}: ${data.msg}`);
}

// ─────────────────────────────────────────────
// UTILITY: PRUNING HISTORY
// ─────────────────────────────────────────────
async function pruneHistory(path, maxItems) {
  const snap = await db.ref(path).orderByKey().once('value');
  const count = snap.numChildren();
  if (count <= maxItems) return;

  const keys = [];
  snap.forEach((child) => keys.push(child.key));

  const toDelete = keys.slice(0, count - maxItems);
  const updates = {};
  toDelete.forEach((k) => { updates[`${path}/${k}`] = null; });
  await db.ref().update(updates);
}

// ─────────────────────────────────────────────
// FIREBASE → MQTT: Forward command ke ESP32
// ─────────────────────────────────────────────
// Listen perubahan di Firebase commands node
db.ref('commands').on('child_added', (snap) => {
  const cmd = snap.val();
  if (!cmd || !cmd.vehicleId || cmd.sent) return;

  const topic = `rentakar/${cmd.vehicleId}/command`;
  const payload = JSON.stringify({ cmd: cmd.action, param: cmd.param });

  client.publish(topic, payload, { qos: 1 }, async (err) => {
    if (err) {
      console.error('Gagal publish command:', err);
      return;
    }
    // Tandai sebagai terkirim
    await snap.ref.update({ sent: true, sentAt: Date.now() });
    console.log(`→ CMD [${cmd.vehicleId}]: ${cmd.action}`);
  });
});

// ─────────────────────────────────────────────
// HEARTBEAT: Tandai kendaraan offline jika tidak ada data 2 menit
// ─────────────────────────────────────────────
setInterval(async () => {
  const now = Date.now();
  const snap = await db.ref('vehicles').once('value');
  snap.forEach((veh) => {
    const status = veh.child('status').val();
    if (status && status.lastSeen && (now - status.lastSeen > 120000)) {
      db.ref(`vehicles/${veh.key}/status/online`).set(false);
      console.log(`⚠ [${veh.key}] ditandai offline`);
    }
  });
}, 60000);

console.log('╔══════════════════════════════════╗');
console.log('║  RentaKar Bridge Server Running  ║');
console.log('╚══════════════════════════════════╝');
