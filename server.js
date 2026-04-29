'use strict';

const mqtt   = require('mqtt');
const admin  = require('firebase-admin');
const dotenv = require('dotenv');
dotenv.config();

// Ambil konfigurasi dari Railway Variables
const MQTT_BROKER = process.env.MQTT_BROKER;
const MQTT_USER   = process.env.MQTT_USER;
const MQTT_PASS   = process.env.MQTT_PASS;
const MQTT_PORT   = parseInt(process.env.MQTT_PORT || '8883');
const FB_DB_URL   = process.env.FIREBASE_DB_URL;

// Inisialisasi Tanpa File JSON (Mode Publik)
admin.initializeApp({
  databaseURL: FB_DB_URL
});

const db = admin.database();
console.log('✓ Firebase Connected (Public Mode)');

// Konfigurasi MQTT
const mqttOptions = {
  port: MQTT_PORT,
  protocol: 'mqtts',
  username: MQTT_USER,
  password: MQTT_PASS,
  clientId: `Bridge_${Date.now()}`,
  rejectUnauthorized: false 
};

const client = mqtt.connect(`mqtts://${MQTT_BROKER}`, mqttOptions);

client.on('connect', () => {
  console.log('✓ MQTT Connected to HiveMQ');
  client.subscribe(['rentakar/+/gps', 'rentakar/+/sensor'], { qos: 1 });
});

client.on('message', async (topic, payload) => {
  const [,, vehicleId, type] = topic.split('/');
  try {
    const data = JSON.parse(payload.toString());
    await db.ref(`vehicles/${vehicleId}/${type}/latest`).set({
      ...data,
      serverTs: admin.database.ServerValue.TIMESTAMP
    });
    console.log(`[DATA] ${vehicleId} updated ${type}`);
  } catch (err) {
    console.error('Error:', err.message);
  }
});
