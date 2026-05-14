const mqtt = require('mqtt');
const uuid = require('uuid');
const Crypto = require('crypto');
const CryptoJS = require('crypto-js');
const LINK_ID = uuid.v1();
GCM_TAG_LENGTH = 16;
var debuglog;

const HEALTH_CHECK_MS = 5 * 60 * 1000;   // If no message for 5 min, force reconnect

class TuyaOpenMQ {

    constructor(api, type, log) {
        this.type = type;
        this.api = api;
        this.message_listeners = new Set();
        this.client = null;
        this._lastMessageTime = 0;
        debuglog = log;
    }

    start() {
        this.running = true;
        this._retryDelay = 5000; // start with 5s backoff
        this._loop_start();
    }

    stop() {
        this.running = false;
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
        if (this._healthTimer) {
            clearInterval(this._healthTimer);
            this._healthTimer = null;
        }
        if (this._interruptReconnect) {
            this._interruptReconnect();
        }
        if (this.client) {
            this.client.end();
        }
    }

    async _loop_start() {
        let that = this;
        while (this.running) {

            let res = await this._getMQConfig('mqtt');
            if (res == null || res.success == false) {
                debuglog.log('TuyaOpenMQ: failed to get MQ config, stopping');
                this.stop();
                break;
            }

            let mqConfig = res.result;
            let { url, client_id, username, password, expire_time, source_topic } = mqConfig;
            that.deviceTopic = source_topic.device;
            debuglog.log(`TuyaOpenMQ connecting: ${url}, topic: ${that.deviceTopic}`);

            let client = mqtt.connect(url, {
                clientId: client_id,
                username: username,
                password: password,
                connectTimeout: 30000,
                reconnectPeriod: 0, // disable built-in reconnect; we handle it ourselves
                keepalive: 30,      // send PINGREQ every 30s to detect dead connections
            });

            // Promise that resolves when we should reconnect (either via timer or early interrupt)
            let interruptReconnect;
            const reconnectPromise = new Promise(resolve => {
                interruptReconnect = resolve;
                that._interruptReconnect = resolve;
            });

            let connected = false;

            client.on('connect', () => {
                connected = true;
                that._retryDelay = 5000; // reset backoff on successful connect
                that._lastMessageTime = Date.now();
                debuglog.log('TuyaOpenMQ connected');

                // Subscribe with confirmation
                client.subscribe(that.deviceTopic, { qos: 1 }, (err, granted) => {
                    if (err) {
                        debuglog.log('TuyaOpenMQ subscribe ERROR:', err);
                    } else {
                        debuglog.log(`TuyaOpenMQ subscribed: ${JSON.stringify(granted)}`);
                    }
                });

                // Schedule normal reconnect just before token expiry (2h)
                that._reconnectTimer = setTimeout(() => {
                    debuglog.log('TuyaOpenMQ: token expiry approaching, reconnecting');
                    interruptReconnect('expiry');
                }, (expire_time - 60) * 1000);
            });

            // Health check: if no messages received for a long time, force reconnect
            that._healthTimer = setInterval(() => {
                if (connected && Date.now() - that._lastMessageTime > HEALTH_CHECK_MS) {
                    debuglog.log(`TuyaOpenMQ: no messages for ${HEALTH_CHECK_MS / 1000}s, forcing reconnect`);
                    interruptReconnect('health');
                }
            }, 60 * 1000);

            client.on('error', (err) => {
                debuglog.log('TuyaOpenMQ error:', err);
                if (!connected) {
                    // Connection never established; interrupt and retry with backoff
                    interruptReconnect('error');
                }
            });

            client.on('close', () => {
                debuglog.log('TuyaOpenMQ connection closed');
                interruptReconnect('close');
            });

            client.on('offline', () => {
                debuglog.log('TuyaOpenMQ offline — broker unreachable');
                interruptReconnect('offline');
            });

            client.on('end', () => {
                debuglog.log('TuyaOpenMQ end');
            });

            client.on('message', (topic, payload) => that._onMessage(client, mqConfig, topic, payload));

            if (this.client) {
                this.client.end();
            }
            this.client = client;

            // Wait until we need to reconnect (expiry timer OR error/close)
            const reason = await reconnectPromise;

            clearTimeout(that._reconnectTimer);
            that._reconnectTimer = null;
            if (that._healthTimer) {
                clearInterval(that._healthTimer);
                that._healthTimer = null;
            }
            client.end();

            if (!this.running) break;

            if (reason === 'error' || reason === 'close' || reason === 'offline' || reason === 'health') {
                // Apply exponential backoff (max 5 minutes)
                that._retryDelay = Math.min(that._retryDelay * 2, 300000);
                debuglog.log(`TuyaOpenMQ: reconnecting in ${that._retryDelay / 1000}s (reason: ${reason})`);
                await new Promise(r => setTimeout(r, that._retryDelay));
            }
            // reason === 'expiry': reconnect immediately with fresh token
        }

    }

    async _getMQConfig(linkType) {
        let res = await this.api.post('/v1.0/iot-03/open-hub/access-config', {
            'uid': this.api.tokenInfo.uid,
            'link_id': LINK_ID,
            'link_type': linkType,
            'topics': 'device',
            'msg_encrypted_version': this.type,
        });
        return res;
    }


    _onMessage(client, mqConfig, topic, payload) {
        try {
            this._lastMessageTime = Date.now();
            let message = JSON.parse(payload.toString());

            // Auto-detect protocol version from the message itself (pv field)
            // instead of relying on the hardcoded this.type
            const messageVersion = message.pv || this.type;
            let decoded;
            try {
                decoded = messageVersion === '2.0'
                    ? this._decodeMQMessage(message.data, mqConfig.password, message.t)
                    : this._decodeMQMessage_1_0(message.data, mqConfig.password);
            } catch (decryptErr) {
                debuglog.log(`TuyaOpenMQ decryption failed (pv=${messageVersion}), trying fallback...`);
                // Try the other version as fallback
                try {
                    decoded = messageVersion === '2.0'
                        ? this._decodeMQMessage_1_0(message.data, mqConfig.password)
                        : this._decodeMQMessage(message.data, mqConfig.password, message.t);
                    debuglog.log('TuyaOpenMQ fallback decryption succeeded');
                } catch (fallbackErr) {
                    debuglog.log('TuyaOpenMQ decryption failed with both versions:', decryptErr.message);
                    return; // skip this message, don't crash
                }
            }

            if (!decoded || decoded.length === 0) {
                debuglog.log('TuyaOpenMQ: empty decoded message, skipping');
                return;
            }

            message.data = JSON.parse(decoded);
            debuglog.log(`TuyaOpenMQ onMessage: topic = ${topic}, devId = ${message.data.devId || 'N/A'}, bizCode = ${message.data.bizCode || 'none'}`);

            this.message_listeners.forEach(listener => {
                try {
                    if (this.deviceTopic == topic) {
                        listener(message.data);
                    }
                } catch (listenerErr) {
                    debuglog.log('TuyaOpenMQ listener error:', listenerErr);
                }
            });
        } catch (err) {
            debuglog.log('TuyaOpenMQ _onMessage error:', err.message || err);
            // Don't rethrow — one bad message must not kill the handler for all future messages
        }
    }

    // 1.0
    _decodeMQMessage_1_0(b64msg, password) {
        password = password.substring(8, 24);
        let msg = CryptoJS.AES.decrypt(b64msg, CryptoJS.enc.Utf8.parse(password), {
            mode: CryptoJS.mode.ECB,
            padding: CryptoJS.pad.Pkcs7,
        }).toString(CryptoJS.enc.Utf8);
        return msg;
    }

    _decodeMQMessage(data, password, t) {
        // Base64 decoding generates Buffers
        var tmpbuffer = Buffer.from(data, 'base64');
        var key = password.substring(8, 24).toString('utf8');
        //get iv_length & iv_buffer
        var iv_length = tmpbuffer.readUIntBE(0,4);
        var iv_buffer = tmpbuffer.slice(4, iv_length + 4);
        //Removes the IV bits of the head and 16 bits of the tail tags
        var data_buffer = tmpbuffer.slice(iv_length + 4, tmpbuffer.length - GCM_TAG_LENGTH);
        var cipher = Crypto.createDecipheriv('aes-128-gcm', key, iv_buffer);
        //setAuthTag buffer
        cipher.setAuthTag(tmpbuffer.slice(tmpbuffer.length - GCM_TAG_LENGTH, tmpbuffer.length));
        //setAAD buffer
        const buf = Buffer.allocUnsafe(6);
        buf.writeUIntBE(t, 0, 6);
        cipher.setAAD(buf);
        
        var msg = cipher.update(data_buffer);
        return msg.toString('utf8');
    }

    addMessageListener(listener) {
        this.message_listeners.add(listener);
    }

    removeMessageListener(listener) {
        this.message_listeners.delete(listener);
    }

}

module.exports = TuyaOpenMQ;
