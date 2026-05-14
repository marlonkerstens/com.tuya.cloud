'use strict';

const TuyaBaseDevice = require('../tuyabasedevice');

const CAPABILITIES_SET_DEBOUNCE = 1000;
const BATTERY_LOW_THRESHOLD = 20;
const POLL_INTERVAL_MS = 15 * 1000;       // 15s — MQTT unreliable, need faster polling
const RAPID_POLL_INTERVAL_MS = 3 * 1000;  // 3s — used when mid-feed is detected
const RAPID_POLL_MAX_MS = 30 * 1000;      // stop rapid polling after 30s

class TuyaPetFeederDevice extends TuyaBaseDevice {

    async onInit() {
        this.initDevice(this.getData().id);

        // Migrate existing devices: add capabilities that may have been added after initial pairing
        const requiredCapabilities = [
            'quick_feed',
            'petfeeder_feed_state',
            'measure_feed_report',
            'measure_battery',
            'alarm_battery',
            'petfeeder_voice_times',
            'petfeeder_meal_plan'
        ];
        for (const cap of requiredCapabilities) {
            if (!this.hasCapability(cap)) {
                this.log(`Migrating: adding missing capability '${cap}'`);
                await this.addCapability(cap).catch(err => this.error(`Failed to add capability ${cap}:`, err));
            }
        }

        // In-memory trackers for feed_state transition detection
        this._lastFeedState = null;          // last known feed_state
        this._lastFeedReport = 0;            // last raw feed_report value (used by state machine)
        this._lastPolledFeedReport = null;   // feed_report from previous poll (used to detect external feeds)
        this._feedingInProgress = false;     // true from 'feeding' until counted at standby
        this._doneFired = false;             // prevents double-firing triggerFeedingDone per cycle
        this._rapidPollActive = false;

        this.setDeviceConfig(this.get_deviceConfig());

        // Fetch live status once at startup, then start regular polling
        await this._pollDeviceStatus();
        this._startPolling();

        // ── Direct MQTT listener ─────────────────────────────────────────────
        // Bypass app.js routing — ensures we receive MQTT messages even if
        // the generic routing has issues for this device type
        this._mqttListener = (data) => {
            if (data && data.devId === this.id && Array.isArray(data.status)) {
                this.log('MQTT direct listener: received status for this device');
                this.updateCapabilities(data.status, true);
            }
        };
        if (this.homey.app.tuyaOpenMQ) {
            this.homey.app.tuyaOpenMQ.addMessageListener(this._mqttListener);
            this.log('Registered direct MQTT listener for pet feeder');
        }

        this.registerMultipleCapabilityListener(['petfeeder_voice_times'], async (values, options) => {
            return this._onMultipleCapabilityListener(values, options);
        }, CAPABILITIES_SET_DEBOUNCE);
        this.registerCapabilityListener('quick_feed', async () => {
            this.quickFeed();
        });
        this.log(`Tuya Pet Feeder ${this.getName()} has been initialized`);
    }

    onDeleted() {
        this._stopPolling();
        this._stopRapidPoll();
        // Remove direct MQTT listener
        if (this.homey.app.tuyaOpenMQ && this._mqttListener) {
            this.homey.app.tuyaOpenMQ.removeMessageListener(this._mqttListener);
        }
    }

    // ── Polling ───────────────────────────────────────────────────────────────

    _startPolling() {
        this._stopPolling();
        this.log(`Starting pet feeder polling every ${POLL_INTERVAL_MS / 1000}s`);
        this._pollTimer = this.homey.setInterval(() => this._pollDeviceStatus(), POLL_INTERVAL_MS);
    }

    _stopPolling() {
        if (this._pollTimer) {
            this.homey.clearInterval(this._pollTimer);
            this._pollTimer = null;
        }
    }

    _startRapidPoll() {
        if (this._rapidPollActive) return;
        this._rapidPollActive = true;
        this.log('Starting rapid polling (3s) to track feeding cycle');
        this._rapidPollTimer = this.homey.setInterval(() => this._pollDeviceStatus(), RAPID_POLL_INTERVAL_MS);
        // Safety timeout: stop rapid polling after max duration
        this._rapidPollTimeout = this.homey.setTimeout(() => {
            this.log('Rapid polling max duration reached, stopping');
            this._stopRapidPoll();
        }, RAPID_POLL_MAX_MS);
    }

    _stopRapidPoll() {
        if (!this._rapidPollActive) return;
        this._rapidPollActive = false;
        if (this._rapidPollTimer) {
            this.homey.clearInterval(this._rapidPollTimer);
            this._rapidPollTimer = null;
        }
        if (this._rapidPollTimeout) {
            this.homey.clearTimeout(this._rapidPollTimeout);
            this._rapidPollTimeout = null;
        }
    }

    /**
     * Main poll method — detects external feedings via three strategies:
     * 1. Catch mid-feed state (feeding/done) → arm state machine + rapid poll
     * 2. State machine was armed (via MQTT or mid-feed catch) → complete at standby
     * 3. Completely missed cycle → detect via feed_report value change
     */
    async _pollDeviceStatus() {
        if (!this.homey.app.tuyaOpenApi) return;
        try {
            const freshStatus = await this.homey.app.tuyaOpenApi.getDeviceStatus(this.id);
            if (!freshStatus || !Array.isArray(freshStatus)) return;

            // Extract key values for smart detection
            const statusMap = {};
            for (const s of freshStatus) {
                statusMap[s.code] = s.value;
            }
            const feedState = statusMap['feed_state'];
            const feedReport = statusMap['feed_report'];
            let triggerFlows = false;

            // ── Strategy 1: caught mid-feed ──────────────────────────────────
            if (feedState === 'feeding' || feedState === 'done') {
                this.log(`Poll caught mid-feed: feed_state=${feedState}`);
                if (!this._feedingInProgress) {
                    this._feedingInProgress = true;
                    this._doneFired = false;
                }
                triggerFlows = true;
                this._startRapidPoll();
            }
            // ── Strategy 2: state machine armed, now at standby → complete ──
            else if (feedState === 'standby' && this._feedingInProgress) {
                this.log('Poll: completing armed feeding cycle at standby');
                triggerFlows = true;
                this._stopRapidPoll();
            }
            // ── Strategy 3: entirely missed cycle → feed_report changed ─────
            else if (feedState === 'standby'
                     && feedReport != null && feedReport > 0
                     && this._lastPolledFeedReport != null   // skip very first poll
                     && feedReport !== this._lastPolledFeedReport) {
                this.log(`Poll detected missed external feed: feed_report ${this._lastPolledFeedReport} → ${feedReport}`);
                // Simulate arm so state machine will count at standby
                this._feedingInProgress = true;
                this._doneFired = false;
                triggerFlows = true;
            }
            // ── Normal standby, no changes ───────────────────────────────────
            else {
                if (this._rapidPollActive) this._stopRapidPoll();
            }

            this._lastPolledFeedReport = feedReport;
            this.updateCapabilities(freshStatus, triggerFlows);
        } catch (err) {
            this.error('Failed to poll device status:', err);
        }
    }

    /**
     * Add portions to today's daily total (persistent across restarts).
     * Resets automatically at midnight.
     */
    _addToDailyTotal(portions) {
        const today = new Date().toISOString().slice(0, 10); // 'YYYY-MM-DD'
        const storedDate = this.getStoreValue('daily_feed_date');
        let dailyTotal = this.getStoreValue('daily_feed_total') || 0;

        if (storedDate !== today) {
            dailyTotal = 0;
            this.setStoreValue('daily_feed_date', today).catch(this.error);
        }

        dailyTotal += portions;
        this.setStoreValue('daily_feed_total', dailyTotal).catch(this.error);
        this.log(`Feeding completed: +${portions} portion(s), daily total: ${dailyTotal}`);
        return dailyTotal;
    }

    /**
     * Returns current daily total without modifying it.
     * Resets to 0 if it's a new day.
     */
    _getDailyTotal() {
        const today = new Date().toISOString().slice(0, 10);
        const storedDate = this.getStoreValue('daily_feed_date');
        if (storedDate !== today) {
            this.setStoreValue('daily_feed_date', today).catch(this.error);
            this.setStoreValue('daily_feed_total', 0).catch(this.error);
            return 0;
        }
        return this.getStoreValue('daily_feed_total') || 0;
    }

    setDeviceConfig(deviceConfig) {
        if (deviceConfig != null) {
            this.log('set petfeeder device config: ' + JSON.stringify(deviceConfig));
            let statusArr = deviceConfig.status ? deviceConfig.status : [];
            this.updateCapabilities(statusArr, false);
        } else {
            this.homey.log('No device config found for pet feeder');
        }
    }

    _onMultipleCapabilityListener(valueObj, optsObj) {
        this.log('Pet Feeder capabilities changed by Homey: ' + JSON.stringify(valueObj));
        try {
            if (valueObj.petfeeder_voice_times != null) {
                this.sendCommand('voice_times', valueObj.petfeeder_voice_times);
            }
        } catch (ex) {
            this.homey.app.logToHomey(ex);
        }
    }

    // Called by polling and MQTT messages
    updateCapabilities(statusArr, triggerFlows = true) {
        this.log('Update pet feeder capabilities from Tuya: ' + JSON.stringify(statusArr));
        if (!statusArr) return;

        // Build a map so we can process feed_state + feed_report together
        const statusMap = {};
        for (const s of statusArr) {
            statusMap[s.code] = s.value;
        }

        // ── feed_report: always update raw tracker ───────────────────────────
        if ('feed_report' in statusMap) {
            this._lastFeedReport = statusMap['feed_report'];
        }

        // ── feed_state capability display (always, polls + MQTT) ─────────────
        if ('feed_state' in statusMap) {
            this.normalAsync('petfeeder_feed_state', statusMap['feed_state']);
        }

        // ── feed_state state machine (triggerFlows=true from MQTT or smart poll) ─
        // Cycle:  feeding → (done) → standby
        //         ↑ arm     ↑ early  ↑ count + trigger (guaranteed)
        if ('feed_state' in statusMap && triggerFlows) {
            const newState = statusMap['feed_state'];

            this.driver.triggerFeedStateChanged(this, { feed_state: newState }, {});

            // Arm: feeding started → reset flags
            if (newState === 'feeding' && this._lastFeedState !== 'feeding') {
                this._feedingInProgress = true;
                this._doneFired = false;
                this.log('Feeding cycle started');
            }

            // 'done' arrives (optional — may be skipped if cycle is very fast)
            // Fire "voeren voltooid" early, mark as fired so standby won't double-fire
            if (newState === 'done' && !this._doneFired) {
                this.log('feed_state=done: firing triggerFeedingDone (early)');
                this.driver.triggerFeedingDone(this, {}, {});
                this._doneFired = true;
            }

            // Count at standby: feed_report is guaranteed final by now
            if (newState === 'standby' && this._feedingInProgress) {
                const portions = this._lastFeedReport || 0;
                if (portions > 0) {
                    const daily = this._addToDailyTotal(portions);
                    this.log(`Feeding complete at standby: +${portions} portion(s), daily total: ${daily}`);
                    this.normalAsync('measure_feed_report', daily);
                }
                // If done was missed, still fire the trigger now
                if (!this._doneFired) {
                    this.log('feed_state=standby: firing triggerFeedingDone (done was missed)');
                    this.driver.triggerFeedingDone(this, {}, {});
                }
                this._feedingInProgress = false;
                this._doneFired = false;
            }

            // Track last known state for transition detection
            this._lastFeedState = newState;
        }

        // Show current daily total
        this.normalAsync('measure_feed_report', this._getDailyTotal());

        // ── All other DPs ────────────────────────────────────────────────────
        for (const [code, value] of Object.entries(statusMap)) {
            switch (code) {
                case 'feed_state':
                case 'feed_report':
                    break; // handled above

                case 'battery_percentage':
                    this.normalAsync('measure_battery', value);
                    this.setCapabilityValue('alarm_battery', value < BATTERY_LOW_THRESHOLD).catch(this.error);
                    break;

                case 'voice_times':
                    this.normalAsync('petfeeder_voice_times', value);
                    break;

                case 'meal_plan':
                    this.normalAsync('petfeeder_meal_plan', String(value));
                    break;

                case 'quick_feed':
                    this.normalAsync('quick_feed', value);
                    break;

                case 'manual_feed':
                    this.log(`manual_feed status: ${value} portion(s)`);
                    break;

                default:
                    this.log('Unknown pet feeder DP: ' + code + ' = ' + value);
                    break;
            }
        }
    }


    normalAsync(name, value) {
        this.log(`Set pet feeder capability ${name} with ${value}`);
        if (!this.hasCapability(name)) {
            this.log(`Skipping setCapabilityValue for '${name}': capability not registered on device`);
            return;
        }
        this.setCapabilityValue(name, value).catch(error => this.error(error));
    }

    sendCommand(code, value) {
        const param = {
            commands: [{ code, value }]
        };
        this.homey.app.tuyaOpenApi.sendCommand(this.id, param).catch((error) => {
            this.error('[SET][%s] capabilities Error: %s', this.id, error);
            throw new Error(`Error sending command: ${error}`);
        });
    }

    // ── Public command helpers (called by driver flow listeners) ──────────────

    quickFeed() {
        this.log('Pet feeder: quick feed triggered');
        // Arm state machine immediately — MQTT may miss the 'feeding' state
        this._feedingInProgress = true;
        this._doneFired = false;
        this.sendCommand('quick_feed', true);
        // Start rapid polling to catch the full feeding cycle
        this._startRapidPoll();
    }

    manualFeed(portions) {
        const safe = Math.min(50, Math.max(1, Math.round(portions)));
        this.log(`Pet feeder: manual feed ${safe} portion(s)`);
        // Arm state machine immediately — MQTT may miss the 'feeding' state
        this._feedingInProgress = true;
        this._doneFired = false;
        this.sendCommand('manual_feed', safe);
        // Start rapid polling to catch the full feeding cycle
        this._startRapidPoll();
    }

    setVoiceTimes(times) {
        const safe = Math.min(5, Math.max(0, Math.round(times)));
        this.log(`Pet feeder: set voice_times to ${safe}`);
        this.sendCommand('voice_times', safe);
        if (this.hasCapability('petfeeder_voice_times')) {
            this.normalAsync('petfeeder_voice_times', safe);
        }
    }

    /**
     * Send a raw meal_plan payload.
     * @param {string} planJson  JSON string representing the meal plan object.
     */
    setMealPlan(planJson) {
        let plan;
        try {
            plan = JSON.parse(planJson);
        } catch (e) {
            throw new Error('Invalid meal plan JSON: ' + e.message);
        }
        this.log('Pet feeder: set meal_plan ' + JSON.stringify(plan));
        this.sendCommand('meal_plan', plan);
    }
}

module.exports = TuyaPetFeederDevice;

