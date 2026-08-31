# RainbowCare MQTTNIO fork

This fork is based exactly on MQTTNIO 2.13.0 (`91d3578`). Its sole intentional
behavioral extension is opt-in, completion-controlled PUBACK for inbound QoS 1
publishes through `setManualQoS1AcknowledgementHandler(_:limits:)`. With no manual
handler installed, upstream 2.13.0 behavior is unchanged.

Manual mode must be installed before connecting and is immutable for the life of
the connection. MQTTNIO requires a stable, non-empty client identifier and a
persistent broker session (`cleanSession: false` for MQTT 3.1.1). It bounds
concurrent durable admissions, applies socket auto-read backpressure, and emits
PUBACKs in PUBLISH receive order as required by MQTT-4.6.0-2. Admission or PUBACK
write failure closes the connection and suppresses later acknowledgements so the
broker can redeliver the unacknowledged suffix.

Keep the fork delta limited to this API, its tests, and necessary compatibility
updates. Apply upstream maintenance deliberately against the 2.13.x base and run
the complete test suite after every rebase or dependency update.
