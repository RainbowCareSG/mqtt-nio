# RainbowCare MQTTNIO fork

This fork is based exactly on MQTTNIO 2.13.0 (`91d3578`). Its sole intentional
behavioral extension is opt-in, completion-controlled PUBACK for inbound QoS 1
publishes through `setManualQoS1AcknowledgementHandler(_:)`. With no manual
handler installed, upstream 2.13.0 behavior is unchanged.

Keep the fork delta limited to this API, its tests, and necessary compatibility
updates. Apply upstream maintenance deliberately against the 2.13.x base and run
the complete test suite after every rebase or dependency update.
