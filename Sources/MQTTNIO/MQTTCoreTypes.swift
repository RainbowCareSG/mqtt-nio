//===----------------------------------------------------------------------===//
//
// This source file is part of the MQTTNIO project
//
// Copyright (c) 2020-2021 Adam Fowler
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIOCore

/// Indicates the level of assurance for delivery of a packet.
public enum MQTTQoS: UInt8, Sendable {
    /// fire and forget
    case atMostOnce = 0
    /// wait for PUBACK, if you don't receive it after a period of time retry sending
    case atLeastOnce = 1
    /// wait for PUBREC, send PUBREL and then wait for PUBCOMP
    case exactlyOnce = 2
}

/// MQTT Packet type enumeration
public enum MQTTPacketType: UInt8, Sendable {
    case CONNECT = 0x10
    case CONNACK = 0x20
    case PUBLISH = 0x30
    case PUBACK = 0x40
    case PUBREC = 0x50
    case PUBREL = 0x62
    case PUBCOMP = 0x70
    case SUBSCRIBE = 0x82
    case SUBACK = 0x90
    case UNSUBSCRIBE = 0xA2
    case UNSUBACK = 0xB0
    case PINGREQ = 0xC0
    case PINGRESP = 0xD0
    case DISCONNECT = 0xE0
    case AUTH = 0xF0
}

/// MQTT PUBLISH packet parameters.
public struct MQTTPublishInfo: Sendable {
    /// Quality of Service for message.
    public let qos: MQTTQoS

    /// Whether this is a retained message.
    public let retain: Bool

    /// Whether this is a duplicate publish message.
    public let dup: Bool

    /// Topic name on which the message is published.
    public let topicName: String

    /// MQTT v5 properties
    public let properties: MQTTProperties

    /// Message payload.
    public let payload: ByteBuffer

    public init(
        qos: MQTTQoS,
        retain: Bool,
        dup: Bool = false,
        topicName: String,
        payload: ByteBuffer,
        properties: MQTTProperties
    ) {
        self.qos = qos
        self.retain = retain
        self.dup = dup
        self.topicName = topicName
        self.payload = payload
        self.properties = properties
    }

    static let emptyByteBuffer = ByteBufferAllocator().buffer(capacity: 0)
}

/// Handles an inbound QoS 1 publish whose PUBACK is controlled by the application.
///
/// Return a future that succeeds only after `publish` has been durably accepted. The
/// future may belong to any event loop. MQTTNIO sends PUBACK after it succeeds and in
/// the same order as the corresponding PUBLISH packets were received. If admission or
/// PUBACK writing fails, MQTTNIO closes the connection, suppresses all later PUBACKs,
/// and relies on the required persistent broker session for redelivery.
///
/// The handler is called on the connection's event loop and must return promptly. Do
/// not block while doing durable work; represent that work with the returned future.
/// Install it before connecting with a stable client identifier and persistent session.
public typealias MQTTManualQoS1AcknowledgementHandler =
    @Sendable (
        _ packetIdentifier: UInt16,
        _ publish: MQTTPublishInfo
    ) -> EventLoopFuture<Void>

/// Bounds the number of inbound QoS 1 publishes waiting for durable admission.
///
/// MQTTNIO pauses socket auto-read when `maximumPending` publishes are pending and
/// resumes it after the ordered queue drains to `resumePendingAt` or fewer. A publish
/// that was already decoded after the maximum was reached fails the connection closed
/// without PUBACK.
public struct MQTTManualQoS1AcknowledgementLimits: Equatable, Sendable {
    /// Maximum number of publishes admitted concurrently. Must be greater than zero.
    public let maximumPending: Int
    /// Pending count at or below which socket auto-read resumes. Must be non-negative
    /// and less than `maximumPending`.
    public let resumePendingAt: Int

    public init(maximumPending: Int = 128, resumePendingAt: Int = 64) {
        self.maximumPending = maximumPending
        self.resumePendingAt = resumePendingAt
    }
}

/// Errors configuring or operating completion-controlled inbound QoS 1 acknowledgement.
public enum MQTTManualQoS1AcknowledgementError: Error, Equatable, Sendable {
    /// The client has begun shutting down.
    case clientShutdown
    /// A connection attempt or live connection already owns the acknowledgement mode.
    case connectionActive
    /// Manual acknowledgement can only be cleared by shutting down the client.
    case handlerRemovalRequiresShutdown
    /// `maximumPending` and `resumePendingAt` do not form a valid high/low watermark.
    case invalidPendingLimits
    /// Close-without-PUBACK is only durable when the broker session persists.
    case persistentSessionRequired
    /// Durable broker sessions require a stable, non-empty client identifier.
    case stableClientIdentifierRequired
    /// More publishes were decoded than the configured admission window can hold.
    case pendingAdmissionLimitExceeded(Int)
}

/// MQTT SUBSCRIBE packet parameters.
public struct MQTTSubscribeInfo: Sendable {
    /// Topic filter to subscribe to.
    public let topicFilter: String

    /// Quality of Service for subscription.
    public let qos: MQTTQoS

    public init(topicFilter: String, qos: MQTTQoS) {
        self.qos = qos
        self.topicFilter = topicFilter
    }
}

/// MQTT Sub ACK
///
/// Contains data returned in subscribe ack packets
public struct MQTTSuback: Sendable {
    public enum ReturnCode: UInt8, Sendable {
        case grantedQoS0 = 0
        case grantedQoS1 = 1
        case grantedQoS2 = 2
        case failure = 0x80
    }

    /// MQTT v5 subscribe return codes
    public let returnCodes: [ReturnCode]

    init(returnCodes: [MQTTSuback.ReturnCode]) {
        self.returnCodes = returnCodes
    }
}
