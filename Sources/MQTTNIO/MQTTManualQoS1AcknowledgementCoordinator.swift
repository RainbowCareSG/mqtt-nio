//===----------------------------------------------------------------------===//
//
// This source file is part of the MQTTNIO project
//
// Copyright (c) 2026 RainbowCare
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIO
import NIOConcurrencyHelpers

struct MQTTManualQoS1AcknowledgementSettings: Sendable {
    let handler: MQTTManualQoS1AcknowledgementHandler
    let limits: MQTTManualQoS1AcknowledgementLimits
}

/// Transfers completions from application-owned event loops without leaving a hop
/// promise attached to the connection event loop after its channel has gone away.
private final class MQTTManualQoS1CompletionGate: @unchecked Sendable {
    typealias Delivery = @Sendable (Int, Result<Void, Error>) -> Void

    func open(on eventLoop: EventLoop, delivery: @escaping Delivery) {
        self.lock.withLock {
            self.eventLoop = eventLoop
            self.delivery = delivery
        }
    }

    func deliver(sequence: Int, result: Result<Void, Error>) {
        self.lock.lock()
        guard let eventLoop = self.eventLoop, let delivery = self.delivery else {
            self.lock.unlock()
            return
        }

        if eventLoop.inEventLoop {
            self.lock.unlock()
            delivery(sequence, result)
        } else {
            // Scheduling and closing the gate are serialized. A callback already
            // queued before close is harmless because the coordinator is terminally
            // fenced on the connection event loop before the gate is closed.
            eventLoop.execute {
                delivery(sequence, result)
            }
            self.lock.unlock()
        }
    }

    func close() {
        self.lock.withLock {
            self.delivery = nil
            self.eventLoop = nil
        }
    }

    private let lock = NIOLock()
    private var eventLoop: EventLoop?
    private var delivery: Delivery?
}

/// All mutable state is confined to the channel event loop.
final class MQTTManualQoS1AcknowledgementCoordinator {
    private struct PendingAdmission {
        let sequence: Int
        let packetIdentifier: UInt16
        let topicName: String
        var result: Result<Void, Error>?
    }

    init(client: MQTTClient, settings: MQTTManualQoS1AcknowledgementSettings) {
        self.client = client
        self.settings = settings
    }

    func handlerAdded(context: ChannelHandlerContext) {
        context.eventLoop.assertInEventLoop()
        self.context = context
        self.completionGate.open(on: context.eventLoop) { [weak self] sequence, result in
            self?.admissionCompleted(sequence: sequence, result: result)
        }
    }

    func admit(packetIdentifier: UInt16, publish: MQTTPublishInfo, context: ChannelHandlerContext) {
        context.eventLoop.assertInEventLoop()
        guard !self.isTerminal else { return }

        guard self.pending.count < self.settings.limits.maximumPending else {
            self.failClosed(
                MQTTManualQoS1AcknowledgementError.pendingAdmissionLimitExceeded(
                    self.settings.limits.maximumPending
                )
            )
            return
        }

        let sequence = self.nextSequence
        self.nextSequence &+= 1
        self.pending.append(
            .init(
                sequence: sequence,
                packetIdentifier: packetIdentifier,
                topicName: publish.topicName,
                result: nil
            )
        )
        self.updateReadBackpressure()

        let future = self.settings.handler(packetIdentifier, publish)
        future.whenComplete { [completionGate = self.completionGate] result in
            completionGate.deliver(sequence: sequence, result: result)
        }
    }

    func stop(context: ChannelHandlerContext) {
        context.eventLoop.assertInEventLoop()
        guard !self.isTerminal else {
            self.completionGate.close()
            self.context = nil
            return
        }
        self.isTerminal = true
        self.pending.removeAll(keepingCapacity: false)
        self.writeInProgress = false
        self.completionGate.close()
        self.context = nil
    }

    var pendingCount: Int { self.pending.count }
    var readIsSuspended: Bool { self.autoReadIsSuspended }
    var terminal: Bool { self.isTerminal }

    private func admissionCompleted(sequence: Int, result: Result<Void, Error>) {
        guard let context = self.context else { return }
        context.eventLoop.assertInEventLoop()
        guard !self.isTerminal else { return }
        guard let index = self.pending.firstIndex(where: { $0.sequence == sequence }) else { return }
        self.pending[index].result = result
        self.drainCompletedHead()
    }

    /// MQTT-4.6.0-2 requires PUBACK order to match PUBLISH receive order. Only
    /// the completed head can write, and the next head waits for that write.
    private func drainCompletedHead() {
        guard let context = self.context else { return }
        context.eventLoop.assertInEventLoop()
        guard !self.isTerminal, !self.writeInProgress else { return }
        guard let head = self.pending.first, let result = head.result else { return }

        switch result {
        case .failure(let error):
            self.client.logger.error(
                "Manual QoS 1 acknowledgement failed; closing connection without PUBACK",
                metadata: [
                    "mqtt_error": .string("\(error)"),
                    "mqtt_packet_id": .stringConvertible(head.packetIdentifier),
                    "mqtt_topicName": .string(head.topicName),
                ]
            )
            self.failClosed(error)

        case .success:
            self.writeInProgress = true
            context.channel.writeAndFlush(
                MQTTPubAckPacket(type: .PUBACK, packetId: head.packetIdentifier)
            ).whenComplete { [weak self] writeResult in
                guard let self, let context = self.context else { return }
                context.eventLoop.assertInEventLoop()
                guard !self.isTerminal else { return }
                self.writeInProgress = false

                switch writeResult {
                case .success:
                    guard self.pending.first?.sequence == head.sequence else {
                        self.failClosed(MQTTError.unexpectedMessage)
                        return
                    }
                    self.pending.removeFirst()
                    self.updateReadBackpressure()
                    self.drainCompletedHead()
                case .failure(let error):
                    self.failClosed(error)
                }
            }
        }
    }

    private func updateReadBackpressure() {
        guard let context = self.context, !self.isTerminal else { return }
        context.eventLoop.assertInEventLoop()

        if !self.autoReadIsSuspended, self.pending.count >= self.settings.limits.maximumPending {
            self.autoReadIsSuspended = true
            context.channel.setOption(ChannelOptions.autoRead, value: false).whenFailure { [weak self] error in
                self?.failClosed(error)
            }
        } else if self.autoReadIsSuspended, self.pending.count <= self.settings.limits.resumePendingAt {
            self.autoReadIsSuspended = false
            context.channel.setOption(ChannelOptions.autoRead, value: true).whenFailure { [weak self] error in
                self?.failClosed(error)
            }
        }
    }

    private func failClosed(_ error: Error) {
        guard let context = self.context else { return }
        context.eventLoop.assertInEventLoop()
        guard !self.isTerminal else { return }

        self.isTerminal = true
        self.pending.removeAll(keepingCapacity: false)
        self.writeInProgress = false
        self.completionGate.close()
        context.fireErrorCaught(error)
        context.close(promise: nil)
    }

    private let client: MQTTClient
    private let settings: MQTTManualQoS1AcknowledgementSettings
    private let completionGate = MQTTManualQoS1CompletionGate()
    private weak var context: ChannelHandlerContext?
    private var pending = CircularBuffer<PendingAdmission>(initialCapacity: 16)
    private var nextSequence = 0
    private var writeInProgress = false
    private var isTerminal = false
    private var autoReadIsSuspended = false
}
