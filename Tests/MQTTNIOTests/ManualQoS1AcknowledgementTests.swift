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
import XCTest

@testable import MQTTNIO

final class ManualQoS1AcknowledgementTests: XCTestCase {
    func testLegacyQoS1AcknowledgesBeforeNotifyingListener() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        var channel: EmbeddedChannel!
        var acknowledgementObservedByListener: ByteBuffer?
        client.addPublishListener(named: "test") { _ in
            acknowledgementObservedByListener = try? channel.readOutbound(as: ByteBuffer.self)
        }
        channel = try self.makeChannel(client: client, loop: loop)
        defer { self.shutDown(client: client, loops: [loop]) }

        try channel.writeInbound(self.publish(packetIdentifier: 0x1234))
        loop.run()

        XCTAssertEqual(
            acknowledgementObservedByListener.map { Array($0.readableBytesView) },
            [0x40, 0x02, 0x12, 0x34]
        )
    }

    func testManualQoS1SuccessDelaysAcknowledgementAndExposesPublish() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let acceptance = loop.makePromise(of: Void.self)
        let received = LockedBox<(UInt16, MQTTPublishInfo)?>(nil)
        var legacyListenerCalled = false
        client.addPublishListener(named: "legacy") { _ in legacyListenerCalled = true }
        client.setManualQoS1AcknowledgementHandler { packetIdentifier, publish in
            received.withValue { $0 = (packetIdentifier, publish) }
            return acceptance.futureResult
        }
        let channel = try self.makeChannel(client: client, loop: loop)
        defer { self.shutDown(client: client, loops: [loop]) }

        try channel.writeInbound(
            self.publish(
                packetIdentifier: 0xBEEF,
                topic: "devices/42",
                payload: "durable payload",
                duplicate: true
            )
        )
        loop.run()

        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
        let receivedPublish = try XCTUnwrap(received.withValue { $0 })
        XCTAssertEqual(receivedPublish.0, 0xBEEF)
        XCTAssertEqual(receivedPublish.1.topicName, "devices/42")
        XCTAssertEqual(receivedPublish.1.dup, true)
        XCTAssertEqual(receivedPublish.1.qos, .atLeastOnce)
        var payload = receivedPublish.1.payload
        XCTAssertEqual(payload.readString(length: payload.readableBytes), "durable payload")
        XCTAssertFalse(legacyListenerCalled)

        acceptance.succeed(())
        loop.run()

        XCTAssertEqual(try self.readAcknowledgement(from: channel), [0x40, 0x02, 0xBE, 0xEF])
        XCTAssertFalse(legacyListenerCalled)
    }

    func testManualQoS1FailureSendsNoAcknowledgementAndClosesConnection() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let acceptance = loop.makePromise(of: Void.self)
        client.setManualQoS1AcknowledgementHandler { _, _ in acceptance.futureResult }
        let channel = try self.makeChannel(client: client, loop: loop)
        defer { self.shutDown(client: client, loops: [loop]) }

        try channel.writeInbound(self.publish(packetIdentifier: 7))
        loop.run()
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))

        acceptance.fail(TestError.durableAcceptanceFailed)
        loop.run()

        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
        XCTAssertFalse(channel.isActive)
        XCTAssertThrowsError(try channel.throwIfErrorCaught()) { error in
            XCTAssertEqual(error as? TestError, .durableAcceptanceFailed)
        }
    }

    func testManualQoS1FutureFromAnotherEventLoopCompletesOnConnectionLoopBeforeAcknowledgement() throws {
        let connectionLoop = EmbeddedEventLoop()
        let handlerLoop = EmbeddedEventLoop()
        let client = self.makeClient(loop: connectionLoop)
        let acceptance = handlerLoop.makePromise(of: Void.self)
        client.setManualQoS1AcknowledgementHandler { _, _ in acceptance.futureResult }
        let channel = try self.makeChannel(client: client, loop: connectionLoop)
        defer { self.shutDown(client: client, loops: [connectionLoop, handlerLoop]) }

        try channel.writeInbound(self.publish(packetIdentifier: 99))
        connectionLoop.run()
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))

        acceptance.succeed(())
        handlerLoop.run()
        connectionLoop.run()
        XCTAssertEqual(try self.readAcknowledgement(from: channel), [0x40, 0x02, 0x00, 0x63])
    }

    func testReplacingManualQoS1HandlerUsesLatestHandler() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let calls = LockedBox<[String]>([])
        client.setManualQoS1AcknowledgementHandler { _, _ in
            calls.withValue { $0.append("first") }
            return loop.makeSucceededVoidFuture()
        }
        client.setManualQoS1AcknowledgementHandler { _, _ in
            calls.withValue { $0.append("second") }
            return loop.makeSucceededVoidFuture()
        }
        let channel = try self.makeChannel(client: client, loop: loop)
        defer { self.shutDown(client: client, loops: [loop]) }

        try channel.writeInbound(self.publish(packetIdentifier: 100))
        loop.run()

        XCTAssertEqual(calls.withValue { $0 }, ["second"])
        XCTAssertEqual(try self.readAcknowledgement(from: channel), [0x40, 0x02, 0x00, 0x64])
    }

    func testOutOfOrderAdmissionCompletionPreservesPublishAcknowledgementOrder() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let first = loop.makePromise(of: Void.self)
        let second = loop.makePromise(of: Void.self)
        let futures = LockedBox([first.futureResult, second.futureResult])
        client.setManualQoS1AcknowledgementHandler { _, _ in
            futures.withValue { $0.removeFirst() }
        }
        let channel = try self.makeChannel(client: client, loop: loop)
        defer { self.shutDown(client: client, loops: [loop]) }

        try channel.writeInbound(self.publish(packetIdentifier: 1))
        try channel.writeInbound(self.publish(packetIdentifier: 2))
        second.succeed(())
        loop.run()
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))

        first.succeed(())
        loop.run()
        XCTAssertEqual(try self.readAcknowledgement(from: channel), [0x40, 0x02, 0x00, 0x01])
        XCTAssertEqual(try self.readAcknowledgement(from: channel), [0x40, 0x02, 0x00, 0x02])
    }

    func testFailedHeadSuppressesLaterSuccessfulAcknowledgement() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let first = loop.makePromise(of: Void.self)
        let second = loop.makePromise(of: Void.self)
        let futures = LockedBox([first.futureResult, second.futureResult])
        client.setManualQoS1AcknowledgementHandler { _, _ in
            futures.withValue { $0.removeFirst() }
        }
        let channel = try self.makeChannel(client: client, loop: loop)
        defer { self.shutDown(client: client, loops: [loop]) }

        try channel.writeInbound(self.publish(packetIdentifier: 1))
        try channel.writeInbound(self.publish(packetIdentifier: 2))
        second.succeed(())
        first.fail(TestError.durableAcceptanceFailed)
        loop.run()

        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
        XCTAssertFalse(channel.isActive)
        XCTAssertThrowsError(try channel.throwIfErrorCaught()) { error in
            XCTAssertEqual(error as? TestError, .durableAcceptanceFailed)
        }
    }

    func testAcknowledgementWriteFailureClosesAndSuppressesLaterAcknowledgement() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let first = loop.makePromise(of: Void.self)
        let second = loop.makePromise(of: Void.self)
        let futures = LockedBox([first.futureResult, second.futureResult])
        client.setManualQoS1AcknowledgementHandler { _, _ in
            futures.withValue { $0.removeFirst() }
        }
        let messageHandler = MQTTMessageHandler(
            client,
            pingInterval: .seconds(60),
            manualQoS1AcknowledgementSettings: client.manualQoS1AcknowledgementSettings
        )
        let channel = EmbeddedChannel(
            handlers: [FailingOutboundHandler(), messageHandler],
            loop: loop
        )
        client.connection = MQTTConnection(
            channel: channel,
            cleanSession: false,
            timeout: nil,
            taskHandler: MQTTTaskHandler(client: client)
        )
        try channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 1883)).wait()
        defer { self.shutDown(client: client, loops: [loop]) }

        try channel.writeInbound(self.publish(packetIdentifier: 1))
        try channel.writeInbound(self.publish(packetIdentifier: 2))
        second.succeed(())
        first.succeed(())
        loop.run()

        XCTAssertFalse(channel.isActive)
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
        XCTAssertThrowsError(try channel.throwIfErrorCaught()) { error in
            XCTAssertEqual(error as? TestError, .acknowledgementWriteFailed)
        }
    }

    func testConnectedClientRejectsHandlerReplacementAndRemoval() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let originalCalled = LockedBox(false)
        let replacementCalled = LockedBox(false)
        let acceptance = loop.makePromise(of: Void.self)
        let original: MQTTManualQoS1AcknowledgementHandler = { _, _ in
            originalCalled.withValue { $0 = true }
            return acceptance.futureResult
        }
        XCTAssertNoThrow(try client.setManualQoS1AcknowledgementHandler(original).get())
        let channel = try self.makeChannel(client: client, loop: loop)
        defer { self.shutDown(client: client, loops: [loop]) }

        try channel.writeInbound(self.publish(packetIdentifier: 1))
        loop.run()
        XCTAssertTrue(originalCalled.withValue { $0 })
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))

        let replacement: MQTTManualQoS1AcknowledgementHandler = { _, _ in
            replacementCalled.withValue { $0 = true }
            return loop.makeSucceededVoidFuture()
        }
        XCTAssertThrowsError(try client.setManualQoS1AcknowledgementHandler(replacement).get()) { error in
            XCTAssertEqual(error as? MQTTManualQoS1AcknowledgementError, .connectionActive)
        }
        XCTAssertThrowsError(try client.setManualQoS1AcknowledgementHandler(nil).get()) { error in
            XCTAssertEqual(error as? MQTTManualQoS1AcknowledgementError, .handlerRemovalRequiresShutdown)
        }

        acceptance.succeed(())
        loop.run()
        XCTAssertFalse(replacementCalled.withValue { $0 })
        XCTAssertEqual(try self.readAcknowledgement(from: channel), [0x40, 0x02, 0x00, 0x01])
    }

    func testConnectionAttemptRejectsHandlerReplacement() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let handler: MQTTManualQoS1AcknowledgementHandler = { _, _ in
            loop.makeSucceededVoidFuture()
        }
        XCTAssertNoThrow(try client.setManualQoS1AcknowledgementHandler(handler).get())
        XCTAssertNoThrow(try client.prepareConnection(persistentSession: true).get())

        XCTAssertThrowsError(try client.setManualQoS1AcknowledgementHandler(handler).get()) { error in
            XCTAssertEqual(error as? MQTTManualQoS1AcknowledgementError, .connectionActive)
        }

        client.connectionAttemptDidFailBeforeCreatingChannel()
        XCTAssertNoThrow(try client.syncShutdownGracefully())
        XCTAssertNoThrow(try loop.syncShutdownGracefully())
    }

    func testInvalidPendingLimitsAreRejectedBeforeConnect() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let handler: MQTTManualQoS1AcknowledgementHandler = { _, _ in
            loop.makeSucceededVoidFuture()
        }

        for limits in [
            MQTTManualQoS1AcknowledgementLimits(maximumPending: 0, resumePendingAt: 0),
            MQTTManualQoS1AcknowledgementLimits(maximumPending: 2, resumePendingAt: -1),
            MQTTManualQoS1AcknowledgementLimits(maximumPending: 2, resumePendingAt: 2),
        ] {
            XCTAssertThrowsError(
                try client.setManualQoS1AcknowledgementHandler(handler, limits: limits).get()
            ) { error in
                XCTAssertEqual(error as? MQTTManualQoS1AcknowledgementError, .invalidPendingLimits)
            }
        }

        XCTAssertNoThrow(try client.syncShutdownGracefully())
        XCTAssertNoThrow(try loop.syncShutdownGracefully())
    }

    func testLateExternalCompletionAfterShutdownDoesNothing() throws {
        let connectionLoop = EmbeddedEventLoop()
        let handlerLoop = EmbeddedEventLoop()
        let client = self.makeClient(loop: connectionLoop)
        let acceptance = handlerLoop.makePromise(of: Void.self)
        client.setManualQoS1AcknowledgementHandler { _, _ in acceptance.futureResult }
        let channel = try self.makeChannel(client: client, loop: connectionLoop)

        try channel.writeInbound(self.publish(packetIdentifier: 1))
        connectionLoop.run()
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
        XCTAssertNoThrow(try client.syncShutdownGracefully())

        acceptance.succeed(())
        handlerLoop.run()
        connectionLoop.run()
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
        XCTAssertNoThrow(try connectionLoop.syncShutdownGracefully())
        XCTAssertNoThrow(try handlerLoop.syncShutdownGracefully())
    }

    func testDisconnectFencesPendingAdmissionBeforeSendingDisconnect() throws {
        let connectionLoop = EmbeddedEventLoop()
        let handlerLoop = EmbeddedEventLoop()
        let client = self.makeClient(loop: connectionLoop)
        let acceptance = handlerLoop.makePromise(of: Void.self)
        client.setManualQoS1AcknowledgementHandler { _, _ in acceptance.futureResult }
        let channel = try self.makeChannel(client: client, loop: connectionLoop)

        try channel.writeInbound(self.publish(packetIdentifier: 1))
        connectionLoop.run()
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))

        try client.disconnect().wait()
        connectionLoop.run()
        XCTAssertEqual(
            try channel.readOutbound(as: ByteBuffer.self).map { Array($0.readableBytesView) },
            [0xE0, 0x00]
        )

        acceptance.succeed(())
        handlerLoop.run()
        connectionLoop.run()
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))

        XCTAssertNoThrow(try client.syncShutdownGracefully())
        XCTAssertNoThrow(try connectionLoop.syncShutdownGracefully())
        XCTAssertNoThrow(try handlerLoop.syncShutdownGracefully())
    }

    func testPendingLimitsSuspendAndResumeOrderedAdmissions() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let first = loop.makePromise(of: Void.self)
        let second = loop.makePromise(of: Void.self)
        let futures = LockedBox([first.futureResult, second.futureResult])
        XCTAssertNoThrow(
            try client.setManualQoS1AcknowledgementHandler(
                { _, _ in futures.withValue { $0.removeFirst() } },
                limits: .init(maximumPending: 2, resumePendingAt: 1)
            ).get()
        )
        let (channel, messageHandler) = try self.makeChannelAndHandler(client: client, loop: loop)
        defer { self.shutDown(client: client, loops: [loop]) }

        try channel.writeInbound(self.publish(packetIdentifier: 1))
        try channel.writeInbound(self.publish(packetIdentifier: 2))
        loop.run()
        XCTAssertEqual(messageHandler.manualQoS1PendingCount, 2)
        XCTAssertTrue(messageHandler.manualQoS1ReadIsSuspended)

        first.succeed(())
        loop.run()
        XCTAssertEqual(try self.readAcknowledgement(from: channel), [0x40, 0x02, 0x00, 0x01])
        XCTAssertEqual(messageHandler.manualQoS1PendingCount, 1)
        XCTAssertFalse(messageHandler.manualQoS1ReadIsSuspended)

        second.succeed(())
        loop.run()
        XCTAssertEqual(try self.readAcknowledgement(from: channel), [0x40, 0x02, 0x00, 0x02])
    }

    func testAlreadyDecodedPublishBeyondPendingLimitFailsClosed() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let futures = LockedBox<[EventLoopFuture<Void>]>([
            loop.makePromise(of: Void.self).futureResult,
            loop.makePromise(of: Void.self).futureResult,
        ])
        XCTAssertNoThrow(
            try client.setManualQoS1AcknowledgementHandler(
                { _, _ in futures.withValue { $0.removeFirst() } },
                limits: .init(maximumPending: 2, resumePendingAt: 1)
            ).get()
        )
        let channel = try self.makeChannel(client: client, loop: loop)
        defer { self.shutDown(client: client, loops: [loop]) }

        var burst = try self.publish(packetIdentifier: 1)
        var second = try self.publish(packetIdentifier: 2)
        var overflow = try self.publish(packetIdentifier: 3)
        burst.writeBuffer(&second)
        burst.writeBuffer(&overflow)
        XCTAssertThrowsError(try channel.writeInbound(burst)) { error in
            XCTAssertEqual(
                error as? MQTTManualQoS1AcknowledgementError,
                .pendingAdmissionLimitExceeded(2)
            )
        }
        loop.run()

        XCTAssertFalse(channel.isActive)
        XCTAssertNil(try channel.readOutbound(as: ByteBuffer.self))
    }

    func testManualModeRequiresPersistentSessionAndStableIdentifier() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        client.setManualQoS1AcknowledgementHandler { _, _ in loop.makeSucceededVoidFuture() }
        defer { self.shutDown(client: client, loops: [loop]) }

        XCTAssertEqual(
            client.manualQoS1AcknowledgementValidationError(persistentSession: false),
            .persistentSessionRequired
        )
        XCTAssertNil(client.manualQoS1AcknowledgementValidationError(persistentSession: true))
        XCTAssertThrowsError(try client.connect(cleanSession: true).wait()) { error in
            XCTAssertEqual(error as? MQTTManualQoS1AcknowledgementError, .persistentSessionRequired)
        }

        let emptyIdentifierClient = MQTTClient(
            host: "127.0.0.1",
            port: 1883,
            identifier: "",
            eventLoopGroupProvider: .shared(loop),
            configuration: .init(disablePing: true, useWebSockets: false)
        )
        emptyIdentifierClient.setManualQoS1AcknowledgementHandler { _, _ in loop.makeSucceededVoidFuture() }
        XCTAssertEqual(
            emptyIdentifierClient.manualQoS1AcknowledgementValidationError(persistentSession: true),
            .stableClientIdentifierRequired
        )
        XCTAssertThrowsError(try emptyIdentifierClient.connect(cleanSession: false).wait()) { error in
            XCTAssertEqual(error as? MQTTManualQoS1AcknowledgementError, .stableClientIdentifierRequired)
        }
        XCTAssertNoThrow(try emptyIdentifierClient.syncShutdownGracefully())
    }

    func testShutdownClearsManualQoS1HandlerAndPreventsReinstallation() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let handler: MQTTManualQoS1AcknowledgementHandler = { _, _ in loop.makeSucceededVoidFuture() }
        XCTAssertNoThrow(try client.setManualQoS1AcknowledgementHandler(handler).get())
        XCTAssertNotNil(client.manualQoS1AcknowledgementSettings)

        XCTAssertNoThrow(try client.syncShutdownGracefully())
        XCTAssertNil(client.manualQoS1AcknowledgementSettings)

        XCTAssertThrowsError(try client.setManualQoS1AcknowledgementHandler(handler).get()) { error in
            XCTAssertEqual(error as? MQTTManualQoS1AcknowledgementError, .clientShutdown)
        }
        XCTAssertNil(client.manualQoS1AcknowledgementSettings)
        XCTAssertNoThrow(try loop.syncShutdownGracefully())
    }

    private func makeClient(loop: EmbeddedEventLoop) -> MQTTClient {
        MQTTClient(
            host: "127.0.0.1",
            port: 1883,
            identifier: "manual-qos1-ack-test",
            eventLoopGroupProvider: .shared(loop),
            configuration: .init(disablePing: true, useWebSockets: false)
        )
    }

    private func makeChannel(client: MQTTClient, loop: EmbeddedEventLoop) throws -> EmbeddedChannel {
        try self.makeChannelAndHandler(client: client, loop: loop).0
    }

    private func makeChannelAndHandler(
        client: MQTTClient,
        loop: EmbeddedEventLoop
    ) throws -> (EmbeddedChannel, MQTTMessageHandler) {
        let taskHandler = MQTTTaskHandler(client: client)
        let messageHandler = MQTTMessageHandler(
            client,
            pingInterval: .seconds(60),
            manualQoS1AcknowledgementSettings: client.manualQoS1AcknowledgementSettings
        )
        let channel = EmbeddedChannel(handler: messageHandler, loop: loop)
        client.connection = MQTTConnection(channel: channel, cleanSession: false, timeout: nil, taskHandler: taskHandler)
        try channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 1883)).wait()
        return (channel, messageHandler)
    }

    private func publish(
        packetIdentifier: UInt16,
        topic: String = "topic",
        payload: String = "payload",
        duplicate: Bool = false
    ) throws -> ByteBuffer {
        var buffer = ByteBufferAllocator().buffer(capacity: 64)
        let publish = MQTTPublishInfo(
            qos: .atLeastOnce,
            retain: false,
            dup: duplicate,
            topicName: topic,
            payload: ByteBufferAllocator().buffer(string: payload),
            properties: .init()
        )
        try MQTTPublishPacket(publish: publish, packetId: packetIdentifier)
            .write(version: .v3_1_1, to: &buffer)
        return buffer
    }

    private func readAcknowledgement(from channel: EmbeddedChannel) throws -> [UInt8]? {
        try channel.readOutbound(as: ByteBuffer.self).map { Array($0.readableBytesView) }
    }

    private func shutDown(client: MQTTClient, loops: [EmbeddedEventLoop], file: StaticString = #filePath, line: UInt = #line) {
        XCTAssertNoThrow(try client.syncShutdownGracefully(), file: file, line: line)
        for loop in loops {
            XCTAssertNoThrow(try loop.syncShutdownGracefully(), file: file, line: line)
        }
    }

    private enum TestError: Error, Equatable {
        case durableAcceptanceFailed
        case acknowledgementWriteFailed
    }

    private final class FailingOutboundHandler: ChannelOutboundHandler {
        typealias OutboundIn = ByteBuffer

        func write(
            context: ChannelHandlerContext,
            data: NIOAny,
            promise: EventLoopPromise<Void>?
        ) {
            promise?.fail(TestError.acknowledgementWriteFailed)
        }
    }

    private final class LockedBox<Value>: @unchecked Sendable {
        init(_ value: Value) {
            self.value = value
        }

        func withValue<Result>(_ body: (inout Value) -> Result) -> Result {
            self.lock.lock()
            defer { self.lock.unlock() }
            return body(&self.value)
        }

        private let lock = NSLock()
        private var value: Value
    }
}
