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

    func testManualQoS1FutureFromAnotherEventLoopIsHoppedBeforeAcknowledgement() throws {
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

    func testShutdownClearsManualQoS1HandlerAndPreventsReinstallation() throws {
        let loop = EmbeddedEventLoop()
        let client = self.makeClient(loop: loop)
        let handler: MQTTManualQoS1AcknowledgementHandler = { _, _ in loop.makeSucceededVoidFuture() }
        client.setManualQoS1AcknowledgementHandler(handler)
        XCTAssertNotNil(client.manualQoS1AcknowledgementHandler)

        XCTAssertNoThrow(try client.syncShutdownGracefully())
        XCTAssertNil(client.manualQoS1AcknowledgementHandler)

        client.setManualQoS1AcknowledgementHandler(handler)
        XCTAssertNil(client.manualQoS1AcknowledgementHandler)
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
        let taskHandler = MQTTTaskHandler(client: client)
        let channel = EmbeddedChannel(handler: MQTTMessageHandler(client, pingInterval: .seconds(60)), loop: loop)
        client.connection = MQTTConnection(channel: channel, cleanSession: true, timeout: nil, taskHandler: taskHandler)
        try channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 1883)).wait()
        return channel
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
