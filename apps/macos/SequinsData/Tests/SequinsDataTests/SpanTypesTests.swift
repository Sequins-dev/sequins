import XCTest
@testable import SequinsData

final class SpanTypesTests: XCTestCase {
    func testSpanStatus() {
        XCTAssertEqual(SpanStatus.unset.rawValue, 0)
        XCTAssertEqual(SpanStatus.ok.rawValue, 1)
        XCTAssertEqual(SpanStatus.error.rawValue, 2)
    }

    func testSpanKind() {
        XCTAssertEqual(SpanKind.unspecified.rawValue, 0)
        XCTAssertEqual(SpanKind.internal.rawValue, 1)
        XCTAssertEqual(SpanKind.server.rawValue, 2)
        XCTAssertEqual(SpanKind.client.rawValue, 3)
        XCTAssertEqual(SpanKind.producer.rawValue, 4)
        XCTAssertEqual(SpanKind.consumer.rawValue, 5)
    }

    func testAttributeValue() {
        let stringValue = AttributeValue.string("test")
        if case .string(let value) = stringValue {
            XCTAssertEqual(value, "test")
        } else {
            XCTFail("Expected string attribute")
        }

        let intValue = AttributeValue.int(42)
        if case .int(let value) = intValue {
            XCTAssertEqual(value, 42)
        } else {
            XCTFail("Expected int attribute")
        }

        let boolValue = AttributeValue.bool(true)
        if case .bool(let value) = boolValue {
            XCTAssertEqual(value, true)
        } else {
            XCTFail("Expected bool attribute")
        }

        let doubleValue = AttributeValue.double(3.14)
        if case .double(let value) = doubleValue {
            XCTAssertEqual(value, 3.14, accuracy: 0.001)
        } else {
            XCTFail("Expected double attribute")
        }
    }

    func testSpanEventCreation() {
        let timestamp = Date()
        let attributes: [String: AttributeValue] = [
            "key": .string("value"),
            "count": .int(42)
        ]

        let event = SpanEvent(
            timestamp: timestamp,
            name: "test_event",
            attributes: attributes
        )

        XCTAssertEqual(event.name, "test_event")
        XCTAssertEqual(event.attributes.count, 2)
        XCTAssertNotNil(event.attributes["key"])
        XCTAssertNotNil(event.attributes["count"])
    }

    func testSpanCreation() {
        let traceId = "0102030405060708090a0b0c0d0e0f10"
        let spanId = "0102030405060708"
        let parentSpanId = "0000000000000001"

        let startTime = Date()
        let endTime = startTime.addingTimeInterval(0.1)

        let attributes: [String: AttributeValue] = [
            "http.method": .string("GET"),
            "http.status_code": .int(200)
        ]

        let event = SpanEvent(
            timestamp: startTime,
            name: "request_received",
            attributes: [:]
        )

        let span = Span(
            traceId: traceId,
            spanId: spanId,
            parentSpanId: parentSpanId,
            serviceName: "test-service",
            operationName: "GET /api/test",
            startTime: startTime,
            endTime: endTime,
            duration: 0.1,
            attributes: attributes,
            events: [event],
            status: .ok,
            spanKind: .server
        )

        XCTAssertEqual(span.traceId, traceId)
        XCTAssertEqual(span.spanId, spanId)
        XCTAssertEqual(span.parentSpanId, parentSpanId)
        XCTAssertEqual(span.serviceName, "test-service")
        XCTAssertEqual(span.operationName, "GET /api/test")
        XCTAssertEqual(span.status, .ok)
        XCTAssertEqual(span.spanKind, .server)
        XCTAssertEqual(span.events.count, 1)
        XCTAssertEqual(span.attributes.count, 2)
    }
}
