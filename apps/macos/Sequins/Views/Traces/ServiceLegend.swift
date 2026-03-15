import SequinsData
import SwiftUI

/// Service color palette for consistent span coloring
enum ServiceColorPalette {
    static let colors: [Color] = [
        Color(red: 0.231, green: 0.510, blue: 0.965), // Blue #3B82F6
        Color(red: 0.133, green: 0.773, blue: 0.369), // Green #22C55E
        Color(red: 0.976, green: 0.451, blue: 0.086), // Orange #F97316
        Color(red: 0.659, green: 0.333, blue: 0.969), // Purple #A855F7
        Color(red: 0.078, green: 0.722, blue: 0.651), // Teal #14B8A6
        Color(red: 0.925, green: 0.282, blue: 0.600), // Pink #EC4899
        Color(red: 0.918, green: 0.702, blue: 0.031), // Yellow #EAB308
        Color(red: 0.024, green: 0.714, blue: 0.831), // Cyan #06B6D4
    ]

    static let errorBorderColor = Color(red: 0.937, green: 0.267, blue: 0.267) // Red #EF4444

    static func color(for index: Int) -> Color {
        colors[index % colors.count]
    }
}

/// Maps service names to consistent colors
struct ServiceColorMapper {
    private var serviceToIndex: [String: Int] = [:]
    private var nextIndex = 0

    mutating func color(for serviceName: String) -> Color {
        if let index = serviceToIndex[serviceName] {
            return ServiceColorPalette.color(for: index)
        }
        let index = nextIndex
        serviceToIndex[serviceName] = index
        nextIndex += 1
        return ServiceColorPalette.color(for: index)
    }

    /// Get all service-color mappings
    var mappings: [(service: String, color: Color)] {
        serviceToIndex.sorted { $0.value < $1.value }
            .map { (service: $0.key, color: ServiceColorPalette.color(for: $0.value)) }
    }
}

/// Legend showing service colors and span counts
struct ServiceLegend: View {
    let spans: [Span]

    private var serviceInfo: [(service: String, color: Color, count: Int)] {
        var colorMapper = ServiceColorMapper()
        var counts: [String: Int] = [:]

        // Count spans per service and assign colors
        for span in spans {
            counts[span.serviceName, default: 0] += 1
            _ = colorMapper.color(for: span.serviceName)
        }

        return colorMapper.mappings.map { mapping in
            (service: mapping.service, color: mapping.color, count: counts[mapping.service] ?? 0)
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Services")
                .font(.headline)
                .foregroundColor(.secondary)

            ForEach(serviceInfo, id: \.service) { info in
                HStack(spacing: 8) {
                    RoundedRectangle(cornerRadius: 2)
                        .fill(info.color)
                        .frame(width: 12, height: 12)

                    Text(info.service)
                        .font(.caption)
                        .lineLimit(1)

                    Spacer()

                    Text("\(info.count) span\(info.count == 1 ? "" : "s")")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }
        }
        .padding()
        .background(Color(NSColor.controlBackgroundColor).opacity(0.5))
        .cornerRadius(8)
    }
}

#Preview("ServiceLegend") {
    let traceId = "abc123def456789012345678901234ab"
    let rootSpanId = "1234567890abcdef"
    let baseTime = Date().addingTimeInterval(-120)

    let sampleSpans = [
        Span(
            traceId: traceId,
            spanId: rootSpanId,
            parentSpanId: nil,
            serviceName: "api-gateway",
            operationName: "GET /api/users",
            startTime: baseTime,
            endTime: baseTime.addingTimeInterval(0.5),
            duration: 0.5,
            attributes: [:],
            events: [],
            status: .ok,
            spanKind: .server
        ),
        Span(
            traceId: traceId,
            spanId: "2234567890abcdef",
            parentSpanId: rootSpanId,
            serviceName: "user-service",
            operationName: "database.query",
            startTime: baseTime.addingTimeInterval(0.05),
            endTime: baseTime.addingTimeInterval(0.35),
            duration: 0.3,
            attributes: [:],
            events: [],
            status: .ok,
            spanKind: .client
        ),
        Span(
            traceId: traceId,
            spanId: "3234567890abcdef",
            parentSpanId: rootSpanId,
            serviceName: "user-service",
            operationName: "cache.get",
            startTime: baseTime.addingTimeInterval(0.36),
            endTime: baseTime.addingTimeInterval(0.38),
            duration: 0.02,
            attributes: [:],
            events: [],
            status: .ok,
            spanKind: .internal
        ),
        Span(
            traceId: traceId,
            spanId: "4234567890abcdef",
            parentSpanId: rootSpanId,
            serviceName: "auth-service",
            operationName: "validate.token",
            startTime: baseTime.addingTimeInterval(0.01),
            endTime: baseTime.addingTimeInterval(0.04),
            duration: 0.03,
            attributes: [:],
            events: [],
            status: .ok,
            spanKind: .server
        ),
    ]

    return ServiceLegend(spans: sampleSpans)
        .frame(width: 250)
        .padding()
}
