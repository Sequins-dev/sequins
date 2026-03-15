//
//  HTTPStatusBreakdownCard.swift
//  Sequins
//
//  Card displaying HTTP response status code breakdown as a health indicator
//

import SwiftUI
import SequinsData

/// Card displaying HTTP response status code breakdown
struct HTTPStatusBreakdownCard: View {
    let healthMetricValues: [String: Double]

    /// HTTP 2xx success rate (0.0-1.0)
    private var http2xxRate: Double {
        healthMetricValues["sequins.health.http_2xx_rate"] ?? 0
    }

    /// HTTP 4xx client error rate (0.0-1.0)
    private var http4xxRate: Double {
        healthMetricValues["sequins.health.http_4xx_rate"] ?? 0
    }

    /// HTTP 5xx server error rate (0.0-1.0)
    private var http5xxRate: Double {
        healthMetricValues["sequins.health.http_5xx_rate"] ?? 0
    }

    /// Whether we have any HTTP data
    private var hasData: Bool {
        http2xxRate > 0 || http4xxRate > 0 || http5xxRate > 0
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Header and content with padding
            VStack(alignment: .leading, spacing: 12) {
                // Header
                HStack {
                    Image(systemName: "chart.pie")
                        .foregroundStyle(.secondary)
                    Text("HTTP Response Codes")
                        .font(.headline)
                    Spacer()
                }

                if hasData {
                    // Status breakdown
                    VStack(spacing: 8) {
                        HTTPStatusRow(
                            label: "2xx Success",
                            percentage: http2xxRate * 100,
                            color: .green
                        )
                        HTTPStatusRow(
                            label: "4xx Client Error",
                            percentage: http4xxRate * 100,
                            color: .yellow
                        )
                        HTTPStatusRow(
                            label: "5xx Server Error",
                            percentage: http5xxRate * 100,
                            color: .red
                        )
                    }
                } else {
                    // No data state
                    VStack(spacing: 8) {
                        Image(systemName: "globe")
                            .font(.title2)
                            .foregroundStyle(.tertiary)
                        Text("No HTTP traffic data")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 20)
                }
            }
            .padding()

            // Visual bar representation - full width
            if hasData {
                GeometryReader { geometry in
                    HStack(spacing: 0) {
                        if http2xxRate > 0 {
                            Rectangle()
                                .fill(Color.green)
                                .frame(width: geometry.size.width * http2xxRate)
                        }
                        if http4xxRate > 0 {
                            Rectangle()
                                .fill(Color.yellow)
                                .frame(width: geometry.size.width * http4xxRate)
                        }
                        if http5xxRate > 0 {
                            Rectangle()
                                .fill(Color.red)
                                .frame(width: geometry.size.width * http5xxRate)
                        }
                    }
                    .clipShape(RoundedRectangle(cornerRadius: 6))
                }
                .frame(height: 12)
                .padding(.horizontal)
                .padding(.bottom)
            }
        }
        .background(
            RoundedRectangle(cornerRadius: 8)
                .fill(Color(nsColor: .controlBackgroundColor))
        )
    }
}

/// Row displaying a single HTTP status category
struct HTTPStatusRow: View {
    let label: String
    let percentage: Double
    let color: Color

    var body: some View {
        HStack {
            Circle()
                .fill(color)
                .frame(width: 8, height: 8)

            Text(label)
                .font(.subheadline)

            Spacer()

            Text(String(format: "%.1f%%", percentage))
                .font(.system(.subheadline, design: .monospaced))
                .fontWeight(.medium)
                .foregroundStyle(percentage > 0 ? .primary : .secondary)
        }
    }
}

#Preview("HTTPStatusBreakdownCard - With Data") {
    HTTPStatusBreakdownCard(healthMetricValues: [
        "sequins.health.http_2xx_rate": 0.95,
        "sequins.health.http_4xx_rate": 0.03,
        "sequins.health.http_5xx_rate": 0.02
    ])
    .frame(width: 350)
    .padding()
}

#Preview("HTTPStatusBreakdownCard - High Errors") {
    HTTPStatusBreakdownCard(healthMetricValues: [
        "sequins.health.http_2xx_rate": 0.70,
        "sequins.health.http_4xx_rate": 0.15,
        "sequins.health.http_5xx_rate": 0.15
    ])
    .frame(width: 350)
    .padding()
}

#Preview("HTTPStatusBreakdownCard - No Data") {
    HTTPStatusBreakdownCard(healthMetricValues: [:])
    .frame(width: 350)
    .padding()
}
