//
//  HealthSummaryCard.swift
//  Sequins
//
//  Overall health status summary card
//

import SwiftUI
import SequinsData

/// Card displaying overall health status and score
struct HealthSummaryCard: View {
    let analysis: HealthAnalysis

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            // Header with status
            HStack(spacing: 12) {
                // Status icon
                Image(systemName: analysis.status.iconName)
                    .font(.system(size: 32))
                    .foregroundStyle(analysis.status.color)

                VStack(alignment: .leading, spacing: 4) {
                    Text("Overall Health")
                        .font(.headline)
                        .foregroundStyle(.secondary)

                    Text(analysis.status.label)
                        .font(.title2)
                        .fontWeight(.semibold)
                        .foregroundStyle(analysis.status.color)
                }

                Spacer()

                // Overall score
                VStack(alignment: .trailing, spacing: 4) {
                    Text("Score")
                        .font(.caption)
                        .foregroundStyle(.secondary)

                    Text("\(Int(analysis.overallScore * 100))%")
                        .font(.system(.title, design: .monospaced))
                        .fontWeight(.bold)
                        .foregroundStyle(analysis.status.color)
                }
            }

            // Status message
            Text(statusMessage)
                .font(.subheadline)
                .foregroundStyle(.secondary)

            // Data availability and timestamp
            HStack {
                // Data availability indicator
                if !analysis.hasCompleteData && analysis.hasAnyData {
                    let availability = analysis.dataAvailability
                    HStack(spacing: 4) {
                        Image(systemName: "exclamationmark.circle")
                            .font(.caption)
                            .foregroundStyle(.orange)
                        Text("\(availability.available)/\(availability.total) metrics")
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                    .help("Some configured health metrics are missing data")
                }

                Spacer()

                // Timestamp
                HStack {
                    Text("Last updated:")
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                    Text(analysis.timestamp, style: .time)
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                }
            }
        }
        .padding()
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(Color(nsColor: .controlBackgroundColor))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 12)
                .strokeBorder(analysis.status.color.opacity(0.3), lineWidth: 1)
        )
    }

    private var statusMessage: String {
        // Check for partial data first
        let partialSuffix = analysis.hasCompleteData ? "" : " (partial data)"

        switch analysis.status {
        case .healthy:
            return "All available health indicators are within normal parameters." + partialSuffix
        case .degraded:
            if let worstFactor = analysis.availableFactors.min(by: { $0.score < $1.score }) {
                return "Warning: \(worstFactor.displayName) is elevated at \(worstFactor.formattedValue)." + partialSuffix
            }
            return "Some health indicators are showing warning levels." + partialSuffix
        case .unhealthy:
            if let worstFactor = analysis.availableFactors.min(by: { $0.score < $1.score }) {
                return "Critical: \(worstFactor.displayName) is at \(worstFactor.formattedValue)." + partialSuffix
            }
            return "One or more health indicators are in critical range." + partialSuffix
        case .inactive:
            if analysis.factors.isEmpty {
                return "No health rules configured for this service."
            } else if analysis.missingFactors.count == analysis.factors.count {
                return "No health data available for the selected time range."
            }
            return "Waiting for health data..."
        }
    }
}

#Preview {
    VStack(spacing: 20) {
        HealthSummaryCard(analysis: HealthAnalysis(
            status: .healthy,
            factors: [
                HealthFactor(
                    metricName: "sequins.health.span_error_rate",
                    displayName: "Span Error Rate",
                    rawValue: 0.01,
                    formattedValue: "1.0%",
                    score: 0.95,
                    status: .healthy,
                    weight: 0.4
                )
            ],
            overallScore: 0.92,
            timestamp: Date(),
            serviceName: "my-service"
        ))

        HealthSummaryCard(analysis: HealthAnalysis(
            status: .degraded,
            factors: [
                HealthFactor(
                    metricName: "sequins.health.http_error_rate",
                    displayName: "HTTP Error Rate",
                    rawValue: 0.12,
                    formattedValue: "12.0%",
                    score: 0.55,
                    status: .degraded,
                    weight: 0.25
                )
            ],
            overallScore: 0.68,
            timestamp: Date(),
            serviceName: "my-service"
        ))

        HealthSummaryCard(analysis: HealthAnalysis(
            status: .unhealthy,
            factors: [
                HealthFactor(
                    metricName: "sequins.health.latency_p95",
                    displayName: "Latency (p95)",
                    rawValue: 850,
                    formattedValue: "850ms",
                    score: 0.2,
                    status: .unhealthy,
                    weight: 0.2
                )
            ],
            overallScore: 0.35,
            timestamp: Date(),
            serviceName: "my-service"
        ))

        HealthSummaryCard(analysis: .inactive(serviceName: "my-service"))
    }
    .padding()
    .frame(width: 400)
}
