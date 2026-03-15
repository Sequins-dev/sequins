//
//  HealthFactorRow.swift
//  Sequins
//
//  Individual health factor display with score bar
//

import SwiftUI
import SequinsData

/// Row displaying a single health factor with score bar
struct HealthFactorRow: View {
    let factor: HealthFactor

    var body: some View {
        if factor.hasData {
            availableDataRow
        } else {
            missingDataRow
        }
    }

    /// Row for factors with available data
    private var availableDataRow: some View {
        HStack(spacing: 12) {
            // Factor name
            Text(factor.displayName)
                .font(.system(.body, design: .default))
                .foregroundStyle(.primary)
                .frame(minWidth: 120, alignment: .leading)

            // Score bar
            GeometryReader { geometry in
                ZStack(alignment: .leading) {
                    // Background track
                    RoundedRectangle(cornerRadius: 4)
                        .fill(Color.secondary.opacity(0.2))

                    // Filled portion
                    RoundedRectangle(cornerRadius: 4)
                        .fill(barColor)
                        .frame(width: max(0, geometry.size.width * factor.score))
                }
            }
            .frame(height: 8)

            // Score percentage
            Text("\(Int(factor.score * 100))%")
                .font(.system(.callout, design: .monospaced))
                .foregroundStyle(.secondary)
                .frame(width: 40, alignment: .trailing)

            // Status indicator
            Circle()
                .fill(factor.status.color)
                .frame(width: 10, height: 10)

            // Value
            Text(factor.formattedValue)
                .font(.system(.callout, design: .monospaced))
                .foregroundStyle(.secondary)
                .frame(width: 80, alignment: .trailing)
        }
        .padding(.vertical, 4)
    }

    /// Row for factors with missing data
    private var missingDataRow: some View {
        HStack(spacing: 12) {
            // Factor name with muted styling
            Text(factor.displayName)
                .font(.system(.body, design: .default))
                .foregroundStyle(.secondary)
                .frame(minWidth: 120, alignment: .leading)

            // Dashed placeholder bar
            GeometryReader { geometry in
                RoundedRectangle(cornerRadius: 4)
                    .strokeBorder(style: StrokeStyle(lineWidth: 1, dash: [4, 2]))
                    .foregroundStyle(Color.secondary.opacity(0.3))
            }
            .frame(height: 8)

            // "No data" instead of score
            Text("--")
                .font(.system(.callout, design: .monospaced))
                .foregroundStyle(.tertiary)
                .frame(width: 40, alignment: .trailing)

            // Status indicator (gray for missing)
            Image(systemName: "questionmark.circle")
                .font(.system(size: 10))
                .foregroundStyle(.tertiary)
                .frame(width: 10, height: 10)

            // "No data" label
            Text("No data")
                .font(.system(.callout, design: .monospaced))
                .foregroundStyle(.tertiary)
                .italic()
                .frame(width: 80, alignment: .trailing)
        }
        .padding(.vertical, 4)
    }

    private var barColor: Color {
        switch factor.status {
        case .healthy:
            return .green
        case .degraded:
            return .yellow
        case .unhealthy:
            return .red
        case .inactive:
            return .gray
        }
    }
}

#Preview {
    VStack(spacing: 8) {
        HealthFactorRow(factor: HealthFactor(
            metricName: "sequins.health.span_error_rate",
            displayName: "Span Error Rate",
            rawValue: 0.02,
            formattedValue: "2.0%",
            score: 0.85,
            status: .healthy,
            weight: 0.4
        ))

        HealthFactorRow(factor: HealthFactor(
            metricName: "sequins.health.http_error_rate",
            displayName: "HTTP Error Rate",
            rawValue: 0.12,
            formattedValue: "12.0%",
            score: 0.55,
            status: .degraded,
            weight: 0.25
        ))

        HealthFactorRow(factor: HealthFactor(
            metricName: "sequins.health.latency_p95",
            displayName: "Latency (p95)",
            rawValue: 850,
            formattedValue: "850ms",
            score: 0.25,
            status: .unhealthy,
            weight: 0.2
        ))

        // Missing data example
        HealthFactorRow(factor: HealthFactor.missing(
            metricName: "sequins.health.error_log_rate",
            displayName: "Error Log Rate",
            weight: 0.15
        ))
    }
    .padding()
    .frame(width: 500)
}
