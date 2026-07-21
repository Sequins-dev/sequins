import SwiftUI
import SequinsData

/// Renders a set of **score bars** — one horizontal bar per factor, filled to a
/// 0…1 score and colored by band (≥0.7 green, ≥0.3 yellow, else red) — matching the
/// Health tab's factor rows. Drives off whatever the query produces, so any query
/// whose columns are scores can be shown this way.
///
/// Accepted shapes:
/// - **Wide single row**: each numeric column is a factor; the column name is the
///   label and the cell is the score. An optional companion column `"<name>__value"`
///   supplies a raw value shown to the right. A column named `overall` or `score` is
///   rendered as an emphasized summary row at the top.
/// - **Tall** `[label, score]`: one bar per row.
struct ScoreBarView: View {
    let columns: [String]
    let rows: [[Any?]]
    var columnTypes: [NodeTypeLabel] = []
    var columnRoles: [SeQLColumnRole] = []
    var options: VisualizationOptions = VisualizationOptions()

    private struct Factor: Identifiable {
        let id = UUID()
        let label: String
        let score: Double
        let value: String?
        let isOverall: Bool
    }

    var body: some View {
        let factors = self.factors
        if factors.isEmpty {
            VizMessage(icon: "chart.bar.xaxis", text: "No scores to show")
        } else {
            VStack(alignment: .leading, spacing: 2) {
                ForEach(factors) { factor in
                    if factor.isOverall {
                        overallRow(factor)
                        Divider().padding(.vertical, 2)
                    } else {
                        factorRow(factor)
                    }
                }
            }
            .padding()
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        }
    }

    // MARK: - Rows

    private func factorRow(_ factor: Factor) -> some View {
        HStack(spacing: 12) {
            Text(factor.label)
                .font(.system(.body))
                .foregroundStyle(.primary)
                .frame(minWidth: 120, alignment: .leading)

            GeometryReader { geometry in
                ZStack(alignment: .leading) {
                    RoundedRectangle(cornerRadius: 4)
                        .fill(Color.secondary.opacity(0.2))
                    RoundedRectangle(cornerRadius: 4)
                        .fill(bandColor(factor.score))
                        .frame(width: max(0, geometry.size.width * factor.score.clamped01))
                }
            }
            .frame(height: 8)

            Text("\(Int((factor.score.clamped01) * 100))%")
                .font(.system(.callout, design: .monospaced))
                .foregroundStyle(.secondary)
                .frame(width: 44, alignment: .trailing)

            Circle()
                .fill(bandColor(factor.score))
                .frame(width: 10, height: 10)

            Text(factor.value ?? "")
                .font(.system(.callout, design: .monospaced))
                .foregroundStyle(.secondary)
                .frame(width: 80, alignment: .trailing)
        }
        .padding(.vertical, 4)
    }

    private func overallRow(_ factor: Factor) -> some View {
        HStack(spacing: 12) {
            Text(factor.label)
                .font(.headline)
                .frame(minWidth: 120, alignment: .leading)
            Spacer(minLength: 0)
            Text("\(Int((factor.score.clamped01) * 100))%")
                .font(.system(.title2, design: .rounded).weight(.semibold))
                .foregroundStyle(bandColor(factor.score))
            Circle()
                .fill(bandColor(factor.score))
                .frame(width: 12, height: 12)
        }
        .padding(.vertical, 4)
    }

    // MARK: - Data mapping

    /// Column index of the `"<name>__value"` companion for a factor column, if present.
    private func valueColumnIndex(for name: String) -> Int? {
        columns.firstIndex(of: "\(name)__value")
    }

    private var factors: [Factor] {
        guard !columns.isEmpty, !rows.isEmpty else { return [] }

        // Tall `[label, score]` — a text first column + a numeric second column,
        // more than one row.
        if rows.count > 1, columns.count >= 2,
           rows.allSatisfy({ VizFormat.numeric($0.first.flatMap { $0 } ?? nil) == nil }) {
            return rows.compactMap { row -> Factor? in
                guard row.count >= 2, let score = VizFormat.numeric(row[1]) else { return nil }
                let label = VizFormat.string(row.first ?? nil)
                return Factor(label: label, score: score, value: nil, isOverall: isOverallName(label))
            }
        }

        // Wide single row: each numeric, non-companion column is a factor.
        let row = rows[0]
        var out: [Factor] = []
        for (i, name) in columns.enumerated() {
            if name.hasSuffix("__value") { continue }
            guard i < row.count, let score = VizFormat.numeric(row[i]) else { continue }
            let value = valueColumnIndex(for: name)
                .flatMap { idx -> String? in idx < row.count ? formatValue(row[idx]) : nil }
            out.append(Factor(
                label: factorLabel(name),
                score: score,
                value: value,
                isOverall: isOverallName(name)
            ))
        }
        // Overall rows float to the top.
        out.sort { $0.isOverall && !$1.isOverall }
        return out
    }

    private func isOverallName(_ name: String) -> Bool {
        let n = name.lowercased()
        return n == "overall" || n == "score" || n == "overall_score" || n == "health"
    }

    /// Turn a column name into a display label: split on `_`, drop a trailing
    /// `score`/`status` token, and title-case (e.g. `span_error_score` → "Span Error").
    private func factorLabel(_ name: String) -> String {
        var parts = name.split(separator: "_").map(String.init)
        if let last = parts.last?.lowercased(), last == "score" || last == "status" {
            parts.removeLast()
        }
        if parts.isEmpty { parts = [name] }
        return parts.map { $0.prefix(1).uppercased() + $0.dropFirst() }.joined(separator: " ")
    }

    private func formatValue(_ cell: Any?) -> String? {
        if let s = cell as? String { return s }
        if let n = VizFormat.numeric(cell) { return VizFormat.number(n) }
        return nil
    }

    /// Score band → status color, matching the score→status mapping (healthy ≥0.7,
    /// degraded ≥0.3, else unhealthy).
    private func bandColor(_ score: Double) -> Color {
        let s = score.clamped01
        if s >= 0.7 { return .green }
        if s >= 0.3 { return .yellow }
        return .red
    }
}

private extension Double {
    var clamped01: Double { Swift.min(1.0, Swift.max(0.0, self)) }
}
