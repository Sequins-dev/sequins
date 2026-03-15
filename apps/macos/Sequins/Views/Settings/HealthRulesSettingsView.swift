//
//  HealthRulesSettingsView.swift
//  Sequins
//
//  UI for configuring health metric rules and thresholds.
//

import SwiftUI
import SequinsData

struct HealthRulesSettingsView: View {
    @Environment(AppStateViewModel.self) private var appState
    @State private var healthConfig: HealthThresholdConfig = .default
    @State private var isLoading = true
    @State private var error: String?
    @State private var showingAddRule = false
    @State private var editingRule: HealthMetricRule?

    var body: some View {
        VStack(spacing: 0) {
            // Header
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Health Rules")
                        .font(.headline)
                    Text("Configure which metrics contribute to health and their thresholds.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button(action: { showingAddRule = true }) {
                    Label("Add Rule", systemImage: "plus")
                }
                .buttonStyle(.borderedProminent)
            }
            .padding()

            Divider()

            if isLoading {
                ProgressView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if let error = error {
                ContentUnavailableView(
                    "Error Loading Rules",
                    systemImage: "exclamationmark.triangle",
                    description: Text(error)
                )
            } else if healthConfig.rules.isEmpty {
                ContentUnavailableView(
                    "No Health Rules",
                    systemImage: "heart.slash",
                    description: Text("Add rules to define which metrics contribute to health.")
                )
            } else {
                List {
                    ForEach(healthConfig.rules) { rule in
                        HealthRuleRow(rule: rule, onEdit: { editingRule = rule })
                            .contextMenu {
                                Button("Edit") {
                                    editingRule = rule
                                }
                                Button("Delete", role: .destructive) {
                                    deleteRule(rule)
                                }
                            }
                    }
                    .onDelete(perform: deleteRules)
                }
            }

            Divider()

            // Footer with reset button
            HStack {
                Button("Reset to Defaults") {
                    resetToDefaults()
                }
                .buttonStyle(.bordered)

                Spacer()

                if healthConfig.rules != HealthThresholdConfig.default.rules {
                    Text("Modified")
                        .font(.caption)
                        .foregroundStyle(.orange)
                }
            }
            .padding()
        }
        .onAppear {
            loadHealthConfig()
        }
        .sheet(isPresented: $showingAddRule) {
            AddHealthRuleSheet(
                onSave: { rule in
                    addRule(rule)
                    showingAddRule = false
                },
                onCancel: { showingAddRule = false }
            )
        }
        .sheet(item: $editingRule) { rule in
            EditHealthRuleSheet(
                rule: rule,
                onSave: { updatedRule in
                    updateRule(updatedRule)
                    editingRule = nil
                },
                onCancel: { editingRule = nil }
            )
        }
    }

    // MARK: - Data Operations

    private func loadHealthConfig() {
        guard let dataSource = appState.dataSource else {
            error = "No data source connected"
            isLoading = false
            return
        }

        isLoading = true
        error = nil

        do {
            healthConfig = try dataSource.getHealthThresholdConfig()
            isLoading = false
        } catch {
            self.error = error.localizedDescription
            isLoading = false
        }
    }

    private func addRule(_ rule: HealthMetricRule) {
        guard let dataSource = appState.dataSource else { return }

        do {
            try dataSource.addHealthRule(rule)
            healthConfig.rules.append(rule)
            appState.healthMonitorService.reloadHealthConfig()
        } catch {
            self.error = "Failed to add rule: \(error.localizedDescription)"
        }
    }

    private func updateRule(_ rule: HealthMetricRule) {
        guard let dataSource = appState.dataSource else { return }

        do {
            try dataSource.addHealthRule(rule) // Add overwrites existing
            if let index = healthConfig.rules.firstIndex(where: { $0.id == rule.id }) {
                healthConfig.rules[index] = rule
            }
            appState.healthMonitorService.reloadHealthConfig()
        } catch {
            self.error = "Failed to update rule: \(error.localizedDescription)"
        }
    }

    private func deleteRule(_ rule: HealthMetricRule) {
        guard let dataSource = appState.dataSource else { return }

        do {
            try dataSource.removeHealthRule(metricName: rule.metricName, serviceName: rule.serviceName)
            healthConfig.rules.removeAll { $0.id == rule.id }
            appState.healthMonitorService.reloadHealthConfig()
        } catch {
            self.error = "Failed to delete rule: \(error.localizedDescription)"
        }
    }

    private func deleteRules(at offsets: IndexSet) {
        for index in offsets {
            deleteRule(healthConfig.rules[index])
        }
    }

    private func resetToDefaults() {
        guard let dataSource = appState.dataSource else { return }

        do {
            try dataSource.setHealthThresholdConfig(.default)
            healthConfig = .default
            appState.healthMonitorService.reloadHealthConfig()
        } catch {
            self.error = "Failed to reset: \(error.localizedDescription)"
        }
    }
}

// MARK: - Health Rule Row

struct HealthRuleRow: View {
    let rule: HealthMetricRule
    let onEdit: () -> Void

    var body: some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text(rule.displayName)
                    .font(.headline)

                HStack(spacing: 12) {
                    Label {
                        Text(formatThreshold(rule.warningThreshold, metricName: rule.metricName))
                    } icon: {
                        Image(systemName: "exclamationmark.triangle.fill")
                            .foregroundStyle(.yellow)
                    }
                    .font(.caption)

                    Label {
                        Text(formatThreshold(rule.errorThreshold, metricName: rule.metricName))
                    } icon: {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundStyle(.red)
                    }
                    .font(.caption)
                }

                HStack(spacing: 8) {
                    Text(rule.metricName)
                        .font(.caption2)
                        .foregroundStyle(.secondary)

                    if let service = rule.serviceName {
                        Text("(\(service))")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }

                    Spacer()

                    Text("Weight: \(Int(rule.weight * 100))%")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }

            Spacer()

            Button(action: onEdit) {
                Image(systemName: "chevron.right")
                    .foregroundStyle(.secondary)
            }
            .buttonStyle(.plain)
        }
        .padding(.vertical, 4)
    }

    private func formatThreshold(_ value: Double, metricName: String) -> String {
        if metricName.contains("latency") {
            let ms = value / 1_000_000.0
            if ms < 1000 {
                return String(format: "%.0fms", ms)
            } else {
                return String(format: "%.1fs", ms / 1000)
            }
        } else if metricName.contains("rate") && value <= 1.0 {
            return String(format: "%.1f%%", value * 100)
        } else {
            return String(format: "%.1f", value)
        }
    }
}

// MARK: - Add Health Rule Sheet

struct AddHealthRuleSheet: View {
    let onSave: (HealthMetricRule) -> Void
    let onCancel: () -> Void

    @State private var metricName = ""
    @State private var displayName = ""
    @State private var serviceName = ""
    @State private var warningThreshold = ""
    @State private var errorThreshold = ""
    @State private var higherIsWorse = true
    @State private var weight = 0.25

    // Predefined metric options
    private let predefinedMetrics = [
        ("Custom", ""),
        ("Span Error Rate", HealthMetricNames.spanErrorRate),
        ("HTTP Error Rate", HealthMetricNames.httpErrorRate),
        ("Latency P50", HealthMetricNames.latencyP50),
        ("Latency P95", HealthMetricNames.latencyP95),
        ("Latency P99", HealthMetricNames.latencyP99),
        ("Throughput", HealthMetricNames.throughput),
        ("Error Log Rate", HealthMetricNames.errorLogRate)
    ]

    var body: some View {
        VStack(spacing: 0) {
            // Header
            HStack {
                Text("Add Health Rule")
                    .font(.headline)
                Spacer()
                Button("Cancel") { onCancel() }
            }
            .padding()

            Divider()

            Form {
                Section("Metric") {
                    Picker("Predefined", selection: $metricName) {
                        ForEach(predefinedMetrics, id: \.1) { name, value in
                            Text(name).tag(value)
                        }
                    }
                    .onChange(of: metricName) { _, newValue in
                        if let found = predefinedMetrics.first(where: { $0.1 == newValue }) {
                            if !found.0.isEmpty && found.0 != "Custom" {
                                displayName = found.0
                            }
                        }
                    }

                    if metricName.isEmpty {
                        TextField("Custom Metric Name", text: $metricName)
                    }

                    TextField("Display Name", text: $displayName)

                    TextField("Service Filter (optional)", text: $serviceName)
                        .help("Leave empty to apply to all services")
                }

                Section("Thresholds") {
                    Toggle("Higher values are worse", isOn: $higherIsWorse)

                    HStack {
                        Text("Warning")
                            .frame(width: 80, alignment: .leading)
                        TextField("Threshold", text: $warningThreshold)
                            .textFieldStyle(.roundedBorder)
                    }

                    HStack {
                        Text("Error")
                            .frame(width: 80, alignment: .leading)
                        TextField("Threshold", text: $errorThreshold)
                            .textFieldStyle(.roundedBorder)
                    }

                    Text("For rates, use decimal (0.05 = 5%). For latency, use nanoseconds.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                Section("Weight") {
                    Slider(value: $weight, in: 0...1, step: 0.05)
                    Text("\(Int(weight * 100))% contribution to overall health")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .formStyle(.grouped)

            Divider()

            // Footer
            HStack {
                Spacer()
                Button("Save") {
                    saveRule()
                }
                .buttonStyle(.borderedProminent)
                .disabled(!isValid)
            }
            .padding()
        }
        .frame(width: 450, height: 500)
    }

    private var isValid: Bool {
        !metricName.isEmpty &&
        !displayName.isEmpty &&
        Double(warningThreshold) != nil &&
        Double(errorThreshold) != nil
    }

    private func saveRule() {
        guard let warning = Double(warningThreshold),
              let error = Double(errorThreshold) else { return }

        let rule = HealthMetricRule(
            metricName: metricName,
            serviceName: serviceName.isEmpty ? nil : serviceName,
            warningThreshold: warning,
            errorThreshold: error,
            higherIsWorse: higherIsWorse,
            weight: weight,
            displayName: displayName
        )

        onSave(rule)
    }
}

// MARK: - Edit Health Rule Sheet

struct EditHealthRuleSheet: View {
    let rule: HealthMetricRule
    let onSave: (HealthMetricRule) -> Void
    let onCancel: () -> Void

    @State private var displayName: String
    @State private var serviceName: String
    @State private var warningThreshold: String
    @State private var errorThreshold: String
    @State private var higherIsWorse: Bool
    @State private var weight: Double

    init(rule: HealthMetricRule, onSave: @escaping (HealthMetricRule) -> Void, onCancel: @escaping () -> Void) {
        self.rule = rule
        self.onSave = onSave
        self.onCancel = onCancel
        self._displayName = State(initialValue: rule.displayName)
        self._serviceName = State(initialValue: rule.serviceName ?? "")
        self._warningThreshold = State(initialValue: String(rule.warningThreshold))
        self._errorThreshold = State(initialValue: String(rule.errorThreshold))
        self._higherIsWorse = State(initialValue: rule.higherIsWorse)
        self._weight = State(initialValue: rule.weight)
    }

    var body: some View {
        VStack(spacing: 0) {
            // Header
            HStack {
                Text("Edit Health Rule")
                    .font(.headline)
                Spacer()
                Button("Cancel") { onCancel() }
            }
            .padding()

            Divider()

            Form {
                Section("Metric") {
                    LabeledContent("Metric Name", value: rule.metricName)
                        .foregroundStyle(.secondary)

                    TextField("Display Name", text: $displayName)

                    TextField("Service Filter (optional)", text: $serviceName)
                        .help("Leave empty to apply to all services")
                }

                Section("Thresholds") {
                    Toggle("Higher values are worse", isOn: $higherIsWorse)

                    HStack {
                        Text("Warning")
                            .frame(width: 80, alignment: .leading)
                        TextField("Threshold", text: $warningThreshold)
                            .textFieldStyle(.roundedBorder)
                    }

                    HStack {
                        Text("Error")
                            .frame(width: 80, alignment: .leading)
                        TextField("Threshold", text: $errorThreshold)
                            .textFieldStyle(.roundedBorder)
                    }
                }

                Section("Weight") {
                    Slider(value: $weight, in: 0...1, step: 0.05)
                    Text("\(Int(weight * 100))% contribution to overall health")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .formStyle(.grouped)

            Divider()

            // Footer
            HStack {
                Spacer()
                Button("Save") {
                    saveRule()
                }
                .buttonStyle(.borderedProminent)
                .disabled(!isValid)
            }
            .padding()
        }
        .frame(width: 450, height: 450)
    }

    private var isValid: Bool {
        !displayName.isEmpty &&
        Double(warningThreshold) != nil &&
        Double(errorThreshold) != nil
    }

    private func saveRule() {
        guard let warning = Double(warningThreshold),
              let error = Double(errorThreshold) else { return }

        var updatedRule = rule
        updatedRule.displayName = displayName
        updatedRule.serviceName = serviceName.isEmpty ? nil : serviceName
        updatedRule.warningThreshold = warning
        updatedRule.errorThreshold = error
        updatedRule.higherIsWorse = higherIsWorse
        updatedRule.weight = weight

        onSave(updatedRule)
    }
}

#Preview {
    HealthRulesSettingsView()
        .frame(width: 500, height: 600)
        .environment(AppStateViewModel())
}
