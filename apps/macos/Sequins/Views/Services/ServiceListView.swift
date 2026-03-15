import SwiftUI
import SequinsData

/// Service list sidebar view using a live SeQL query
struct ServiceListView: View {
    @Environment(AppStateViewModel.self) private var appState
    @State private var services: [Service] = []
    @State private var isLoading: Bool = false
    @State private var selectedServiceName: String?
    @State private var liveStream: LiveSeQLStream?
    /// Accumulated resource rows — rebuilt on every batch/delta so new services appear live
    @State private var resourceRows: [(resourceId: UInt32, serviceName: String, attributesJSON: String)] = []

    var body: some View {
        @Bindable var appState = appState

        List(selection: $selectedServiceName) {
            if isLoading {
                ProgressView()
                    .frame(maxWidth: .infinity, alignment: .center)
            } else if services.isEmpty {
                VStack(spacing: 12) {
                    ContentUnavailableView(
                        "No Services",
                        systemImage: "server.rack",
                        description: Text("No telemetry data received yet")
                    )

                    if case .running(let grpcPort, let httpPort) = appState.serverStatus {
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Send telemetry to:")
                                .font(.caption)
                                .foregroundStyle(.secondary)

                            Group {
                                Text("gRPC: localhost:\(String(grpcPort))")
                                Text("HTTP: localhost:\(String(httpPort))")
                            }
                            .font(.system(.caption, design: .monospaced))
                            .foregroundStyle(.secondary)
                        }
                        .padding(12)
                        .background(Color(nsColor: .controlBackgroundColor))
                        .clipShape(RoundedRectangle(cornerRadius: 8))
                    }
                }
                .padding()
            } else {
                ForEach(services) { service in
                    ServiceRow(service: service)
                        .tag(service.name)
                }
            }
        }
        .navigationTitle("Services")
        .listStyle(.sidebar)
        .onChange(of: selectedServiceName) { _, newValue in
            if let serviceName = newValue,
               let service = services.first(where: { $0.name == serviceName }) {
                appState.selectedService = service
            } else {
                appState.selectedService = nil
            }
        }
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                HStack(spacing: 8) {
                    if false {
                        Button(action: { generateTestData() }) {
                            Label("Generate Test Data", systemImage: "doc.badge.plus")
                        }
                        .disabled(isLoading || appState.dataSource == nil)
                    }

                    if false {
                        Button(action: {}) {
                            Label("Refresh", systemImage: "arrow.clockwise")
                        }
                        .disabled(isLoading || appState.dataSource == nil)
                    }
                }
            }
        }
        .task(id: appState.dataSourceId) {
            if appState.dataSource != nil {
                startLiveStream()
            } else {
                liveStream?.cancel()
                liveStream = nil
                services = []
                resourceRows = []
            }
        }
        .onDisappear {
            liveStream?.cancel()
            liveStream = nil
        }
        .onChange(of: services) { _, newServices in
            // Auto-select first service if none selected
            if appState.selectedService == nil, let first = newServices.first {
                selectedServiceName = first.name
                appState.selectedService = first
            }
            // Keep selected service attributes up to date
            if let currentName = selectedServiceName,
               let updated = newServices.first(where: { $0.name == currentName }) {
                appState.selectedService = updated
            }
        }
    }

    // MARK: - Live stream

    private func startLiveStream() {
        guard let dataSource = appState.dataSource else { return }

        liveStream?.cancel()
        liveStream = nil
        resourceRows = []
        services = []
        isLoading = true

        do {
            let stream = try dataSource.executeLiveSeQL("resources last 24h")

            stream.onBatchCallback = { batch, _ in
                Task { @MainActor in
                    self.resourceRows.append(contentsOf: self.parseRows(from: batch))
                    self.rebuildServices()
                    self.isLoading = false
                }
            }

            stream.onDeltaCallback = { ops in
                Task { @MainActor in
                    for op in ops {
                        switch op.type {
                        case .append:
                            if let batch = op.data as? RecordBatch {
                                self.resourceRows.append(contentsOf: self.parseRows(from: batch))
                                self.rebuildServices()
                            }
                        case .replace:
                            if let batch = op.data as? RecordBatch {
                                self.resourceRows = self.parseRows(from: batch)
                                self.rebuildServices()
                            }
                        default:
                            break
                        }
                    }
                }
            }

            liveStream = stream
        } catch {
            print("[ServiceListView] Failed to start live stream: \(error)")
            isLoading = false
        }
    }

    private func parseRows(from batch: RecordBatch) -> [(resourceId: UInt32, serviceName: String, attributesJSON: String)] {
        batch.toRows().compactMap { row in
            guard row.count >= 2 else { return nil }
            let resourceId: UInt32
            if let num = row[0] as? NSNumber { resourceId = num.uint32Value }
            else if let u32 = row[0] as? UInt32 { resourceId = u32 }
            else { return nil }
            let serviceName = row[1] as? String ?? "unknown"
            let attributesJSON = row.count >= 3 ? (row[2] as? String ?? "") : ""
            return (resourceId, serviceName, attributesJSON)
        }
    }

    private func rebuildServices() {
        var serviceResourceIds: [String: [UInt32]] = [:]
        var serviceAttributes: [String: [String: Set<String>]] = [:]

        for row in resourceRows {
            guard !row.serviceName.isEmpty && row.serviceName != "unknown" else { continue }
            serviceResourceIds[row.serviceName, default: []].append(row.resourceId)
            if !row.attributesJSON.isEmpty,
               let data = row.attributesJSON.data(using: .utf8),
               let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                for (key, value) in dict {
                    let strValue: String
                    if let s = value as? String { strValue = s }
                    else if let n = value as? NSNumber { strValue = n.stringValue }
                    else { strValue = "\(value)" }
                    serviceAttributes[row.serviceName, default: [:]][key, default: Set()].insert(strValue)
                }
            }
        }

        services = serviceResourceIds.sorted { $0.key < $1.key }.map { name, ids in
            let attrs = (serviceAttributes[name] ?? [:])
                .sorted { $0.key < $1.key }
                .map { key, valSet in ResourceAttribute(key: key, values: valSet.sorted()) }
            return Service(name: name, spanCount: 0, logCount: 0, resourceAttributes: attrs, resourceIds: ids)
        }
    }

    // MARK: - Test data (hidden)

    private func generateTestData() {
        guard let dataSource = appState.dataSource else { return }
        Task {
            do {
                let count = try dataSource.generateTestData()
                print("[ServiceListView] Generated \(count) test spans")
            } catch {
                print("[ServiceListView] Failed to generate test data: \(error)")
            }
        }
    }
}

#Preview {
    ServiceListView()
        .environment(AppStateViewModel())
}
