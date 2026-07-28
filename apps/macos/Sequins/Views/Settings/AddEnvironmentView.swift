import SwiftUI

struct AddEnvironmentView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Environment(\.dismiss) private var dismiss

    @State private var name = ""
    // The query URL is the daemon's Arrow Flight SQL origin (host:port, no path); the
    // management URL is its HTTP API. Defaults match the daemon's default ports.
    @State private var queryURL = "http://localhost:4319"
    @State private var managementURL = "http://localhost:8081"
    @State private var connectAfterAdd = true

    private var isValid: Bool {
        !name.trimmingCharacters(in: .whitespaces).isEmpty &&
        URL(string: queryURL) != nil &&
        URL(string: managementURL) != nil
    }

    var body: some View {
        VStack(spacing: 0) {
            // Header
            HStack {
                Text("Add Remote Environment")
                    .font(.headline)
                Spacer()
                Button {
                    dismiss()
                } label: {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundStyle(.secondary)
                }
                .buttonStyle(.borderless)
            }
            .padding()

            Divider()

            // Form
            Form {
                Section {
                    TextField("Environment Name", text: $name)
                        .textFieldStyle(.roundedBorder)
                }

                Section {
                    LabeledContent("Query URL") {
                        TextField("http://host:4319", text: $queryURL)
                            .textFieldStyle(.roundedBorder)
                    }

                    LabeledContent("Management URL") {
                        TextField("http://host:8081", text: $managementURL)
                            .textFieldStyle(.roundedBorder)
                    }
                } header: {
                    Text("Connection")
                } footer: {
                    Text("Query URL is the Arrow Flight SQL endpoint (host:port, no path). "
                        + "Management URL is the HTTP API.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                Section {
                    Toggle("Connect after adding", isOn: $connectAfterAdd)
                }
            }
            .formStyle(.grouped)
            .scrollContentBackground(.hidden)

            Divider()

            // Actions
            HStack {
                Button("Cancel") {
                    dismiss()
                }
                .keyboardShortcut(.cancelAction)

                Spacer()

                Button("Add Environment") {
                    addEnvironment()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(!isValid)
            }
            .padding()
        }
        .frame(width: 450, height: 350)
    }

    private func addEnvironment() {
        let environment = appState.environmentManager.addRemoteEnvironment(
            name: name.trimmingCharacters(in: .whitespaces),
            queryURL: queryURL,
            managementURL: managementURL
        )

        if connectAfterAdd {
            appState.switchToEnvironment(environment)
        }

        dismiss()
    }
}

#Preview {
    AddEnvironmentView()
        .environment(AppStateViewModel())
}
