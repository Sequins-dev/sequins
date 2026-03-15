import SwiftUI

struct AddEnvironmentView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Environment(\.dismiss) private var dismiss

    @State private var name = ""
    @State private var queryURL = "http://localhost:8080/query"
    @State private var managementURL = "http://localhost:8080/management"
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

                Section("Connection") {
                    LabeledContent("Query URL") {
                        TextField("URL", text: $queryURL)
                            .textFieldStyle(.roundedBorder)
                    }

                    LabeledContent("Management URL") {
                        TextField("URL", text: $managementURL)
                            .textFieldStyle(.roundedBorder)
                    }
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
