import SwiftUI

/// The chat composer: a growing text field plus send/stop control.
struct ChatInputBar: View {
    @Bindable var viewModel: AssistantViewModel
    let onSend: () -> Void

    @State private var modelHover = false
    @State private var reasoningHover = false

    private var canSend: Bool {
        !viewModel.inputText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            TextField("Ask about your telemetry…", text: $viewModel.inputText, axis: .vertical)
                .textFieldStyle(.plain)
                .lineLimit(1...6)
                .padding(8)
                .background(Color(nsColor: .textBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
                .onSubmit {
                    if canSend { onSend() }
                }

            // Per-turn controls under the composer; send/stop is right-aligned.
            HStack(spacing: 14) {
                modelPicker
                reasoningPicker
                Spacer()
                sendButton
            }
            .padding(.horizontal, 6)
            .padding(.bottom, 2)
        }
        .padding(8)
    }

    @ViewBuilder
    private var sendButton: some View {
        if viewModel.isStreaming {
            Button {
                viewModel.cancelStreaming()
            } label: {
                Image(systemName: "stop.circle.fill")
                    .font(.title2)
            }
            .buttonStyle(.borderless)
            .help("Stop")
        } else {
            Button(action: onSend) {
                Image(systemName: "arrow.up.circle.fill")
                    .font(.title2)
            }
            .buttonStyle(.borderless)
            .disabled(!canSend)
            .help("Send")
        }
    }

    /// Model selector — lists the provider/daemon's advertised models and changes the
    /// model used for the next turn on the fly.
    private var modelPicker: some View {
        Menu {
            if viewModel.availableModels.isEmpty {
                Text(viewModel.modelsError == nil ? "Loading models…" : "No models available")
            }
            ForEach(viewModel.availableModels, id: \.self) { model in
                Button {
                    viewModel.selectedModel = model
                } label: {
                    if viewModel.selectedModel == model {
                        Label(model, systemImage: "checkmark")
                    } else {
                        Text(model)
                    }
                }
            }
        } label: {
            pill(hovered: modelHover) {
                Image(systemName: "cpu")
                Text(viewModel.selectedModel ?? "Model")
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .frame(maxWidth: 200)
                Image(systemName: "chevron.down")
                    .font(.system(size: 8, weight: .semibold))
            }
        }
        .menuStyle(.button)
        .buttonStyle(.plain)
        .menuIndicator(.hidden)
        .fixedSize()
        .onHover { modelHover = $0 }
        .help(viewModel.modelsError ?? "Model for the next message")
    }

    /// Reasoning-effort selector — controls how hard the model thinks before answering.
    /// Unsupported choices are reconciled by the assistant middleware.
    private var reasoningPicker: some View {
        Menu {
            ForEach(ReasoningEffort.allCases) { effort in
                Button {
                    viewModel.selectedReasoning = effort
                } label: {
                    if viewModel.selectedReasoning == effort {
                        Label(effort.label, systemImage: "checkmark")
                    } else {
                        Text(effort.label)
                    }
                }
            }
        } label: {
            pill(hovered: reasoningHover) {
                Image(systemName: "brain")
                Text(viewModel.selectedReasoning.label)
                Image(systemName: "chevron.down")
                    .font(.system(size: 8, weight: .semibold))
            }
        }
        .menuStyle(.button)
        .buttonStyle(.plain)
        .menuIndicator(.hidden)
        .fixedSize()
        .onHover { reasoningHover = $0 }
        .help("Reasoning effort for the next message")
    }

    /// A compact pill label for the per-turn control menus: subtle/faded by default, and
    /// lit up (brighter fill + border, full opacity) while hovered.
    @ViewBuilder
    private func pill<Content: View>(
        hovered: Bool, @ViewBuilder content: () -> Content
    ) -> some View {
        HStack(spacing: 4) {
            content()
        }
        .font(.caption)
        .foregroundStyle(.secondary)
        .padding(.horizontal, 10)
        .padding(.vertical, 4)
        .background(
            Capsule().fill(Color.secondary.opacity(hovered ? 0.16 : 0.07))
        )
        .overlay(
            Capsule().strokeBorder(Color.secondary.opacity(hovered ? 0.3 : 0.12), lineWidth: 0.5)
        )
        .opacity(hovered ? 1.0 : 0.6)
        .animation(.easeOut(duration: 0.15), value: hovered)
    }
}
