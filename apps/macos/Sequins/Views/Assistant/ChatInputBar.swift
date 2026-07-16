import SwiftUI

/// The chat composer: a growing text field plus send/stop control.
struct ChatInputBar: View {
    @Bindable var viewModel: AssistantViewModel
    let onSend: () -> Void

    private var canSend: Bool {
        !viewModel.inputText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    var body: some View {
        HStack(alignment: .bottom, spacing: 8) {
            TextField("Ask about your telemetry…", text: $viewModel.inputText, axis: .vertical)
                .textFieldStyle(.plain)
                .lineLimit(1...6)
                .padding(8)
                .background(Color(nsColor: .textBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
                .onSubmit {
                    if canSend { onSend() }
                }

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
        .padding(8)
    }
}
