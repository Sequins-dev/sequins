import SwiftUI
import SequinsData

/// "New from Template" gallery — lists the built-in dashboard templates and creates a
/// fresh dashboard from the chosen one.
struct TemplateGallerySheet: View {
    @Bindable var viewModel: DashboardsViewModel
    let dataSource: DataSource?

    @Environment(\.dismiss) private var dismiss
    @State private var templates: [DashboardTemplateInfo] = []
    @State private var selectedID: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            Text("New from Template")
                .font(.title3.weight(.semibold))
                .padding(.horizontal, 16)
                .padding(.top, 16)
                .padding(.bottom, 8)

            if templates.isEmpty {
                VStack(spacing: 6) {
                    Image(systemName: "rectangle.stack")
                        .font(.title2)
                        .foregroundStyle(.tertiary)
                    Text("No templates available")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .frame(minHeight: 160)
            } else {
                ScrollView {
                    VStack(spacing: 8) {
                        ForEach(templates) { template in
                            templateCard(template)
                        }
                    }
                    .padding(16)
                }
            }

            Divider()
            HStack {
                Spacer()
                Button("Cancel", role: .cancel) { dismiss() }
                    .keyboardShortcut(.cancelAction)
                Button("Create") { create() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(selectedID == nil || dataSource == nil)
            }
            .padding(16)
        }
        .frame(width: 460, height: 380)
        .onAppear {
            if let ds = dataSource {
                templates = viewModel.loadTemplates(dataSource: ds)
                selectedID = templates.first?.id
            }
        }
    }

    private func templateCard(_ template: DashboardTemplateInfo) -> some View {
        Button {
            selectedID = template.id
        } label: {
            VStack(alignment: .leading, spacing: 4) {
                Text(template.title)
                    .font(.headline)
                    .foregroundStyle(.primary)
                Text(template.description)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.leading)
                    .fixedSize(horizontal: false, vertical: true)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(12)
            .background(
                RoundedRectangle(cornerRadius: 8)
                    .fill(selectedID == template.id
                          ? Color.accentColor.opacity(0.15)
                          : Color(nsColor: .controlBackgroundColor))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 8)
                    .strokeBorder(selectedID == template.id ? Color.accentColor : Color.clear, lineWidth: 1.5)
            )
        }
        .buttonStyle(.plain)
    }

    private func create() {
        guard let id = selectedID, let ds = dataSource else { return }
        viewModel.createFromTemplate(id: id, dataSource: ds)
        dismiss()
    }
}
