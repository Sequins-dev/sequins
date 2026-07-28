import SwiftUI

/// A lightweight block-level Markdown renderer for chat messages — headings, bullet and
/// numbered lists, fenced code blocks, and paragraphs, with inline emphasis/`code`/links
/// rendered via `AttributedString`. Enough for assistant and user chat text without
/// pulling in a Markdown dependency, and it degrades gracefully on the partial Markdown
/// produced while an assistant reply is still streaming.
struct MarkdownText: View {
    let text: String

    var body: some View {
        let blocks = Self.parse(text)
        VStack(alignment: .leading, spacing: 8) {
            ForEach(Array(blocks.enumerated()), id: \.offset) { _, block in
                view(for: block)
            }
        }
    }

    // MARK: - Block views

    @ViewBuilder
    private func view(for block: Block) -> some View {
        switch block {
        case .heading(let level, let content):
            Self.inline(content)
                .font(Self.headingFont(level))
                .fontWeight(.semibold)
        case .paragraph(let content):
            // `inlineOnlyPreservingWhitespace` keeps soft line breaks within a paragraph.
            Self.inline(content)
        case .bullet(let items):
            VStack(alignment: .leading, spacing: 3) {
                ForEach(Array(items.enumerated()), id: \.offset) { _, item in
                    HStack(alignment: .firstTextBaseline, spacing: 6) {
                        Text("•").foregroundStyle(.secondary)
                        Self.inline(item)
                    }
                }
            }
        case .ordered(let items):
            VStack(alignment: .leading, spacing: 3) {
                ForEach(Array(items.enumerated()), id: \.offset) { idx, item in
                    HStack(alignment: .firstTextBaseline, spacing: 6) {
                        Text("\(idx + 1).").monospacedDigit().foregroundStyle(.secondary)
                        Self.inline(item)
                    }
                }
            }
        case .code(let code):
            Text(code)
                .font(.system(.caption, design: .monospaced))
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(8)
                .background(Color.primary.opacity(0.06))
                .clipShape(RoundedRectangle(cornerRadius: 6))
        }
    }

    private static func headingFont(_ level: Int) -> Font {
        switch level {
        case 1: return .title3
        case 2: return .headline
        default: return .subheadline
        }
    }

    // MARK: - Inline

    /// Render one span of inline Markdown (bold/italic/`code`/links), preserving the
    /// original whitespace and newlines. Falls back to plain text on a parse failure.
    static func inline(_ s: String) -> Text {
        if let attributed = try? AttributedString(
            markdown: s,
            options: .init(interpretedSyntax: .inlineOnlyPreservingWhitespace)
        ) {
            return Text(attributed)
        }
        return Text(s)
    }

    // MARK: - Parsing

    enum Block {
        case heading(level: Int, content: String)
        case paragraph(String)
        case bullet([String])
        case ordered([String])
        case code(String)
    }

    static func parse(_ text: String) -> [Block] {
        var blocks: [Block] = []
        let lines = text.components(separatedBy: "\n")
        var para: [String] = []
        var i = 0

        func flushParagraph() {
            if !para.isEmpty {
                blocks.append(.paragraph(para.joined(separator: "\n")))
                para.removeAll()
            }
        }

        while i < lines.count {
            let raw = lines[i]
            let line = raw.trimmingCharacters(in: .whitespaces)

            // Fenced code block: ``` … ```
            if line.hasPrefix("```") {
                flushParagraph()
                i += 1
                var code: [String] = []
                while i < lines.count, !lines[i].trimmingCharacters(in: .whitespaces).hasPrefix("```") {
                    code.append(lines[i])
                    i += 1
                }
                i += 1 // consume the closing fence (or run off the end while streaming)
                blocks.append(.code(code.joined(separator: "\n")))
                continue
            }

            // ATX heading: #, ##, ### followed by a space.
            if let level = headingLevel(line) {
                flushParagraph()
                let content = String(line.drop(while: { $0 == "#" }))
                    .trimmingCharacters(in: .whitespaces)
                blocks.append(.heading(level: level, content: content))
                i += 1
                continue
            }

            // Unordered list run.
            if isBullet(line) {
                flushParagraph()
                var items: [String] = []
                while i < lines.count {
                    let t = lines[i].trimmingCharacters(in: .whitespaces)
                    guard isBullet(t) else { break }
                    items.append(String(t.dropFirst(2)))
                    i += 1
                }
                blocks.append(.bullet(items))
                continue
            }

            // Ordered list run (`1. `, `2. `, …).
            if orderedDrop(line) != nil {
                flushParagraph()
                var items: [String] = []
                while i < lines.count {
                    let t = lines[i].trimmingCharacters(in: .whitespaces)
                    guard let drop = orderedDrop(t) else { break }
                    items.append(String(t.dropFirst(drop)))
                    i += 1
                }
                blocks.append(.ordered(items))
                continue
            }

            // Blank line ends a paragraph.
            if line.isEmpty {
                flushParagraph()
                i += 1
                continue
            }

            para.append(raw)
            i += 1
        }
        flushParagraph()
        return blocks
    }

    private static func headingLevel(_ line: String) -> Int? {
        guard line.hasPrefix("#") else { return nil }
        let hashes = line.prefix(while: { $0 == "#" }).count
        guard hashes <= 3, line.dropFirst(hashes).first == " " else { return nil }
        return hashes
    }

    private static func isBullet(_ line: String) -> Bool {
        line.hasPrefix("- ") || line.hasPrefix("* ") || line.hasPrefix("+ ")
    }

    /// If `line` starts an ordered-list item (`12. `), the number of leading characters
    /// to drop to reach the item text; otherwise `nil`.
    private static func orderedDrop(_ line: String) -> Int? {
        let digits = line.prefix(while: { $0.isNumber })
        guard !digits.isEmpty, line.dropFirst(digits.count).hasPrefix(". ") else { return nil }
        return digits.count + 2
    }
}
