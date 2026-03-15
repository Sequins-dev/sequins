import SwiftUI
import AppKit

// 220 (sidebar ideal) + 660 (detail min: 200+250+200 panels + dividers) + chrome
private final class SequinsWindowDelegate: NSObject, NSWindowDelegate {}

final class MainWindowController {
    static let shared = MainWindowController()

    private var window: NSWindow?
    private let delegate = SequinsWindowDelegate()

    private init() {}

    func showWindow(appState: AppStateViewModel, selectedTab: NavigationItem = .health) {
        if let existingWindow = window, existingWindow.isVisible {
            existingWindow.makeKeyAndOrderFront(nil)
            NSApp.activate(ignoringOtherApps: true)
            return
        }

        if window == nil {
            let contentView = MainWindow(initialTab: selectedTab)
                .environment(appState)

            window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 1200, height: 800),
                styleMask: [.titled, .closable, .miniaturizable, .resizable],
                backing: .buffered,
                defer: false
            )
            window?.center()
            window?.setFrameAutosaveName("SequinsMainWindow")
            window?.titlebarAppearsTransparent = true
            window?.titleVisibility = .hidden
            window?.toolbarStyle = .unified
            window?.styleMask.insert(.fullSizeContentView)
            window?.isReleasedWhenClosed = false
            window?.delegate = delegate
            window?.contentView = NSHostingView(rootView: contentView)
        }

        window?.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }
}
