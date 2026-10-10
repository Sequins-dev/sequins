import Foundation
import Observation

/// App-wide UI preferences, independent of connection environments.
@Observable
final class AppPreferences {
    @ObservationIgnored private let defaults: UserDefaults

    var showMainWindowOnLaunch: Bool {
        didSet { defaults.set(showMainWindowOnLaunch, forKey: "showMainWindowOnLaunch") }
    }
    var hasAcknowledgedWelcome: Bool {
        didSet { defaults.set(hasAcknowledgedWelcome, forKey: "hasAcknowledgedWelcome") }
    }

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
        showMainWindowOnLaunch = defaults.object(forKey: "showMainWindowOnLaunch") == nil
            ? true : defaults.bool(forKey: "showMainWindowOnLaunch")
        hasAcknowledgedWelcome = defaults.bool(forKey: "hasAcknowledgedWelcome")
    }

    func shouldOpenMainWindow(environment: [String: String]) -> Bool {
        showMainWindowOnLaunch || environment["SEQUINS_AUTO_OPEN_WINDOW"] != nil
    }
}
