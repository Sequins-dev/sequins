import Foundation
import Testing
@testable import Sequins

struct AppPreferencesTests {
    @Test func firstLaunchShowsWindowUntilUserOptsOut() {
        let domain = "SequinsTests.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: domain)!
        defer { defaults.removePersistentDomain(forName: domain) }

        let preferences = AppPreferences(defaults: defaults)
        #expect(preferences.shouldOpenMainWindow(environment: [:]))
        preferences.showMainWindowOnLaunch = false
        let reloaded = AppPreferences(defaults: defaults)
        #expect(!reloaded.shouldOpenMainWindow(environment: [:]))
        #expect(reloaded.shouldOpenMainWindow(environment: ["SEQUINS_AUTO_OPEN_WINDOW": "Health"]))
        reloaded.showMainWindowOnLaunch = true
        #expect(AppPreferences(defaults: defaults).shouldOpenMainWindow(environment: [:]))
        // UserDefaults launch arguments use strings rather than stored Bool values.
        defaults.set("NO", forKey: "showMainWindowOnLaunch")
        #expect(!AppPreferences(defaults: defaults).shouldOpenMainWindow(environment: [:]))
    }

    @Test func welcomeAcknowledgementPersistsIndependentlyOfLaunchPreference() {
        let domain = "SequinsTests.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: domain)!
        defer { defaults.removePersistentDomain(forName: domain) }

        let preferences = AppPreferences(defaults: defaults)
        #expect(!preferences.hasAcknowledgedWelcome)
        preferences.hasAcknowledgedWelcome = true
        let reloaded = AppPreferences(defaults: defaults)
        #expect(reloaded.hasAcknowledgedWelcome)
        #expect(reloaded.showMainWindowOnLaunch)
    }
}
