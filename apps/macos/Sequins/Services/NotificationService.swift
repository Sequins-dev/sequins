//
//  NotificationService.swift
//  Sequins
//
//  Handles macOS notifications for health status changes.
//  Clicking a notification opens the app with the relevant environment and service selected.
//

import Foundation
import UserNotifications
import SequinsData

/// Service for managing health-related notifications
@MainActor
final class NotificationService: NSObject, ObservableObject, UNUserNotificationCenterDelegate {

    /// Shared instance
    static let shared = NotificationService()

    /// Whether notifications are authorized
    @Published private(set) var isAuthorized = false

    /// Callback for handling notification clicks
    /// Parameters: serviceName, environmentId
    var onNotificationClicked: ((String, String?) -> Void)?

    /// UNUserNotificationCenter requires a bundle identifier — returns nil when running without one
    private var notificationCenter: UNUserNotificationCenter? {
        guard Bundle.main.bundleIdentifier != nil else { return nil }
        return UNUserNotificationCenter.current()
    }

    private override init() {
        super.init()
        notificationCenter?.delegate = self
    }

    // MARK: - Authorization

    /// Request notification authorization
    func requestAuthorization() async {
        guard let center = notificationCenter else { return }
        do {
            let granted = try await center.requestAuthorization(options: [.alert, .sound, .badge])
            await MainActor.run {
                self.isAuthorized = granted
            }
            if granted {
                print("Notifications authorized")
            } else {
                print("Notifications denied")
            }
        } catch {
            print("Failed to request notification authorization: \(error)")
        }
    }

    /// Check current authorization status
    func checkAuthorizationStatus() async {
        guard let center = notificationCenter else { return }
        let settings = await center.notificationSettings()
        await MainActor.run {
            self.isAuthorized = settings.authorizationStatus == .authorized
        }
    }

    // MARK: - Showing Notifications

    /// Show a warning notification (ephemeral - auto-dismisses after a while)
    ///
    /// - Parameters:
    ///   - serviceName: The service that triggered the warning
    ///   - environmentId: The environment ID (for navigation)
    ///   - message: The notification message
    func showWarningNotification(
        serviceName: String,
        environmentId: String?,
        message: String
    ) {
        guard let center = notificationCenter else { return }

        let content = UNMutableNotificationContent()
        content.title = "Health Warning: \(serviceName)"
        content.body = message
        content.sound = .default

        // Store navigation info in userInfo
        var userInfo: [String: String] = ["serviceName": serviceName, "status": "warning"]
        if let envId = environmentId {
            userInfo["environmentId"] = envId
        }
        content.userInfo = userInfo

        // Category for warning (could add custom actions in future)
        content.categoryIdentifier = "HEALTH_WARNING"

        let request = UNNotificationRequest(
            identifier: "health-warning-\(serviceName)-\(Date().timeIntervalSince1970)",
            content: content,
            trigger: nil // Deliver immediately
        )

        center.add(request) { error in
            if let error = error {
                print("Failed to show warning notification: \(error)")
            }
        }
    }

    /// Show an error notification (persistent - should require user attention)
    ///
    /// - Parameters:
    ///   - serviceName: The service that triggered the error
    ///   - environmentId: The environment ID (for navigation)
    ///   - message: The notification message
    func showErrorNotification(
        serviceName: String,
        environmentId: String?,
        message: String
    ) {
        guard let center = notificationCenter else { return }

        let content = UNMutableNotificationContent()
        content.title = "Health Alert: \(serviceName)"
        content.body = message
        content.sound = .defaultCritical

        // Store navigation info in userInfo
        var userInfo: [String: String] = ["serviceName": serviceName, "status": "error"]
        if let envId = environmentId {
            userInfo["environmentId"] = envId
        }
        content.userInfo = userInfo

        // Category for error (could add custom actions in future)
        content.categoryIdentifier = "HEALTH_ERROR"

        // Use a stable identifier for error notifications so we can update them
        // rather than creating duplicates
        let request = UNNotificationRequest(
            identifier: "health-error-\(serviceName)",
            content: content,
            trigger: nil // Deliver immediately
        )

        center.add(request) { error in
            if let error = error {
                print("Failed to show error notification: \(error)")
            }
        }
    }

    /// Show a recovery notification (when health improves)
    ///
    /// - Parameters:
    ///   - serviceName: The service that recovered
    ///   - environmentId: The environment ID
    ///   - message: The notification message
    func showRecoveryNotification(
        serviceName: String,
        environmentId: String?,
        message: String
    ) {
        guard let center = notificationCenter else { return }

        // Remove any existing error notification for this service
        center.removeDeliveredNotifications(withIdentifiers: ["health-error-\(serviceName)"])

        let content = UNMutableNotificationContent()
        content.title = "Health Recovered: \(serviceName)"
        content.body = message
        content.sound = .default

        var userInfo: [String: String] = ["serviceName": serviceName, "status": "recovered"]
        if let envId = environmentId {
            userInfo["environmentId"] = envId
        }
        content.userInfo = userInfo

        content.categoryIdentifier = "HEALTH_RECOVERY"

        let request = UNNotificationRequest(
            identifier: "health-recovery-\(serviceName)-\(Date().timeIntervalSince1970)",
            content: content,
            trigger: nil
        )

        center.add(request) { error in
            if let error = error {
                print("Failed to show recovery notification: \(error)")
            }
        }
    }

    // MARK: - UNUserNotificationCenterDelegate

    /// Handle notification when app is in foreground
    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        // Show notifications even when app is in foreground
        completionHandler([.banner, .sound])
    }

    /// Handle notification click
    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse,
        withCompletionHandler completionHandler: @escaping () -> Void
    ) {
        let userInfo = response.notification.request.content.userInfo

        if let serviceName = userInfo["serviceName"] as? String {
            let environmentId = userInfo["environmentId"] as? String

            Task { @MainActor in
                self.onNotificationClicked?(serviceName, environmentId)
            }
        }

        completionHandler()
    }

    // MARK: - Notification Categories

    /// Register notification categories (for custom actions)
    func registerCategories() {
        guard let center = notificationCenter else { return }

        let warningCategory = UNNotificationCategory(
            identifier: "HEALTH_WARNING",
            actions: [],
            intentIdentifiers: [],
            options: []
        )

        let errorCategory = UNNotificationCategory(
            identifier: "HEALTH_ERROR",
            actions: [],
            intentIdentifiers: [],
            options: []
        )

        let recoveryCategory = UNNotificationCategory(
            identifier: "HEALTH_RECOVERY",
            actions: [],
            intentIdentifiers: [],
            options: []
        )

        center.setNotificationCategories([
            warningCategory,
            errorCategory,
            recoveryCategory
        ])
    }
}
