import Foundation
import Security

/// Minimal macOS Keychain wrapper for the assistant's per-environment secret (the LLM
/// API key for local, or the bearer token for remote). Secrets are keyed by the
/// environment's UUID so each environment has its own credential.
struct KeychainStore {
    static let shared = KeychainStore()

    private let service = "com.sequins.assistant"

    private init() {}

    private func account(for environmentId: UUID) -> String {
        "assistant-secret-\(environmentId.uuidString)"
    }

    /// Read the assistant secret for an environment, or `nil` if unset.
    func assistantSecret(environmentId: UUID) -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account(for: environmentId),
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var item: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &item)
        guard status == errSecSuccess,
              let data = item as? Data,
              let value = String(data: data, encoding: .utf8),
              !value.isEmpty
        else { return nil }
        return value
    }

    /// Store (or clear, when `secret` is nil/empty) the assistant secret for an environment.
    @discardableResult
    func setAssistantSecret(_ secret: String?, environmentId: UUID) -> Bool {
        let account = account(for: environmentId)
        let base: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
        ]

        // Clearing.
        guard let secret, !secret.isEmpty else {
            let status = SecItemDelete(base as CFDictionary)
            return status == errSecSuccess || status == errSecItemNotFound
        }

        let data = Data(secret.utf8)
        // Update if present, else add.
        let attributes: [String: Any] = [kSecValueData as String: data]
        let updateStatus = SecItemUpdate(base as CFDictionary, attributes as CFDictionary)
        if updateStatus == errSecSuccess { return true }
        if updateStatus == errSecItemNotFound {
            var addQuery = base
            addQuery[kSecValueData as String] = data
            addQuery[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlock
            return SecItemAdd(addQuery as CFDictionary, nil) == errSecSuccess
        }
        return false
    }
}
