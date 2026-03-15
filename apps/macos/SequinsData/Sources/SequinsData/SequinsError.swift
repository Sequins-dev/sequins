import Foundation

/// Errors that can occur when interacting with the Sequins FFI
public enum SequinsError: Error, LocalizedError {
    /// An error occurred in the FFI layer
    case ffiError(String)

    /// A null pointer was encountered when a valid pointer was expected
    case nullPointer

    /// Invalid UTF-8 data was encountered
    case invalidUtf8

    public var errorDescription: String? {
        switch self {
        case .ffiError(let message):
            return message
        case .nullPointer:
            return "Unexpected null pointer"
        case .invalidUtf8:
            return "Invalid UTF-8 data encountered"
        }
    }
}
