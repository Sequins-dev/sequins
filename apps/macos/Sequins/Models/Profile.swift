//
//  Profile.swift
//  Sequins
//
//  Profile UI types — enum labels, colour scheme, export formats.
//  The runtime flamegraph model lives in SequinsData.FlamegraphFeed.
//

import Foundation
import SwiftUI

// MARK: - Profile Type

enum ProfileType: String, CaseIterable, Codable, Identifiable {
    case cpu = "cpu"
    case heap = "heap"
    case allocation = "allocation"

    var id: String { rawValue }

    var displayName: String {
        switch self {
        case .cpu: return "CPU"
        case .heap: return "Heap"
        case .allocation: return "Allocation"
        }
    }

    var unit: String {
        switch self {
        case .cpu: return "nanoseconds"
        case .heap: return "bytes"
        case .allocation: return "count"
        }
    }

    var icon: String {
        switch self {
        case .cpu: return "cpu"
        case .heap, .allocation: return "memorychip"
        }
    }
}

// MARK: - Profile Color Scheme

struct ProfileColorScheme {
    func colorForRatio(_ ratio: Double) -> Color {
        let clampedRatio = min(max(ratio, 0), 1)
        let hue = 0.6 // Blue hue
        let saturation = 0.05 + 0.95 * clampedRatio
        let brightness = 0.3 + 0.7 * clampedRatio
        return Color(hue: hue, saturation: saturation, brightness: brightness)
    }
}

// MARK: - Export Format

enum ProfileExportFormat {
    case pprof
    case speedscope
    case json

    var fileExtension: String {
        switch self {
        case .pprof: return "pb.gz"
        case .speedscope: return "speedscope.json"
        case .json: return "json"
        }
    }
}
