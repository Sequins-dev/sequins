import Foundation

/// Minimal CBOR decoder for `_overflow_attrs` attribute values.
///
/// Handles only the types produced by ciborium in `sequins-otlp/overflow_map.rs`:
/// unsigned/negative integer, UTF-8 text string, float (half/single/double),
/// bool, null, and typed arrays of the above.
///
/// CBOR spec: RFC 7049 / RFC 8949.
func cborDecode(_ data: Data) -> AttributeValue? {
    var offset = 0
    return cborDecodeValue(data, offset: &offset)
}

private func cborDecodeValue(_ data: Data, offset: inout Int) -> AttributeValue? {
    guard offset < data.count else { return nil }
    let initial = data[offset]
    offset += 1
    let major = initial >> 5
    let info  = initial & 0x1F

    switch major {
    case 0: // Unsigned integer
        guard let n = cborReadUInt(info, data: data, offset: &offset) else { return nil }
        return .int(Int64(bitPattern: n))

    case 1: // Negative integer: value = -1 - n
        guard let n = cborReadUInt(info, data: data, offset: &offset) else { return nil }
        // Clamp to avoid overflow for n > Int64.max
        let neg: Int64 = n > UInt64(Int64.max) ? Int64.min : -1 - Int64(n)
        return .int(neg)

    case 3: // UTF-8 text string
        guard let len = cborReadUInt(info, data: data, offset: &offset) else { return nil }
        let byteLen = Int(len)
        guard offset + byteLen <= data.count else { return nil }
        let bytes = data[offset ..< (offset + byteLen)]
        offset += byteLen
        guard let s = String(bytes: bytes, encoding: .utf8) else { return nil }
        return .string(s)

    case 4: // Array
        guard let count = cborReadUInt(info, data: data, offset: &offset) else { return nil }
        var items: [AttributeValue] = []
        items.reserveCapacity(Int(min(count, 1024)))
        for _ in 0..<count {
            if let item = cborDecodeValue(data, offset: &offset) {
                items.append(item)
            }
        }
        return cborArrayToAttributeValue(items)

    case 7: // Floats and simple values
        switch info {
        case 20: return .bool(false)   // 0xF4
        case 21: return .bool(true)    // 0xF5
        case 22: return nil            // 0xF6 null
        case 25:                       // Float16 (0xF9) — 2 bytes
            guard offset + 2 <= data.count else { return nil }
            let bits = UInt16(data[offset]) << 8 | UInt16(data[offset + 1])
            offset += 2
            return .double(Double(cborHalfToFloat(bits)))
        case 26:                       // Float32 (0xFA) — 4 bytes
            guard offset + 4 <= data.count else { return nil }
            let bits = UInt32(data[offset]) << 24 | UInt32(data[offset+1]) << 16
                     | UInt32(data[offset+2]) << 8  | UInt32(data[offset+3])
            offset += 4
            return .double(Double(Float(bitPattern: bits)))
        case 27:                       // Float64 (0xFB) — 8 bytes
            guard offset + 8 <= data.count else { return nil }
            var bits: UInt64 = 0
            for i in 0..<8 { bits = (bits << 8) | UInt64(data[offset + i]) }
            offset += 8
            return .double(Double(bitPattern: bits))
        default:
            return nil
        }

    default:
        return nil
    }
}

/// Read a CBOR unsigned integer from the stream (inline or following bytes).
private func cborReadUInt(_ info: UInt8, data: Data, offset: inout Int) -> UInt64? {
    if info <= 23 { return UInt64(info) }
    switch info {
    case 24:
        guard offset < data.count else { return nil }
        defer { offset += 1 }
        return UInt64(data[offset])
    case 25:
        guard offset + 2 <= data.count else { return nil }
        defer { offset += 2 }
        return UInt64(data[offset]) << 8 | UInt64(data[offset + 1])
    case 26:
        guard offset + 4 <= data.count else { return nil }
        defer { offset += 4 }
        return UInt64(data[offset]) << 24 | UInt64(data[offset+1]) << 16
             | UInt64(data[offset+2]) << 8  | UInt64(data[offset+3])
    case 27:
        guard offset + 8 <= data.count else { return nil }
        var v: UInt64 = 0
        for i in 0..<8 { v = (v << 8) | UInt64(data[offset + i]) }
        offset += 8
        return v
    default:
        return nil
    }
}

/// Convert a decoded CBOR array to a typed `AttributeValue` array variant.
private func cborArrayToAttributeValue(_ items: [AttributeValue]) -> AttributeValue {
    guard !items.isEmpty else { return .stringArray([]) }
    var strings: [String]   = []
    var ints:    [Int64]    = []
    var doubles: [Double]   = []
    var bools:   [Bool]     = []
    for item in items {
        switch item {
        case .string(let s): strings.append(s)
        case .int(let i):    ints.append(i)
        case .double(let d): doubles.append(d)
        case .bool(let b):   bools.append(b)
        default:             break
        }
    }
    if strings.count == items.count { return .stringArray(strings) }
    if ints.count    == items.count { return .intArray(ints)       }
    if doubles.count == items.count { return .doubleArray(doubles) }
    if bools.count   == items.count { return .boolArray(bools)     }
    return .stringArray(items.map { "\($0)" }) // mixed fallback
}

/// Convert an IEEE 754 half-precision float (16-bit) to Float.
private func cborHalfToFloat(_ bits: UInt16) -> Float {
    let sign: Float = (bits & 0x8000) != 0 ? -1 : 1
    let exp  = Int((bits >> 10) & 0x1F)
    let mant = Int(bits & 0x3FF)
    if exp == 0  { return sign * Float(mant) * pow(2, -24) }
    if exp == 31 { return mant == 0 ? sign * .infinity : .nan }
    return sign * Float(mant + 0x400) * pow(2, Float(exp - 25))
}
