use crate::ast::TimeRange;
use crate::parser::lexer::{uint_literal, ws, KW_LAST, KW_TODAY, KW_YESTERDAY};
use winnow::combinator::{alt, delimited, opt};
use winnow::token::literal;
use winnow::{ModalResult, Parser};

const NS_PER_MS: u64 = 1_000_000;
const NS_PER_SEC: u64 = 1_000_000_000;
const NS_PER_MIN: u64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: u64 = 3_600 * NS_PER_SEC;
const NS_PER_DAY: u64 = 86_400 * NS_PER_SEC;

/// Parse a duration literal like `500ms`, `1s`, `2m`, `1h`, `7d` → nanoseconds
pub fn duration_ns(input: &mut &str) -> ModalResult<u64> {
    let n = uint_literal.parse_next(input)?;
    let unit = alt((
        literal("ms").value(NS_PER_MS),
        literal("s").value(NS_PER_SEC),
        literal("m").value(NS_PER_MIN),
        literal("h").value(NS_PER_HOUR),
        literal("d").value(NS_PER_DAY),
    ))
    .parse_next(input)?;
    Ok(n * unit)
}

/// Parse `last <duration>` → [`TimeRange::SlidingWindow`]
pub fn parse_last(input: &mut &str) -> ModalResult<TimeRange> {
    literal(KW_LAST).parse_next(input)?;
    ws.parse_next(input)?;
    let ns = duration_ns.parse_next(input)?;
    Ok(TimeRange::SlidingWindow { start_ns: ns })
}

/// Parse `today` → absolute range covering today in UTC
pub fn parse_today(input: &mut &str) -> ModalResult<TimeRange> {
    literal(KW_TODAY).parse_next(input)?;
    let now_ns = now_ns();
    let day_ns = NS_PER_DAY;
    let start_ns = (now_ns / day_ns) * day_ns;
    Ok(TimeRange::Absolute {
        start_ns,
        end_ns: start_ns + day_ns,
    })
}

/// Parse `yesterday` → absolute range covering yesterday in UTC
pub fn parse_yesterday(input: &mut &str) -> ModalResult<TimeRange> {
    literal(KW_YESTERDAY).parse_next(input)?;
    let now_ns = now_ns();
    let day_ns = NS_PER_DAY;
    let today_start = (now_ns / day_ns) * day_ns;
    let start_ns = today_start - day_ns;
    Ok(TimeRange::Absolute {
        start_ns,
        end_ns: today_start,
    })
}

/// Parse `between(<timestamp>, <timestamp>)` → [`TimeRange::Absolute`]
pub fn parse_between(input: &mut &str) -> ModalResult<TimeRange> {
    literal("between").parse_next(input)?;
    let (start_ns, end_ns) = delimited(
        literal("("),
        (uint_literal, ws, opt(literal(",")), ws, uint_literal).map(|(s, _, _, _, e)| (s, e)),
        literal(")"),
    )
    .parse_next(input)?;
    Ok(TimeRange::Absolute { start_ns, end_ns })
}

/// Parse the time scope part of a query: `last <duration>`, `today`, `yesterday`, or `between(...)`
pub fn parse_time_scope(input: &mut &str) -> ModalResult<TimeRange> {
    alt((parse_last, parse_today, parse_yesterday, parse_between)).parse_next(input)
}

fn now_ns() -> u64 {
    sequins_types::NowTime::now_ns(&sequins_types::SystemNowTime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_ms() {
        assert_eq!(duration_ns.parse("500ms").unwrap(), 500 * NS_PER_MS);
    }

    #[test]
    fn parse_duration_s() {
        assert_eq!(duration_ns.parse("30s").unwrap(), 30 * NS_PER_SEC);
    }

    #[test]
    fn parse_duration_m() {
        assert_eq!(duration_ns.parse("15m").unwrap(), 15 * NS_PER_MIN);
    }

    #[test]
    fn parse_duration_h() {
        assert_eq!(duration_ns.parse("1h").unwrap(), NS_PER_HOUR);
    }

    #[test]
    fn parse_duration_d() {
        assert_eq!(duration_ns.parse("7d").unwrap(), 7 * NS_PER_DAY);
    }

    #[test]
    fn parse_last_1h() {
        let tr = parse_last.parse("last 1h").unwrap();
        assert_eq!(
            tr,
            TimeRange::SlidingWindow {
                start_ns: NS_PER_HOUR
            }
        );
    }

    #[test]
    fn parse_between_absolute() {
        let tr = parse_between.parse("between(1000, 2000)").unwrap();
        assert_eq!(
            tr,
            TimeRange::Absolute {
                start_ns: 1000,
                end_ns: 2000
            }
        );
    }

    #[test]
    fn parse_today_returns_absolute() {
        let tr = parse_today.parse("today").unwrap();
        assert!(matches!(tr, TimeRange::Absolute { .. }));
    }

    #[test]
    fn parse_yesterday_returns_absolute() {
        let tr = parse_yesterday.parse("yesterday").unwrap();
        assert!(matches!(tr, TimeRange::Absolute { .. }));
    }

    #[test]
    fn test_parse_zero_duration() {
        // `0s` is syntactically valid (parsed without error)
        // and produces a zero-nanosecond sliding window
        let tr = parse_last.parse("last 0s").unwrap();
        assert_eq!(tr, TimeRange::SlidingWindow { start_ns: 0 });
    }

    #[test]
    fn test_overflow_integer_gives_error() {
        // u64::MAX + 1 as a string — parse::<u64>() must fail, not panic
        let overflow = "18446744073709551616"; // u64::MAX + 1
        use super::super::lexer::uint_literal;
        let result = uint_literal.parse(overflow);
        assert!(
            result.is_err(),
            "u64::MAX+1 should produce a parse error, not panic"
        );
    }
}
