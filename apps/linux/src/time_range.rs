//! Time range types shared by all tab components.

/// Window of recent data — used by the non-metrics tabs (Logs, Traces, Health, Profiles).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeRange {
    Min15,
    #[default]
    Hour1,
    Hour6,
    Hour24,
}

impl TimeRange {
    /// SeQL clause, e.g. `"last 1h"`.
    pub fn seql_window(self) -> &'static str {
        match self {
            Self::Min15 => "last 15m",
            Self::Hour1 => "last 1h",
            Self::Hour6 => "last 6h",
            Self::Hour24 => "last 24h",
        }
    }

    /// Short label for the dropdown.
    pub fn label(self) -> &'static str {
        match self {
            Self::Min15 => "15 min",
            Self::Hour1 => "1 hour",
            Self::Hour6 => "6 hours",
            Self::Hour24 => "24 hours",
        }
    }

    /// All variants in ascending order, matching the dropdown indices 0..3.
    pub const ALL: [TimeRange; 4] = [
        TimeRange::Min15,
        TimeRange::Hour1,
        TimeRange::Hour6,
        TimeRange::Hour24,
    ];
}

// ── Live/Paused time ranges ────────────────────────────────────────────────────

/// Time window options available in live (streaming) mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LiveRange {
    Min1,
    Min5,
    #[default]
    Min15,
    Min30,
    Hour1,
    Hour6,
}

impl LiveRange {
    pub fn seql_window(self) -> &'static str {
        match self {
            Self::Min1 => "last 1m",
            Self::Min5 => "last 5m",
            Self::Min15 => "last 15m",
            Self::Min30 => "last 30m",
            Self::Hour1 => "last 1h",
            Self::Hour6 => "last 6h",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Min1 => "1 min",
            Self::Min5 => "5 min",
            Self::Min15 => "15 min",
            Self::Min30 => "30 min",
            Self::Hour1 => "1 hour",
            Self::Hour6 => "6 hours",
        }
    }

    /// Window width in nanoseconds.
    pub fn duration_ns(self) -> i64 {
        const MIN: i64 = 60_000_000_000;
        const HOUR: i64 = 3_600_000_000_000;
        match self {
            Self::Min1 => MIN,
            Self::Min5 => 5 * MIN,
            Self::Min15 => 15 * MIN,
            Self::Min30 => 30 * MIN,
            Self::Hour1 => HOUR,
            Self::Hour6 => 6 * HOUR,
        }
    }

    pub const ALL: [LiveRange; 6] = [
        LiveRange::Min1,
        LiveRange::Min5,
        LiveRange::Min15,
        LiveRange::Min30,
        LiveRange::Hour1,
        LiveRange::Hour6,
    ];
}

/// Time window options available in paused (snapshot) mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PausedRange {
    Min15,
    #[default]
    Hour1,
    Hour6,
    Hour24,
    Day7,
}

impl PausedRange {
    pub fn seql_window(self) -> &'static str {
        match self {
            Self::Min15 => "last 15m",
            Self::Hour1 => "last 1h",
            Self::Hour6 => "last 6h",
            Self::Hour24 => "last 24h",
            Self::Day7 => "last 7d",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Min15 => "15 min",
            Self::Hour1 => "1 hour",
            Self::Hour6 => "6 hours",
            Self::Hour24 => "24 hours",
            Self::Day7 => "7 days",
        }
    }

    pub fn duration_ns(self) -> i64 {
        const MIN: i64 = 60_000_000_000;
        const HOUR: i64 = 3_600_000_000_000;
        const DAY: i64 = 24 * HOUR;
        match self {
            Self::Min15 => 15 * MIN,
            Self::Hour1 => HOUR,
            Self::Hour6 => 6 * HOUR,
            Self::Hour24 => DAY,
            Self::Day7 => 7 * DAY,
        }
    }

    pub const ALL: [PausedRange; 5] = [
        PausedRange::Min15,
        PausedRange::Hour1,
        PausedRange::Hour6,
        PausedRange::Hour24,
        PausedRange::Day7,
    ];
}

/// Unified time range used by the app-level filter bar and metrics tab.
/// Carries the current mode (Live/Paused) and the selected duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppTimeRange {
    /// Streaming live query with sliding window.
    Live(LiveRange),
    /// Snapshot query with a fixed time window.
    Paused(PausedRange),
}

impl Default for AppTimeRange {
    fn default() -> Self {
        Self::Live(LiveRange::default())
    }
}

impl AppTimeRange {
    pub fn seql_window(self) -> &'static str {
        match self {
            Self::Live(lr) => lr.seql_window(),
            Self::Paused(pr) => pr.seql_window(),
        }
    }

    pub fn is_live(self) -> bool {
        matches!(self, Self::Live(_))
    }

    pub fn duration_ns(self) -> i64 {
        match self {
            Self::Live(lr) => lr.duration_ns(),
            Self::Paused(pr) => pr.duration_ns(),
        }
    }
}

/// Best-fit conversion for forwarding time range changes to legacy tabs.
impl From<AppTimeRange> for TimeRange {
    fn from(atr: AppTimeRange) -> Self {
        match atr {
            AppTimeRange::Live(LiveRange::Min1)
            | AppTimeRange::Live(LiveRange::Min5)
            | AppTimeRange::Live(LiveRange::Min15)
            | AppTimeRange::Paused(PausedRange::Min15) => TimeRange::Min15,

            AppTimeRange::Live(LiveRange::Min30)
            | AppTimeRange::Live(LiveRange::Hour1)
            | AppTimeRange::Paused(PausedRange::Hour1) => TimeRange::Hour1,

            AppTimeRange::Live(LiveRange::Hour6) | AppTimeRange::Paused(PausedRange::Hour6) => {
                TimeRange::Hour6
            }

            AppTimeRange::Paused(PausedRange::Hour24) | AppTimeRange::Paused(PausedRange::Day7) => {
                TimeRange::Hour24
            }
        }
    }
}
