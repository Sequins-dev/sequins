use leptos::prelude::*;
use leptos_meta::*;

#[cfg(feature = "ssr")]
#[component]
pub fn shell() -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width, initial-scale=1"/>
                <link rel="preconnect" href="https://fonts.googleapis.com"/>
                <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin="anonymous"/>
                <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700;800&display=swap" rel="stylesheet"/>
                <link rel="stylesheet" href="/pkg/sequins-web.css"/>
                <MetaTags/>
            </head>
            <body>
                <App/>
            </body>
        </html>
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Theme {
    Light,
    Dark,
    Auto,
}

impl Theme {
    fn as_str(&self) -> &'static str {
        match self {
            Theme::Light => "light",
            Theme::Dark => "dark",
            Theme::Auto => "auto",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "dark" => Theme::Dark,
            "light" => Theme::Light,
            _ => Theme::Auto,
        }
    }
}

#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();

    // Initialize theme state
    let (theme, set_theme) = signal(Theme::Auto);

    // Load theme from localStorage on mount
    Effect::new(move |_| {
        if let Some(window) = web_sys::window() {
            if let Ok(Some(storage)) = window.local_storage() {
                if let Ok(Some(saved_theme)) = storage.get_item("theme") {
                    set_theme.set(Theme::from_str(&saved_theme));
                }
            }
        }
    });

    // Apply theme to document
    Effect::new(move |_| {
        if let Some(window) = web_sys::window() {
            if let Some(document) = window.document() {
                if let Some(html) = document.document_element() {
                    let theme_value = theme.get();
                    let _ = html.set_attribute("data-theme", theme_value.as_str());

                    // Save to localStorage
                    if let Ok(Some(storage)) = window.local_storage() {
                        let _ = storage.set_item("theme", theme_value.as_str());
                    }
                }
            }
        }
    });

    view! {
        <Title text="Sequins - Local-First OpenTelemetry Observability" />
        <Meta name="description" content="Free desktop app for visualizing OpenTelemetry traces, logs, metrics, and profiles. Enterprise-ready on-prem OTLP database." />

        <Header theme=theme set_theme=set_theme />
        <Hero />
        <QuickStart />
        <Features />
        <ScreenshotsDemo />
        <ComparisonTable />
        <Pricing />
        <Footer />
    }
}

#[component]
fn Header(theme: ReadSignal<Theme>, set_theme: WriteSignal<Theme>) -> impl IntoView {
    view! {
        <header class="header">
            <nav class="nav">
                <a href="#" class="logo">
                    <img src="logo.png" alt="Sequins" class="logo-img" />
                    <span class="logo-text">"Sequins"</span>
                </a>
                <div class="nav-right">
                    <ul class="nav-links">
                        <li><a href="#features">"Features"</a></li>
                        <li><a href="#pricing">"Pricing"</a></li>
                        <li><a href="https://github.com/Sequins-dev/sequins-pro" target="_blank">"GitHub"</a></li>
                        <li><a href="#docs">"Docs"</a></li>
                    </ul>
                    <ThemeSwitcher theme=theme set_theme=set_theme />
                </div>
            </nav>
        </header>
    }
}

#[component]
fn ThemeSwitcher(theme: ReadSignal<Theme>, set_theme: WriteSignal<Theme>) -> impl IntoView {
    view! {
        <div class="theme-switcher">
            <button
                class=move || if theme.get() == Theme::Light { "theme-btn active" } else { "theme-btn" }
                on:click=move |_| set_theme.set(Theme::Light)
                aria-label="Light mode"
            >
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <circle cx="12" cy="12" r="5" stroke="currentColor" stroke-width="2"/>
                    <path d="M12 1V3M12 21V23M23 12H21M3 12H1M20.485 3.515L19.071 4.929M4.929 19.071L3.515 20.485M20.485 20.485L19.071 19.071M4.929 4.929L3.515 3.515" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                </svg>
            </button>
            <button
                class=move || if theme.get() == Theme::Auto { "theme-btn active" } else { "theme-btn" }
                on:click=move |_| set_theme.set(Theme::Auto)
                aria-label="Auto mode"
            >
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <rect x="3" y="3" width="18" height="18" rx="2" stroke="currentColor" stroke-width="2"/>
                    <path d="M3 3L21 21" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                </svg>
            </button>
            <button
                class=move || if theme.get() == Theme::Dark { "theme-btn active" } else { "theme-btn" }
                on:click=move |_| set_theme.set(Theme::Dark)
                aria-label="Dark mode"
            >
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                </svg>
            </button>
        </div>
    }
}

#[component]
fn Hero() -> impl IntoView {
    view! {
        <section class="hero">
            <div class="hero-decorations">
                <div class="hero-circle hero-circle-1"></div>
                <div class="hero-circle hero-circle-2"></div>
                <div class="hero-circle hero-circle-3"></div>
            </div>
            <div class="hero-content">
                <h1>"Not just the Observability you have at home."</h1>
                <p class="subtitle">
                    "Free OpenTelemetry capture and visualization right on your local machine."
                    <br />
                    "Single-step self-hosting for your whole team."
                </p>
                <div class="cta-buttons">
                    // <a href="#download" class="btn btn-primary">"Download for Mac"</a>
                    <a href="#pricing" class="btn btn-secondary btn-glass">"Explore Team Solutions →"</a>
                </div>
            </div>
        </section>
    }
}

#[component]
fn QuickStart() -> impl IntoView {
    view! {
            <section class="section" id="quick-start">
                <div class="section-header">
                    <h2>"Up and Running in Minutes"</h2>
                    <p>"No infrastructure to set up. No accounts to create. Just point your app and go."</p>
                </div>
                <div class="quick-start-steps">
                    <div class="quick-start-step">
                        <div class="step-number">"1"</div>
                        <h3>"Download"</h3>
                        <p>"Get the desktop app for Mac, Linux, or Windows"</p>
                        <div class="step-note">"Coming soon - sign up for early access"</div>
                    </div>
                    <div class="quick-start-step">
                        <div class="step-number">"2"</div>
                        <h3>"Configure"</h3>
                        <p>"Point your OpenTelemetry SDK to localhost"</p>
                        <pre class="code-snippet">
    <code>"OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
    OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf"</code>
                        </pre>
                    </div>
                    <div class="quick-start-step">
                        <div class="step-number">"3"</div>
                        <h3>"Observe"</h3>
                        <p>"Start your app and see telemetry immediately"</p>
                        <div class="step-note">"No sign-up, no configuration, just works"</div>
                    </div>
                </div>
            </section>
        }
}

#[component]
fn Features() -> impl IntoView {
    view! {
        <ObservabilityTypes />
        <CoreFeatures />
        <AdvancedFeatures />
    }
}

#[component]
fn ObservabilityTypes() -> impl IntoView {
    view! {
        <section class="section bg-light" id="observability">
            <div class="section-header">
                <h2>"Complete Observability Stack"</h2>
                <p>"All four pillars of observability, beautifully integrated"</p>
            </div>
            <div class="observability-grid-container">
                <div class="observability-grid">
                    <FeatureCard
                        icon="📊"
                        title="Metrics"
                        description="Track application performance with customizable dashboards. Monitor latency, throughput, and error rates at a glance."
                    />
                    <FeatureCard
                        icon="📝"
                        title="Logs"
                        description="Search structured logs with full-text search and filtering. Link directly to related traces for complete context."
                    />
                    <FeatureCard
                        icon="🎯"
                        title="Distributed Tracing"
                        description="Visualize request flows across services with interactive trace timelines. See the full picture of your distributed system."
                    />
                    <FeatureCard
                        icon="🔥"
                        title="Profiling"
                        description="Identify performance bottlenecks with interactive flame graphs. Native support for pprof and other standard formats."
                    />
                </div>
                <div class="observability-connector">
                    <svg class="data-flow-diagram" viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">
                        // Gradients with fade on both ends (0 -> 1 -> 0)
                        <defs>
                            <linearGradient id="flowGradientFade1">
                                <stop offset="0%" style="stop-color:#3b82f6;stop-opacity:0"/>
                                <stop offset="50%" style="stop-color:#3b82f6;stop-opacity:1"/>
                                <stop offset="100%" style="stop-color:#3b82f6;stop-opacity:0"/>
                            </linearGradient>
                            <linearGradient id="flowGradientFade2">
                                <stop offset="0%" style="stop-color:#8b5cf6;stop-opacity:0"/>
                                <stop offset="50%" style="stop-color:#8b5cf6;stop-opacity:1"/>
                                <stop offset="100%" style="stop-color:#8b5cf6;stop-opacity:0"/>
                            </linearGradient>
                            <linearGradient id="flowGradientFade3">
                                <stop offset="0%" style="stop-color:#ec4899;stop-opacity:0"/>
                                <stop offset="50%" style="stop-color:#ec4899;stop-opacity:1"/>
                                <stop offset="100%" style="stop-color:#ec4899;stop-opacity:0"/>
                            </linearGradient>
                            <linearGradient id="flowGradientFade4">
                                <stop offset="0%" style="stop-color:#06b6d4;stop-opacity:0"/>
                                <stop offset="50%" style="stop-color:#06b6d4;stop-opacity:1"/>
                                <stop offset="100%" style="stop-color:#06b6d4;stop-opacity:0"/>
                            </linearGradient>
                        </defs>

                        // Connection lines - VERY steep angles (>45 degrees) from card middles
                        // Top-left: steep angle from card to logo (shifted up for breathing room)
                        <line x1="50" y1="0" x2="90" y2="80" stroke="url(#flowGradientFade1)" stroke-width="2.5" stroke-dasharray="6,4" class="flow-line"/>
                        // Top-right: steep angle from card to logo (shifted up for breathing room)
                        <line x1="150" y1="0" x2="110" y2="80" stroke="url(#flowGradientFade2)" stroke-width="2.5" stroke-dasharray="6,4" class="flow-line"/>
                        // Bottom-left: steep angle from card to logo (shifted down for breathing room)
                        <line x1="50" y1="200" x2="90" y2="120" stroke="url(#flowGradientFade3)" stroke-width="2.5" stroke-dasharray="6,4" class="flow-line"/>
                        // Bottom-right: steep angle from card to logo (shifted down for breathing room)
                        <line x1="150" y1="200" x2="110" y2="120" stroke="url(#flowGradientFade4)" stroke-width="2.5" stroke-dasharray="6,4" class="flow-line"/>

                        // Hidden paths for animation
                        <path id="path1" d="M 50,0 L 90,80" fill="none"/>
                        <path id="path2" d="M 150,0 L 110,80" fill="none"/>
                        <path id="path3" d="M 50,200 L 90,120" fill="none"/>
                        <path id="path4" d="M 150,200 L 110,120" fill="none"/>

                        // Animated dots traveling along paths with fade effect
                        <circle r="4" fill="#3b82f6" class="flow-dot">
                            <animateMotion dur="2.5s" repeatCount="indefinite">
                                <mpath href="#path1"/>
                            </animateMotion>
                            <animate attributeName="opacity" values="0;1;0" dur="2.5s" repeatCount="indefinite"/>
                        </circle>
                        <circle r="4" fill="#8b5cf6" class="flow-dot">
                            <animateMotion dur="2.5s" repeatCount="indefinite" begin="0.6s">
                                <mpath href="#path2"/>
                            </animateMotion>
                            <animate attributeName="opacity" values="0;1;0" dur="2.5s" repeatCount="indefinite" begin="0.6s"/>
                        </circle>
                        <circle r="4" fill="#ec4899" class="flow-dot">
                            <animateMotion dur="2.5s" repeatCount="indefinite" begin="1.2s">
                                <mpath href="#path3"/>
                            </animateMotion>
                            <animate attributeName="opacity" values="0;1;0" dur="2.5s" repeatCount="indefinite" begin="1.2s"/>
                        </circle>
                        <circle r="4" fill="#06b6d4" class="flow-dot">
                            <animateMotion dur="2.5s" repeatCount="indefinite" begin="1.8s">
                                <mpath href="#path4"/>
                            </animateMotion>
                            <animate attributeName="opacity" values="0;1;0" dur="2.5s" repeatCount="indefinite" begin="1.8s"/>
                        </circle>

                        // OpenTelemetry icon (from icepanel.io)
                        <svg x="70" y="70" width="60" height="60" viewBox="0 0 128 128" xmlns="http://www.w3.org/2000/svg">
                            <path fill="#f5a800" d="M67.648 69.797c-5.246 5.25-5.246 13.758 0 19.008 5.25 5.246 13.758 5.246 19.004 0 5.25-5.25 5.25-13.758 0-19.008-5.246-5.246-13.754-5.246-19.004 0Zm14.207 14.219a6.649 6.649 0 0 1-9.41 0 6.65 6.65 0 0 1 0-9.407 6.649 6.649 0 0 1 9.41 0c2.598 2.586 2.598 6.809 0 9.407ZM86.43 3.672l-8.235 8.234a4.17 4.17 0 0 0 0 5.875l32.149 32.149a4.17 4.17 0 0 0 5.875 0l8.234-8.235c1.61-1.61 1.61-4.261 0-5.87L92.29 3.671a4.159 4.159 0 0 0-5.86 0ZM28.738 108.895a3.763 3.763 0 0 0 0-5.31l-4.183-4.187a3.768 3.768 0 0 0-5.313 0l-8.644 8.649-.016.012-2.371-2.375c-1.313-1.313-3.45-1.313-4.75 0-1.313 1.312-1.313 3.449 0 4.75l14.246 14.242a3.353 3.353 0 0 0 4.746 0c1.3-1.313 1.313-3.45 0-4.746l-2.375-2.375.016-.012Zm0 0"/>
                            <path fill="#425cc7" d="M72.297 27.313 54.004 45.605c-1.625 1.625-1.625 4.301 0 5.926L65.3 62.824c7.984-5.746 19.18-5.035 26.363 2.153l9.148-9.149c1.622-1.625 1.622-4.297 0-5.922L78.22 27.313a4.185 4.185 0 0 0-5.922 0ZM60.55 67.585l-6.672-6.672c-1.563-1.562-4.125-1.562-5.684 0l-23.53 23.54a4.036 4.036 0 0 0 0 5.687l13.331 13.332a4.036 4.036 0 0 0 5.688 0l15.132-15.157c-3.199-6.609-2.625-14.593 1.735-20.73Zm0 0"/>
                        </svg>
                    </svg>
                </div>
            </div>
        </section>
    }
}

#[component]
fn CoreFeatures() -> impl IntoView {
    view! {
        <section class="section" id="core-features">
            <div class="section-header">
                <h2>"Local-First Architecture"</h2>
                <p>"Everything you need, nothing you don't. No cloud required."</p>
            </div>
            <div class="features-grid">
                <FeatureCard
                    icon="⚡"
                    title="Embedded OTLP Collector"
                    description="Built-in OpenTelemetry collector with gRPC and HTTP support. Point your app to localhost:4317 and start immediately—no external dependencies."
                />
                <FeatureCard
                    icon="💾"
                    title="Local-First Storage"
                    description="All data stored locally in embedded database. Lightning-fast queries, zero network overhead, complete privacy. Perfect for development."
                />
                <FeatureCard
                    icon="🎨"
                    title="GPU-Accelerated UI"
                    description="Blazingly fast interface built with GPUI. Smoothly navigate millions of spans, thousands of logs, complex flame graphs—all at 60fps."
                />
            </div>
        </section>
    }
}

#[component]
fn AdvancedFeatures() -> impl IntoView {
    view! {
        <section class="section bg-light" id="advanced-features">
            <div class="section-header">
                <h2>"Power User Features"</h2>
                <p>"Designed for deep investigation and rapid debugging"</p>
            </div>
            <div class="features-grid">
                <FeatureCard
                    icon="🔗"
                    title="Trace-Log Linking"
                    description="Jump from a log line to its trace with one click. Or vice versa. See causality and context without switching tools or losing your place."
                />
                <FeatureCard
                    icon="🔍"
                    title="Full-Text Search"
                    description="Search across all telemetry with instant results. Filter by service, operation, status, tags, or any custom attribute. Regex support included."
                />
                <FeatureCard
                    icon="💾"
                    title="Persistent Filters"
                    description="Time ranges, services, and filters persist as you switch between tabs and views. Compare metrics across time periods without repeatedly reconfiguring."
                />
            </div>
        </section>
    }
}

#[component]
fn ScreenshotsDemo() -> impl IntoView {
    view! {
        <section class="section" id="screenshots">
            <div class="section-header">
                <h2>"See It in Action"</h2>
                <p>"A developer tool built by developers, for developers"</p>
            </div>
            <div class="demo-showcase">
                <div class="demo-placeholder">
                    <div class="demo-icon">"🎨"</div>
                    <h3>"Screenshots Coming Soon"</h3>
                    <p>"We're putting the finishing touches on the UI. Check back soon to see:"</p>
                    <ul class="demo-features">
                        <li>"Interactive trace timelines with sub-millisecond precision"</li>
                        <li>"Linked logs and traces in a unified view"</li>
                        <li>"Real-time metrics dashboards"</li>
                        <li>"Flame graphs for performance profiling"</li>
                    </ul>
                    <div class="demo-note">"Want early access? Star us on GitHub!"</div>
                </div>
            </div>
        </section>
    }
}

#[component]
fn ComparisonTable() -> impl IntoView {
    view! {
        <section class="section bg-light" id="comparison">
            <div class="section-header">
                <h2>"Simple. Powerful. Affordable."</h2>
                <p>"Enterprise observability without the enterprise price tag"</p>
            </div>
            <div class="comparison-container">
                <table class="comparison-table">
                    <thead>
                        <tr>
                            <th></th>
                            <th class="highlight-col">"Sequins Free"</th>
                            <th>"Commercial SaaS"</th>
                            <th class="highlight-col">"Sequins Enterprise"</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td class="feature-name">"Price"</td>
                            <td class="highlight-col"><strong>"$0"</strong>" forever"</td>
                            <td>"$15-100+/host/month"</td>
                            <td class="highlight-col"><strong>"Custom"</strong>" (fraction of SaaS)"</td>
                        </tr>
                        <tr>
                            <td class="feature-name">"Local Development"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" Built for it"</td>
                            <td><span class="cross">"✗"</span>" Cloud-only"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" Desktop + cloud"</td>
                        </tr>
                        <tr>
                            <td class="feature-name">"Data Privacy"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" 100% local"</td>
                            <td><span class="cross">"✗"</span>" Sent to vendor"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" On-prem option"</td>
                        </tr>
                        <tr>
                            <td class="feature-name">"Setup Time"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" <5 minutes"</td>
                            <td>"~1 hour+"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" <1 hour"</td>
                        </tr>
                        <tr>
                            <td class="feature-name">"Vendor Lock-in"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" Standard OTLP"</td>
                            <td><span class="warning">"⚠"</span>" Proprietary agents"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" Standard OTLP"</td>
                        </tr>
                        <tr>
                            <td class="feature-name">"Full Stack"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" Traces, Logs, Metrics, Profiles"</td>
                            <td><span class="check">"✓"</span>" Yes ($$$$)"</td>
                            <td class="highlight-col"><span class="check">"✓"</span>" Yes"</td>
                        </tr>
                    </tbody>
                </table>
                <p class="comparison-note">"Commercial pricing based on public data from Datadog, New Relic, and Honeycomb as of 2025."</p>
            </div>
        </section>
    }
}

#[component]
fn FeatureCard(
    icon: &'static str,
    title: &'static str,
    description: &'static str,
) -> impl IntoView {
    view! {
        <div class="feature-card">
            <div class="feature-icon">
                <div class="icon-emoji">{icon}</div>
                <svg class="icon-sparkle" width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M12 2L14.5 9.5L22 12L14.5 14.5L12 22L9.5 14.5L2 12L9.5 9.5L12 2Z" fill="url(#sparkle-gradient)" opacity="0.6"/>
                    <defs>
                        <linearGradient id="sparkle-gradient" x1="2" y1="2" x2="22" y2="22" gradientUnits="userSpaceOnUse">
                            <stop offset="0%" style="stop-color:#3b82f6"/>
                            <stop offset="50%" style="stop-color:#8b5cf6"/>
                            <stop offset="100%" style="stop-color:#ec4899"/>
                        </linearGradient>
                    </defs>
                </svg>
            </div>
            <h3>{title}</h3>
            <p>{description}</p>
        </div>
    }
}

#[component]
fn Pricing() -> impl IntoView {
    view! {
        <section class="section" id="pricing">
            <div class="section-header">
                <h2>"Free for individuals, affordable for teams"</h2>
                <p>"Start free on your local machine. Scale to enterprise when you're ready."</p>
            </div>
            <div class="pricing-grid">
                <PricingCard
                    title="Free"
                    price="$0"
                    period="forever"
                    features=vec![
                        "Desktop app for Mac, Linux, Windows",
                        "Embedded OTLP server (gRPC + HTTP)",
                        "Unlimited traces, logs, metrics, profiles",
                        "Local SQLite storage",
                        "Full-text search",
                        "Service map visualization",
                        "Export capabilities",
                    ]
                    cta_text="Download Now"
                    cta_link="#download"
                    featured=false
                />
                <PricingCard
                    title="Enterprise"
                    price="Custom"
                    period="contact us"
                    features=vec![
                        "Everything in Free, plus:",
                        "Centralized on-prem deployment",
                        "Multi-user collaboration",
                        "Remote query API",
                        "Authentication & authorization",
                        "High availability setup",
                        "Priority support",
                        "Custom retention policies",
                    ]
                    cta_text="Contact Sales"
                    cta_link="mailto:sales@sequins.dev"
                    featured=true
                />
            </div>
        </section>
    }
}

#[component]
fn PricingCard(
    title: &'static str,
    price: &'static str,
    period: &'static str,
    features: Vec<&'static str>,
    cta_text: &'static str,
    cta_link: &'static str,
    featured: bool,
) -> impl IntoView {
    let card_class = if featured {
        "pricing-card featured"
    } else {
        "pricing-card"
    };

    view! {
        <div class={card_class}>
            <h3>{title}</h3>
            <div class="price">
                {price}
                <span class="price-period">" / "{period}</span>
            </div>
            <ul class="features-list">
                {features.into_iter().map(|feature| {
                    view! { <li>{feature}</li> }
                }).collect::<Vec<_>>()}
            </ul>
            <a href={cta_link} class="btn btn-primary" style="width: 100%; text-align: center;">
                {cta_text}
            </a>
        </div>
    }
}

#[component]
fn Footer() -> impl IntoView {
    view! {
        <footer class="footer">
            <div class="footer-content">
                <ul class="footer-links">
                    <li><a href="#features">"Features"</a></li>
                    <li><a href="#pricing">"Pricing"</a></li>
                    <li><a href="https://github.com/Sequins-dev/sequins-pro" target="_blank">"GitHub"</a></li>
                    <li><a href="#docs">"Documentation"</a></li>
                    <li><a href="mailto:hello@sequins.dev">"Contact"</a></li>
                </ul>
                <p>"© 2025 Sequins."</p>
                <p>"OpenTelemetry is a CNCF project. Sequins is an independent visualization tool."</p>
            </div>
        </footer>
    }
}

// Hydrate the app when running in the browser (WASM)
// This function is called by the generated JS to initialize the client-side app
#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
    console_error_panic_hook::set_once();
    leptos::mount::hydrate_body(App);
}
