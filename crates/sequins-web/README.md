# Sequins Marketing Website

This is the marketing website for Sequins, built with [Leptos](https://leptos.dev/) - a full-stack Rust web framework.

## Purpose

The website serves as a promotional vehicle for:
- **Free Desktop App**: Local-first OpenTelemetry visualization tool
- **Enterprise On-Prem**: Paid centralized OTLP database deployment

## Prerequisites

1. **Rust toolchain** - Install from [rustup.rs](https://rustup.rs/)
2. **Trunk** - Build tool for Rust WASM apps
   ```bash
   cargo install trunk
   ```
3. **wasm32 target** - WebAssembly compilation target
   ```bash
   rustup target add wasm32-unknown-unknown
   ```

## Development

### Run Development Server

```bash
cd crates/sequins-web
trunk serve
```

The site will be available at `http://127.0.0.1:8080` with hot-reloading enabled.

### Build for Production

```bash
cd crates/sequins-web
trunk build --release
```

The optimized output will be in `dist/` directory.

## Project Structure

```
sequins-web/
├── src/
│   └── lib.rs          # Main Leptos app with all components
├── index.html          # HTML template
├── styles.css          # Styling (loaded by Trunk)
├── Trunk.toml          # Build configuration
└── Cargo.toml          # Dependencies
```

## Components

The site is built with these main sections:

- **Header**: Navigation bar with logo and links
- **Hero**: Main value proposition with download CTAs
- **Features**: Grid of 9 key product features
- **Pricing**: Free vs Enterprise comparison
- **Footer**: Links and copyright information

## Tech Stack

- **Leptos 0.7**: Reactive web framework for Rust
- **Trunk**: Build tool and dev server
- **WASM**: Compiles to WebAssembly for browser execution
- **CSS**: Custom styling with CSS variables

## Deployment

The site is configured for **Server-Side Rendering (SSR)** for better SEO and performance.

### Deploy to Fly.io

```bash
# From workspace root
cd /Users/stephenbelanger/Code/rust/sequins
fly launch  # fly.toml is in repo root
fly deploy
```

### Local Development with SSR

```bash
# Install cargo-leptos
cargo install cargo-leptos

# Run with hot reload
cd crates/sequins-web
cargo leptos watch
```

The Dockerfile follows the [official Leptos SSR deployment guide](https://book.leptos.dev/deployment/ssr.html).

## Notes

- Uses **Server-Side Rendering (SSR)** with Leptos for SEO and performance
- Client-side hydration makes the app interactive after initial render
- `cargo-leptos` handles both server and WASM builds automatically
- Production builds are optimized for minimal WASM bundle size

## Future Enhancements

- Add interactive demo/screenshots
- Blog/changelog section
- Documentation integration
- Community/testimonials section
- Download links for actual releases (Mac, Linux, Windows)
