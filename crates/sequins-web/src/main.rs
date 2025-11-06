#[tokio::main]
async fn main() {
    use axum::Router;
    use leptos::config::get_configuration;
    use leptos_axum::{generate_route_list, LeptosRoutes};
    use sequins_web::Shell;

    // Get Leptos configuration from Cargo.toml
    let conf = get_configuration(None).unwrap();
    let leptos_options = conf.leptos_options;
    let addr = leptos_options.site_addr;
    let routes = generate_route_list(Shell);

    // Build Axum router with Leptos routes
    // .leptos_routes() handles serving the app and static files
    let app = Router::new()
        .leptos_routes(&leptos_options, routes, Shell)
        .with_state(leptos_options);

    // Start server
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("🚀 Listening on http://{}", &addr);
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}
