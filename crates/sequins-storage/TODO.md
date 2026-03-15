# TODO

- The entire management API and maintenance system (background.rs, maintenance.rs, management.rs) needs to be rewritten from scratch. Most of it is not used anymore.
- It seems like CorrelationHelper is entirely dead code? Should we be removing that file? It doesn't appear to even be implemented.
- Isolate retention policy manager (retention.rs) to its own separate crate.
- Isolate health check system (health.rs) to its own separate crate.
