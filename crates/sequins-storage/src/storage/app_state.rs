//! `DashboardApi` for [`Storage`] — delegates to the durable [`AppStateStore`].
//!
//! This is the **local** implementation of the dashboard interface; the remote
//! client implements the same trait over HTTP so the app has one interface.

use super::Storage;
use sequins_metadata::{Dashboard, DashboardApi, Result as MetaResult};

#[async_trait::async_trait]
impl DashboardApi for Storage {
    async fn list_dashboards(&self) -> MetaResult<Vec<Dashboard>> {
        Ok(self.app_state.dashboards_snapshot().await)
    }

    async fn get_dashboard(&self, id: &str) -> MetaResult<Option<Dashboard>> {
        Ok(self.app_state.get_dashboard(id).await)
    }

    async fn save_dashboard(&self, dashboard: Dashboard) -> MetaResult<Dashboard> {
        self.app_state.upsert_dashboard(dashboard).await
    }

    async fn delete_dashboard(&self, id: &str) -> MetaResult<()> {
        self.app_state.delete_dashboard(id).await
    }
}
