use crate::domain::ApplicationState;

pub(super) const VERSION: &str = "v2026_08_12_initial";

pub(super) fn snapshot(payload: &[u8]) -> Result<ApplicationState, postcard::Error> {
    postcard::from_bytes(payload)
}

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_matches_file_name() {
        assert_eq!(VERSION, module_path!().rsplit("::").nth(1).unwrap());
    }
}
