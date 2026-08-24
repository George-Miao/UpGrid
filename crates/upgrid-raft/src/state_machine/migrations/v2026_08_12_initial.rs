use crate::domain::{ApplicationState, decode_v2026_08_12_application_state};

pub(super) const VERSION: &str = "v2026_08_12_initial";

pub(super) fn snapshot(payload: &[u8]) -> Result<ApplicationState, postcard::Error> {
    decode_v2026_08_12_application_state(payload)
}

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_matches_file_name() {
        assert_eq!(VERSION, module_path!().rsplit("::").nth(1).unwrap());
    }
}
