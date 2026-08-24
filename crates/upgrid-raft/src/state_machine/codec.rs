use std::{io, str};

use serde::Serialize;
use snafu::{OptionExt, ResultExt, Snafu};

use super::migrations::{self, CURRENT_VERSION};
use crate::domain::ApplicationState;

const VERSION_TERMINATOR: u8 = b'\n';

pub(super) struct Decoded<T> {
    pub(super) value: T,
    pub(super) migrated: bool,
}

pub(crate) fn encode_snapshot(value: &ApplicationState) -> io::Result<Vec<u8>> {
    encode(value)
}

pub(super) fn decode_snapshot(bytes: &[u8]) -> io::Result<Decoded<ApplicationState>> {
    decode(bytes).map_err(invalid_data)
}

fn decode(bytes: &[u8]) -> Result<Decoded<ApplicationState>, FormatError> {
    let (version, payload) = split(bytes)?;
    let value = migrations::snapshot(version, payload).context(DecodeSnafu)?;
    Ok(Decoded {
        value,
        migrated: version != CURRENT_VERSION,
    })
}

fn encode<T>(value: &T) -> io::Result<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let mut bytes = Vec::with_capacity(CURRENT_VERSION.len() + 1);
    bytes.extend_from_slice(CURRENT_VERSION.as_bytes());
    bytes.push(VERSION_TERMINATOR);
    postcard::to_extend(value, bytes)
        .context(EncodeSnafu)
        .map_err(io::Error::other)
}

fn split(bytes: &[u8]) -> Result<(&str, &[u8]), FormatError> {
    let terminator = bytes
        .iter()
        .position(|byte| *byte == VERSION_TERMINATOR)
        .context(MissingVersionSnafu)?;
    let (version, payload) = bytes.split_at(terminator);
    let version = str::from_utf8(version).context(InvalidVersionSnafu)?;
    Ok((version, &payload[1..]))
}

fn invalid_data(error: FormatError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[derive(Debug, Snafu)]
enum FormatError {
    #[snafu(display("state-machine data has no version string"))]
    MissingVersion,

    #[snafu(display("state-machine version is not UTF-8: {source}"))]
    InvalidVersion { source: str::Utf8Error },

    #[snafu(display("failed to decode state-machine data: {source}"))]
    Decode { source: migrations::Error },

    #[snafu(display("failed to encode state-machine data: {source}"))]
    Encode { source: postcard::Error },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use uuid::Uuid;

    use super::{decode_snapshot, encode_snapshot, split};
    use crate::domain::{
        ApplicationState, Command, JoinTokenHash, encode_v2026_08_12_application_state,
        encode_v2026_08_19_application_state,
        encode_v2026_08_19_connectivity_alerts_application_state,
    };
    use crate::state_machine::migrations::{
        CONNECTIVITY_ALERTS_VERSION, CURRENT_VERSION, INITIAL_VERSION, REACHABILITY_VERSION,
    };
    use crate::{DirectedRoute, NodeReachability, ReachableAddress};

    #[test]
    fn initial_snapshot_decodes_with_reachability_state_empty() {
        let node_id = Uuid::from_u128(1);
        let token = JoinTokenHash([7; 32]);
        let mut expected = ApplicationState::default();
        expected.history_retention_ms = 123_456;
        expected.join_tokens.insert(token, 987_654);
        expected
            .node_names
            .insert(node_id, "legacy-node".to_owned());
        expected.public_status_enabled = true;
        let payload = encode_v2026_08_12_application_state(expected.clone()).unwrap();
        let mut bytes = format!("{INITIAL_VERSION}\n").into_bytes();
        bytes.extend_from_slice(&payload);

        let decoded = decode_snapshot(&bytes).unwrap();

        assert!(decoded.migrated);
        assert_eq!(expected, decoded.value);
        assert!(decoded.value.node_reachability.is_empty());
        assert!(decoded.value.connectivity_failures.is_empty());
    }

    #[test]
    fn reachability_snapshot_preserves_confirmed_connectivity_failures() {
        let node_id = Uuid::from_u128(1);
        let address = ReachableAddress::parse("up://node.example:4242").unwrap();
        let route = DirectedRoute {
            source: node_id,
            destination: Uuid::from_u128(2),
        };
        let reservation_id = Uuid::from_u128(3);
        let token = JoinTokenHash([8; 32]);
        let mut expected = ApplicationState::default();
        expected.node_reachability.insert(
            node_id,
            NodeReachability::configured(BTreeSet::from([address])),
        );
        expected.connectivity_failures.insert(route);
        expected
            .apply(Command::PutLimitedJoinToken {
                hash: token,
                expires_at_ms: 60_000,
                uses: 1,
            })
            .unwrap();
        expected
            .apply(Command::ReserveJoinToken {
                hash: token,
                reservation_id,
                reservation_operation_id: uuid::Uuid::nil(),
                reserved_at_ms: 1_000,
                readmission: false,
            })
            .unwrap();
        let payload = encode_v2026_08_19_application_state(expected.clone()).unwrap();
        let mut bytes = format!("{REACHABILITY_VERSION}\n").into_bytes();
        bytes.extend_from_slice(&payload);

        let decoded = decode_snapshot(&bytes).unwrap();

        assert!(decoded.migrated);
        assert_eq!(
            expected.connectivity_failures,
            decoded.value.connectivity_failures
        );
        assert!(
            !decoded
                .value
                .connectivity_scan_requires_record(&BTreeSet::from([route]), 0)
        );
        let mut migrated = decoded.value;
        assert_eq!(
            migrated.active_join_reservation(reservation_id, uuid::Uuid::nil(), 1_000),
            Some(false)
        );
        migrated
            .apply(Command::AbortPendingJoin {
                reservation_id,
                reservation_operation_id: Uuid::nil(),
                completed_at_ms: 1_100,
            })
            .unwrap();
        assert_eq!(migrated.join_tokens.get(&token), Some(&60_000));
        assert_eq!(migrated.join_token_uses.get(&token), Some(&1));
    }

    #[test]
    fn connectivity_alerts_snapshot_infers_explicit_degradation() {
        let source = Uuid::from_u128(1);
        let route = DirectedRoute {
            source,
            destination: Uuid::from_u128(2),
        };
        let mut expected = ApplicationState::default();
        for checked_at_ms in 1..=3 {
            expected
                .apply(Command::RecordConnectivity {
                    leases: Vec::new(),
                    verified: Some(Default::default()),
                    checked_at_ms,
                    failures: BTreeSet::from([route]),
                })
                .unwrap();
        }
        expected.retain_member_reachability(&BTreeSet::from([source]));
        assert!(expected.connectivity_failures.is_empty());
        let payload = encode_v2026_08_19_connectivity_alerts_application_state(expected).unwrap();
        let mut bytes = format!("{CONNECTIVITY_ALERTS_VERSION}\n").into_bytes();
        bytes.extend_from_slice(&payload);

        let decoded = decode_snapshot(&bytes).unwrap();

        assert!(decoded.migrated);
        let mut migrated = decoded.value;
        assert!(migrated.connectivity_degraded());
        migrated.availability_transitions.clear();
        assert!(migrated.connectivity_failures.is_empty());
        assert!(migrated.connectivity_degraded());
    }

    #[test]
    fn current_snapshot_round_trips_new_state() {
        let node_id = Uuid::from_u128(1);
        let destination = Uuid::from_u128(2);
        let reservation_id = Uuid::from_u128(3);
        let address = ReachableAddress::parse("up://node.example:4242").unwrap();
        let token = JoinTokenHash([9; 32]);
        let route = DirectedRoute {
            source: node_id,
            destination,
        };
        let mut expected = ApplicationState::default();
        expected.node_reachability.insert(
            node_id,
            NodeReachability::configured(BTreeSet::from([address])),
        );
        expected
            .apply(Command::RecordConnectivity {
                leases: Vec::new(),
                verified: Some(Default::default()),
                checked_at_ms: 1_000,
                failures: BTreeSet::from([route]),
            })
            .unwrap();
        expected
            .apply(Command::PutLimitedJoinToken {
                hash: token,
                expires_at_ms: 60_000,
                uses: 1,
            })
            .unwrap();
        expected
            .apply(Command::ReserveJoinToken {
                hash: token,
                reservation_id,
                reservation_operation_id: uuid::Uuid::nil(),
                reserved_at_ms: 1_000,
                readmission: true,
            })
            .unwrap();

        let bytes = encode_snapshot(&expected).unwrap();
        assert_eq!(CURRENT_VERSION, split(&bytes).unwrap().0);
        let decoded = decode_snapshot(&bytes).unwrap();

        assert!(!decoded.migrated);
        assert_eq!(expected, decoded.value);
    }

    #[test]
    fn current_snapshot_preserves_degradation_after_member_removal() {
        let source = Uuid::from_u128(1);
        let route = DirectedRoute {
            source,
            destination: Uuid::from_u128(2),
        };
        let mut expected = ApplicationState::default();
        for checked_at_ms in 1..=3 {
            expected
                .apply(Command::RecordConnectivity {
                    leases: Vec::new(),
                    verified: Some(Default::default()),
                    checked_at_ms,
                    failures: BTreeSet::from([route]),
                })
                .unwrap();
        }
        expected.retain_member_reachability(&BTreeSet::from([source]));
        assert!(expected.connectivity_failures.is_empty());
        assert!(expected.connectivity_degraded());
        expected.availability_transitions.clear();
        expected.alerts.clear();

        let bytes = encode_snapshot(&expected).unwrap();
        let decoded = decode_snapshot(&bytes).unwrap();

        assert!(!decoded.migrated);
        assert!(decoded.value.connectivity_failures.is_empty());
        assert!(decoded.value.connectivity_degraded());
        assert!(
            decoded
                .value
                .connectivity_scan_requires_record(&BTreeSet::new(), 4)
        );
    }
}
