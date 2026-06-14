//! JSON schema conformance tests for canonical DFC types.

use dfc_core::{DfcEvent, SourceSystem};
use jsonschema::Validator;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

fn schema_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../schemas")
        .canonicalize()
        .expect("schemas directory")
}

#[test]
fn dfc_event_fixture_matches_schema() {
    let schema_path = schema_dir().join("dfc-event.schema.json");
    let fixture_path = schema_dir().join("fixtures/dfc-event.v1.json");

    let schema: Value =
        serde_json::from_str(&fs::read_to_string(schema_path).expect("read schema"))
            .expect("parse schema");
    let instance: Value =
        serde_json::from_str(&fs::read_to_string(fixture_path).expect("read fixture"))
            .expect("parse fixture");

    let validator = Validator::new(&schema).expect("compile schema");
    let result = validator.validate(&instance);
    if let Err(errors) = result {
        panic!("fixture failed validation: {errors:?}");
    }
}

#[test]
fn correlate_request_fixture_matches_schema() {
    let schema_path = schema_dir().join("correlate-request.schema.json");
    let fixture_path = schema_dir().join("fixtures/correlate-request.v1.json");

    let schema: Value =
        serde_json::from_str(&fs::read_to_string(schema_path).expect("read schema"))
            .expect("parse schema");
    let instance: Value =
        serde_json::from_str(&fs::read_to_string(fixture_path).expect("read fixture"))
            .expect("parse fixture");

    let validator = Validator::new(&schema).expect("compile schema");
    let result = validator.validate(&instance);
    if let Err(errors) = result {
        panic!("fixture failed validation: {errors:?}");
    }
}

#[test]
fn serialized_dfc_event_round_trips_schema_fields() {
    let event = DfcEvent::new(
        "aivcs.snapshot.created",
        "lornu-ai",
        "sha256:test",
        SourceSystem::AivcsApi,
    );
    event.validate().expect("valid event");
    assert_eq!(event.schema_version, "dfc.v1");
}
