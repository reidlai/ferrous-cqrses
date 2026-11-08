//! Integration tests for `--healthcheck` static probe flag and execution.
//!
//! Validates Story 7.5: hardened minimal runtime probe contract for container orchestration.

use clap::Parser;
use ferrous_cqrses::consumer::Config;
use std::process::Command;
use std::time::Duration;

#[test]
fn test_healthcheck_config_flag_parsing() {
    // Default configuration must have healthcheck disabled
    let default_config = Config::default();
    assert!(
        !default_config.healthcheck,
        "healthcheck flag must default to false"
    );

    // Parsing without --healthcheck flag retains false
    let without_flag =
        Config::try_parse_from(["ferrous-cqrses-daemon"]).expect("CLI parsing must succeed");
    assert!(
        !without_flag.healthcheck,
        "healthcheck flag must be false when omitted"
    );

    // Parsing with --healthcheck flag enables healthcheck mode
    let with_flag = Config::try_parse_from(["ferrous-cqrses-daemon", "--healthcheck"])
        .expect("CLI parsing must succeed with --healthcheck");
    assert!(
        with_flag.healthcheck,
        "healthcheck flag must be true when --healthcheck is supplied"
    );
}

#[test]
fn test_healthcheck_binary_probe_execution() {
    let bin_path = env!("CARGO_BIN_EXE_command_handler");

    // Execute command_handler binary with --healthcheck probe flag
    let start = std::time::Instant::now();
    let output = Command::new(bin_path)
        .arg("--healthcheck")
        .output()
        .expect("Failed to execute command_handler with --healthcheck");
    let elapsed = start.elapsed();

    // Probe must exit cleanly with code 0 in sub-second time without blocking
    assert!(
        output.status.success(),
        "command_handler --healthcheck must exit successfully"
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "command_handler --healthcheck must exit with exit code 0"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "healthcheck probe must complete quickly (elapsed: {elapsed:?})"
    );
}

#[test]
fn test_command_handler_help_displays_healthcheck_option() {
    let bin_path = env!("CARGO_BIN_EXE_command_handler");

    let output = Command::new(bin_path)
        .arg("--help")
        .output()
        .expect("Failed to execute command_handler --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--healthcheck"),
        "CLI help text must document --healthcheck option"
    );
}

#[test]
fn test_healthcheck_with_dummy_broker_address_does_not_block() {
    let bin_path = env!("CARGO_BIN_EXE_command_handler");

    // Execute with an unroutable RFC 5737 test address
    let start = std::time::Instant::now();
    let output = Command::new(bin_path)
        .arg("--healthcheck")
        .arg("--iggy-server-address")
        .arg("192.0.2.1:9999")
        .output()
        .expect("Failed to execute command_handler");
    let elapsed = start.elapsed();

    assert!(output.status.success());
    assert_eq!(output.status.code(), Some(0));
    assert!(
        elapsed < Duration::from_secs(2),
        "healthcheck must return immediately without attempting TCP connections (elapsed: {elapsed:?})"
    );
}

#[test]
fn test_command_handler_invalid_argument_fails() {
    let bin_path = env!("CARGO_BIN_EXE_command_handler");

    let output = Command::new(bin_path)
        .arg("--unrecognized-bogus-flag")
        .output()
        .expect("Failed to execute command_handler");

    assert!(
        !output.status.success(),
        "command_handler must fail when invalid arguments are provided"
    );
    assert_ne!(output.status.code(), Some(0));
}
