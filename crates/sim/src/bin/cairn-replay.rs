use std::io::Read;

use cairn_sim::replay::{decode_json, encode_report, replay, ReplayError, MAX_REPLAY_INPUT_BYTES};

fn main() {
    let input = match std::env::args().nth(1) {
        Some(path) if path == "-" => read_stdin(),
        Some(path) => read_file(&path),
        None => Err("usage: cairn-replay <case.json|->".to_string()),
    };
    let input = match input {
        Ok(input) => input,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let case = match decode_json(&input) {
        Ok(case) => case,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let report = match replay(&case) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(if matches!(error, ReplayError::Divergence { .. }) {
                1
            } else {
                2
            });
        }
    };
    match encode_report(&report) {
        Ok(output) => println!("{}", String::from_utf8(output).expect("JSON is UTF-8")),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}

fn read_stdin() -> Result<Vec<u8>, String> {
    read_limited(std::io::stdin())
}

fn read_file(path: &str) -> Result<Vec<u8>, String> {
    let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    read_limited(file)
}

fn read_limited(mut reader: impl Read) -> Result<Vec<u8>, String> {
    let limit = u64::try_from(MAX_REPLAY_INPUT_BYTES)
        .expect("replay input limit fits in u64")
        .saturating_add(1);
    let mut input = Vec::new();
    reader
        .by_ref()
        .take(limit)
        .read_to_end(&mut input)
        .map_err(|error| error.to_string())?;
    if input.len() > MAX_REPLAY_INPUT_BYTES {
        return Err(format!("input exceeds {MAX_REPLAY_INPUT_BYTES} bytes"));
    }
    Ok(input)
}
