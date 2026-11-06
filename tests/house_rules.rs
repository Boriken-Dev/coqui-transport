//! What the library source never holds: process-wide state, `unsafe` code, or output of its own.

use std::fs;
use std::path::{Path, PathBuf};

fn files(directory: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("src/ is readable") {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            files(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

/// Every line of code in `src/` holding one of `markers`, comments aside, with where it is.
fn lines_holding(markers: &[&str], except: impl Fn(&str) -> bool) -> Vec<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut paths = Vec::new();
    files(&root, &mut paths);
    let mut found = Vec::new();
    for path in paths {
        let text = fs::read_to_string(&path).expect("a source file is UTF-8");
        for (index, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or_default();
            if markers.iter().any(|marker| code.contains(marker)) && !except(code) {
                found.push(format!("{}:{}: {}", path.display(), index + 1, line.trim()));
            }
        }
    }
    found
}

#[test]
fn the_source_holds_no_process_wide_state() {
    let found = lines_holding(
        &["static ", "OnceLock", "LazyLock", "thread_local!"],
        |code| code.contains("'static"),
    );
    assert!(found.is_empty(), "process-wide state: {found:?}");
}

#[test]
fn the_source_holds_no_unsafe_code() {
    let found = lines_holding(&["unsafe"], |code| code.contains("forbid(unsafe_code)"));
    assert!(found.is_empty(), "unsafe code: {found:?}");
}

#[test]
fn the_library_neither_prints_nor_exits() {
    let found = lines_holding(
        &[
            "println!",
            "print!(",
            "eprintln!",
            "eprint!(",
            "dbg!",
            "process::exit",
            "abort(",
        ],
        |_| false,
    );
    assert!(found.is_empty(), "output or exit: {found:?}");
}
