//! `Duplex`, `Halves` and `Split`: joining a reader and a writer, and splitting them again.

use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use coqui_transport::{Duplex, Halves, Interrupt, Split};

#[test]
fn a_duplex_reads_from_its_reader_and_writes_to_its_writer() {
    let mut duplex = Duplex::new(Cursor::new(b"in".to_vec()), Vec::new());
    let mut read = String::new();
    duplex.read_to_string(&mut read).unwrap();
    duplex.write_all(b"out").unwrap();
    duplex.flush().unwrap();
    assert_eq!(read, "in");
    assert_eq!(duplex.writer(), b"out");
    duplex.writer_mut().push(b'!');
    assert_eq!(duplex.reader().position(), 2);
    duplex.reader_mut().set_position(0);
    let (reader, writer) = duplex.into_parts();
    assert_eq!(reader.position(), 0);
    assert_eq!(writer, b"out!");
}

#[test]
fn a_duplex_splits_into_its_halves_with_no_interrupt() {
    let halves = Duplex::new(Cursor::new(Vec::new()), Vec::<u8>::new())
        .split()
        .unwrap();
    assert!(halves.interrupt.is_none());
    let halves: Halves<_, _> = Duplex::new(Cursor::new(Vec::new()), Vec::<u8>::new()).into();
    assert!(halves.interrupt.is_none());
}

#[test]
fn halves_split_as_they_are_and_keep_their_interrupt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&calls);
    let halves = Halves::new(Cursor::new(Vec::new()), Vec::<u8>::new())
        .with_interrupt(Interrupt::new(move || {
            counting.fetch_add(1, Ordering::SeqCst);
        }))
        .split()
        .unwrap();
    let interrupt = halves.interrupt.unwrap();
    interrupt.interrupt();
    interrupt.clone().interrupt();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(format!("{interrupt:?}"), "Interrupt");
}
