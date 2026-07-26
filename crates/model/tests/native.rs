use cairn_model::{
    CatalogError, NativeError, OperationTerminal, OperationViewResult, SnapshotHandle, Store,
    MAX_RANGE_WRITE_BYTES,
};

fn store() -> (Store, cairn_model::CollectionId) {
    let mut store = Store::open_default().unwrap();
    let collection_op = store.allocate_operation_id().unwrap();
    let collection = store.create_collection("docs", collection_op).unwrap();
    (store, collection)
}

#[test]
fn native_write_read_and_zero_fill_are_bounded() {
    let (mut store, collection) = store();
    let unused_op = store.allocate_operation_id().unwrap();
    assert!(store.query_operation(unused_op).unwrap().is_none());

    let file_op = store.allocate_operation_id().unwrap();
    let file = store.create_file(collection, "hello", file_op).unwrap();
    let head = store.head(file).unwrap();
    let write_op = store.allocate_operation_id().unwrap();
    let mut txn = store.begin_write(file, head, write_op).unwrap();
    txn.write_range(3..6, b"abc").unwrap();
    let version = txn.commit().unwrap();
    assert_eq!(version.size, 6);

    let snapshot = store.open_snapshot(file, None).unwrap();
    let mut output = [0; 6];
    assert_eq!(snapshot.read_range(0..6, &mut output).unwrap(), 6);
    assert_eq!(&output, b"\0\0\0abc");
    assert_eq!(snapshot.len(), 6);
    assert!(matches!(
        snapshot.read_range(5..7, &mut output),
        Err(NativeError::InvalidRange)
    ));

    let operation = store.query_operation(write_op).unwrap().unwrap();
    assert_eq!(
        operation.result,
        Some(OperationViewResult::Version {
            id: version.id,
            generation: 1,
            size: 6,
            parent_version_id: None,
        })
    );
}

#[test]
fn native_cas_shrink_and_abort() {
    let (mut store, collection) = store();
    let file_op = store.allocate_operation_id().unwrap();
    let file = store.create_file(collection, "cas", file_op).unwrap();
    let initial = store.head(file).unwrap();
    let first_op = store.allocate_operation_id().unwrap();
    let mut first = store.begin_write(file, initial, first_op).unwrap();
    first.write_range(0..5, b"hello").unwrap();
    first.commit().unwrap();

    let stale_op = store.allocate_operation_id().unwrap();
    assert!(matches!(
        store.begin_write(file, initial, stale_op),
        Err(NativeError::Catalog(
            cairn_model::CatalogError::HeadConflict
        ))
    ));

    let current = store.head(file).unwrap();
    let second_op = store.allocate_operation_id().unwrap();
    let mut second = store.begin_write(file, current, second_op).unwrap();
    second.truncate(2).unwrap();
    assert!(matches!(second.truncate(3), Err(NativeError::CannotExtend)));
    second.abort().unwrap();
    assert_eq!(store.head(file).unwrap(), current);
    assert!(matches!(
        store.operation_terminal(second_op),
        Ok(Some(OperationTerminal::Aborted { .. }))
    ));
    assert_eq!(
        store.operation_terminal(second_op),
        Ok(Some(OperationTerminal::Aborted {
            error: CatalogError::Aborted,
        }))
    );
}

#[test]
fn file_view_is_immutable_and_overlapping_bounded_writes_match_an_oracle() {
    let (mut store, collection) = store();
    let file_op = store.allocate_operation_id().unwrap();
    let file = store.create_file(collection, "view", file_op).unwrap();

    let first_op = store.allocate_operation_id().unwrap();
    let mut first = store
        .begin_write(file, store.head(file).unwrap(), first_op)
        .unwrap();
    first.write_range(2..5, b"abc").unwrap();
    let first_version = first.commit().unwrap();
    assert!(matches!(
        store.operation_terminal(first_op),
        Ok(Some(OperationTerminal::Committed(version))) if version == first_version
    ));

    let view: SnapshotHandle = store.open_file_view(file, None).unwrap();
    let mut before = [0; 5];
    view.read_range(0..5, &mut before).unwrap();
    assert_eq!(&before, b"\0\0abc");

    let second_op = store.allocate_operation_id().unwrap();
    let mut second = store
        .begin_write(file, store.head(file).unwrap(), second_op)
        .unwrap();
    second.write_range(1..4, b"XYZ").unwrap();
    assert!(matches!(
        second.write_range(3..5, b"!?"),
        Err(NativeError::OverlappingWrite)
    ));
    second.write_range(4..6, b"!?").unwrap();
    assert!(matches!(
        second.write_range(
            0..(MAX_RANGE_WRITE_BYTES as u64 + 1),
            &vec![0; MAX_RANGE_WRITE_BYTES + 1]
        ),
        Err(NativeError::WriteTooLarge)
    ));
    second.commit().unwrap();

    let mut preserved = [0; 5];
    view.read_range(0..5, &mut preserved).unwrap();
    assert_eq!(&preserved, b"\0\0abc");
    let current = store.open_file_view(file, None).unwrap();
    let mut after = [0; 6];
    current.read_range(0..6, &mut after).unwrap();
    assert_eq!(&after, b"\0XYZ!?");
}

#[test]
fn stale_head_failure_is_terminal_by_operation_id() {
    let (mut store, collection) = store();
    let file_op = store.allocate_operation_id().unwrap();
    let file = store.create_file(collection, "stale", file_op).unwrap();
    let stale = store.head(file).unwrap();
    let winner_op = store.allocate_operation_id().unwrap();
    let mut winner = store.begin_write(file, stale, winner_op).unwrap();
    winner.write_range(0..1, b"x").unwrap();
    winner.commit().unwrap();

    let stale_op = store.allocate_operation_id().unwrap();
    assert!(matches!(
        store.begin_write(file, stale, stale_op),
        Err(NativeError::Catalog(CatalogError::HeadConflict))
    ));
    assert!(matches!(
        store.operation_terminal(stale_op),
        Ok(Some(OperationTerminal::Aborted {
            error: CatalogError::HeadConflict
        }))
    ));
}

#[test]
fn bounded_write_matrix_matches_a_plain_byte_oracle() {
    let (mut store, collection) = store();
    let file_op = store.allocate_operation_id().unwrap();
    let file = store.create_file(collection, "matrix", file_op).unwrap();
    let cases = [
        (0..1, b"a".as_slice()),
        (3..5, b"bc".as_slice()),
        (1..4, b"XYZ".as_slice()),
        (8..9, b"!".as_slice()),
    ];
    let mut oracle = Vec::new();

    for (range, bytes) in cases {
        let op = store.allocate_operation_id().unwrap();
        let mut txn = store
            .begin_write(file, store.head(file).unwrap(), op)
            .unwrap();
        txn.write_range(range.clone(), bytes).unwrap();
        let end = range.end as usize;
        if end > oracle.len() {
            oracle.resize(end, 0);
        }
        oracle[range.start as usize..end].copy_from_slice(bytes);
        txn.commit().unwrap();

        let view = store.open_file_view(file, None).unwrap();
        let mut actual = vec![0; view.len() as usize];
        view.read_range(0..view.len(), &mut actual).unwrap();
        assert_eq!(actual, oracle);
    }
}

#[test]
fn native_reads_across_content_nodes_without_materializing_the_snapshot() {
    let (mut store, collection) = store();
    let file_op = store.allocate_operation_id().unwrap();
    let file = store.create_file(collection, "chunks", file_op).unwrap();
    let write_op = store.allocate_operation_id().unwrap();
    let mut txn = store
        .begin_write(file, store.head(file).unwrap(), write_op)
        .unwrap();
    let chunk = vec![b'a'; MAX_RANGE_WRITE_BYTES];
    txn.write_range(0..chunk.len() as u64, &chunk).unwrap();
    let tail = b"tail";
    let tail_start = chunk.len() as u64;
    txn.write_range(tail_start..tail_start + tail.len() as u64, tail)
        .unwrap();
    txn.commit().unwrap();

    let snapshot = store.open_snapshot(file, None).unwrap();
    let range = tail_start - 2..tail_start + tail.len() as u64;
    let mut output = [0; 6];
    assert_eq!(
        snapshot.read_range(range, &mut output).unwrap(),
        output.len()
    );
    assert_eq!(&output, b"aatail");
    assert_eq!(
        snapshot
            .read_range(snapshot.len()..snapshot.len(), &mut output)
            .unwrap(),
        0
    );
    assert!(matches!(
        snapshot.read_range(tail_start..snapshot.len(), &mut output[..3]),
        Err(NativeError::OutputTooSmall)
    ));
}

#[test]
fn two_snapshot_handles_keep_a_shared_commit_pinned_until_both_drop() {
    let (mut store, collection) = store();
    let file_op = store.allocate_operation_id().unwrap();
    let file = store.create_file(collection, "shared", file_op).unwrap();
    let op = store.allocate_operation_id().unwrap();
    let mut txn = store
        .begin_write(file, store.head(file).unwrap(), op)
        .unwrap();
    txn.write_range(0..5, b"hello").unwrap();
    txn.commit().unwrap();

    let first = store.open_snapshot(file, None).unwrap();
    let second = store.open_snapshot(file, None).unwrap();
    drop(first);
    let mut output = [0; 5];
    assert_eq!(second.read_range(0..5, &mut output).unwrap(), 5);
    assert_eq!(&output, b"hello");
}
