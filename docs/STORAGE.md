# Storage format v1

Paths are `data/topics/<workspace>/<topic>/topic.json` and `<partition>/<base-offset-as-20-digits>.log`. Workspace/topic names are ASCII `[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}`; partition IDs are server-generated integers. Names cannot contain slashes or traverse directories. Topic partition counts are immutable.

Each record has:

| Bytes | Encoding | Meaning |
|---|---|---|
| 0–3 | unsigned big-endian uint32 | JSON payload length |
| 4–7 | unsigned big-endian uint32 | IEEE CRC32 of JSON payload |
| 8 onward | UTF-8 JSON | version-1 event envelope |

The envelope includes ID, topic, partition, offset, server UTC timestamp, type, key, headers and JSON data. CRC32 detects accidental damage; it is not tamper-proof encryption or authentication. Files are mode 0600; directories 0700. Records are capped at 2 MiB, ordinary producer events at 1 MiB. DLQ records can contain the original envelope and use a larger internal limit.

A partition mutex serializes append, fsync, offset assignment, reads and retention. A published event is acknowledged only after full record write and fsync. On write/fsync failure the partition is fenced until restart. Segment files rotate at 4 MiB by default; file creation and directory entries are synchronized. Metadata uses temporary-file write + fsync + atomic rename + parent directory fsync.

Recovery scans records with bounded allocations, retaining only segment metadata. Segment filenames must match consecutive record offsets. An incomplete length header or incomplete payload in the final active file is truncated to the previous valid record and synchronized. Incomplete older files, invalid lengths, checksum failures and offset gaps fail startup. No complete record with a bad checksum is discarded silently. An exclusive OS flock prevents two processes opening a single data directory.

Reads scan selected segments, returning up to 1,000 events and approximately 4 MiB (one final record can exceed this response threshold). They do not load the whole topic. There is no sparse offset index yet: repeated reads near the end of a segment have linear scan cost within that bounded segment. Topic size scales with retained disk, not process memory, but topic/partition counts still allocate metadata.

Retention removes a prefix of sealed segments, synchronizing unlink operations. It never deletes the active segment or reuses offsets, including after restart. Size budgets are divided among partitions, and active files can exceed the budget. Consumer commits do not prevent retention. Out-of-range reads return an explicit error and HTTP 416. An operator must make the data-loss/replay boundary decision through an inactive-group reset.

Metadata/log backups must be coordinated while publication and consumption are stopped. Restoring only PostgreSQL can leave offsets beyond log contents; restoring only disk can duplicate committed work. Crash recovery is not replication, automatic repair of media damage, or a substitute for backups.
