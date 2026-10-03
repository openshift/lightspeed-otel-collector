# Trace Data Collection

This file defines the Collector contract for writing selected OTLP traces to
native JSONL files. It covers service-based resource selection, FileExporter
behavior, source-file ownership, and the interface for downstream consumers.
The contract ends at the trace files; downstream conversion and upload belong
to a separate component.

## Resource Selection and Pipeline

1. The file branch MUST consume OTLP traces only. OTLP logs and metrics MUST
   NOT enter the branch or its files; existing PostgreSQL log handling, admin
   APIs, internal metrics, and configured trace forwarding or `nop` behavior
   remain separate.
2. The `routing/data_collection` connector MUST evaluate the resource attribute
   `service.name` and route only exact matches for
   `lightspeed-agentic-operator` or `lightspeed-agentic-sandbox` to
   `traces/data_collection`. It MUST have no default file pipeline.
3. Resources with missing or non-matching `service.name` values are excluded
   from file output.
4. For a matched resource, the branch MUST retain every span and attached
   event in its original OTLP context, including resource and scope metadata,
   attributes, IDs, timestamps, and links.
5. The trace input MUST fan out to the file-selection connector and the
   configured all-trace backend route or direct-config `nop` sink. Each
   destination receives its trace data intact.
   In routing mode, batching MUST occur only on the backend trace pipeline.
   The input fan-out and `traces/data_collection` pipeline MUST remain unbatched.
   Count-based batching can combine valid requests beyond FileExporter's
   1 MiB write limit and acknowledge them before a later export rejection.
6. Internal telemetry uses the standard Collector and component
   instrumentation configured for the distribution.

## File Format and Reference Configuration

7. The distribution MUST use contrib FileExporter from
   `github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter`
   v0.159.0. Its trace support is alpha; changes to this component version
   require a compatibility review.
8. The active file and reference settings are:

   ```yaml
   file/data_collection:
     path: /var/lib/lightspeed-data/otel/traces.jsonl
     format: json
     create_directory: true
     rotation:
       max_megabytes: 1
       max_backups: 100
       max_days: 1
   ```

   The reference configurations MUST use these settings.
9. `rotation.max_megabytes` is an integer count of MiB. Thus `1` is a 1 MiB
   rotation threshold; a 500 KB threshold is not representable by this field.
   `max_days` is an integer number of days.
10. Each JSONL object MUST represent one complete OTLP trace batch passed to
    the FileExporter after routing. It MUST retain native OTLP nesting:
    `resourceSpans[] -> scopeSpans[] -> spans[] -> events[]`. An object MAY
    contain multiple resources, scopes, and spans; span events stay embedded
    under their spans. FileExporter writes each JSON object and its LF
    separately. At the exact size boundary, a complete object can end a closed
    backup without a trailing LF, with the LF written to the new active file.
    A reader MUST accept a complete final JSON object without LF and ignore
    empty lines at rotation boundaries.
11. Selected files contain raw OTLP trace data and MUST be protected as such.
    The native OTLP structure is the file contract; downstream consumers own
    any conversion to their ingestion schemas.

## Rotation, Retention, and Failure Behavior

12. Rotation MUST be size-triggered only. A below-threshold active file
    remains active across quiet periods and shutdown until later writes
    trigger size rotation. Closing it at shutdown leaves it at the active
    path.
13. With rotation enabled, a process restart MUST reopen and append to the
    configured active file. The `append: true` option MUST NOT be used with
    rotation.
14. A serialized trace batch larger than the configured 1 MiB limit MUST be
    rejected as an export error. The limit applies to the whole serialized
    batch. The OTLP receiver's request-size limit is a separate constraint.
15. `max_backups: 100` and `max_days: 1` are independent stock retention
    criteria, not upload acknowledgements. Count-based cleanup can remove a
    backup before its age limit. Age cleanup is asynchronous housekeeping, not
    an exact wall-clock expiry service, and it does not rotate an idle active
    file.
16. The configured thresholds imply roughly one active MiB plus up to 100
    backup MiB, not a hard filesystem quota. Asynchronous cleanup, files held
    open after unlink, and other volume contents can increase actual disk
    usage. Retention is independent of downstream processing; pod or
    volume removal can lose remaining source files.
17. Invalid filesystem setup can prevent Collector startup. Runtime
    FileExporter failures can propagate through the OTLP trace request even
    if a sibling destination already accepted the same batch. Delivery is
    subject to these partial-success and failure conditions.

## Source Ownership and Consumer Interface

18. The Collector owns the active path, rotation, and source retention.
    Downstream consumers MUST read closed rotated files through a read-only
    source mount, leaving the active path and source-file lifecycle to the
    Collector.
19. A consumer owns its conversion, upload, and ledger state. Ledger entries
    for absent source files MAY be pruned only after a successful, complete
    directory scan. Failed, incomplete, or unreadable scans MUST leave the
    ledger unchanged.

For runnable standalone build, send, and backup-inspection steps, see
[`README.md`](../../../README.md#standalone-local-rotation-smoke).
