# Cloudflare D1 port

Import `src/d1/index.ts` in Workers with `nodejs_compat`. This entry point does
not import `node:sqlite` or the optional file/API readers. Products that need the
readers can explicitly import `src/readers/index.ts`. The synchronous Node API and its guarantees remain
unchanged. D1 uses the same reviewed plans, source identities, retained originals,
provenance and inert-effect policy through an asynchronous native SQL port.

Copy the shared [D1 transport](../../../d1-transport/index.mjs) and its
[types](../../../d1-transport/index.d.mts), preserving their sibling location
beside the import-kit directory.
The D1 entry uses this shared transport automatically. It accounts for actual
UTF-8 bytes and workerd's whole-call two-byte strings, escapes sparse wide text
for SQLite to decode, and keeps each mutation in its original transaction.
Retained plans are read in bounded pages, including their reserved return-query
budget. Calls that still exceed the provider transport ceiling fail explicitly.

Apply `d1Schema`, `d1ErasureSchema`, `d1StreamOriginSchema`,
`d1LegacyStreamReceiptUpgrade`, `d1StagingSchema`,
`d1NativeStagingSchema`, `d1CustodyStateSchema`, `d1PreparationSchema`, `d1ReversalFenceSchema` and `d1RetirementSchema` in product-owned numbered migrations, or call
`initializeD1ImportStorage(database)` during setup. The schema includes the common
kit tables and D1 ownership/assertion tables. Do not mix the synchronous engine
and D1 engine for the same jobs: each engine owns its checkpoint protocol.

Apply stream-origin backfill and legacy receipt conversion in one transaction.
Origins record actual page, parent and job bindings and remain immutable until
the job is deleted. Missing or substituted parents cannot turn a known stream
child into an ordinary job. Ordinary version-three custody remains usable;
neither a nullable reversal-policy hash nor an operation-key shape identifies a
stream. Legacy custody whose entire structural origin was already lost cannot
be distinguished from ordinary custody by this upgrade.

A `D1ImportContext` contains the destination, namespace, mappingVersion,
`storage: { database, targets }`, and
`authorization: { principalId, predicate(action, destination) }`.
`predicate` must return a scalar SQL boolean expression and bound `params` that
checks the current session, actor, destination grant and expiry in D1. Returning
a cached boolean or constant SQL permission defeats this contract. The engine
evaluates the expression at both ends of every atomic mutation batch. A false
predicate raises `IMPORT_ATOMIC_GUARD` and rolls back every native and kit write.
Read operations also require current authority. Jobs remain bound to their actor.
The plan and its fingerprint carry that `principalId`, so a job can never be rebound
to another one, and an archive carries the jobs but not the operators who created
them: after an export, reset, import and setup the owner is a new operator row. A
product that wants restored jobs to stay usable passes a principal that survives a
restore, such as the operator's role (`'role:' + role`), and lets `predicate` check
that the acting operator is still active in that role. A product that passes the
operator's row id keeps its jobs bound to that operator for good.

`readD1ImportJob`, `readD1ImportOriginals`, `readD1PersistedImport` and
`readD1ImportUndoReport` execute SELECT-only SQL, including their initial authority
check and final atomic fence. Their guards retain scalar predicate data and pack
balanced `SELECT CASE WHEN (...) THEN 1 ELSE 0 END AS ok` statements into one
D1 batch. Every scalar result must pass before any candidate is returned; false,
NULL, missing, malformed or failed guard results reject with `IMPORT_ATOMIC_GUARD`.
The final fence still covers current authority, exact binding/plan/custody,
inventory and full report/provenance/native snapshots at the existing boundaries.
Bounded job, original and report reads split oversized custody checks into
SELECT-only batches at the smaller of the caller limit and 2 MiB. Each batch
checks the same monotonic custody revisions, including tombstone changes;
the original plan revision is captured before decoding. This requires the
additive custody-state schema. Ordinary reads without a transport policy retain
their existing atomic admission. `readD1PersistedImport` keeps one complete
atomic native snapshot capped at 2 MiB even when its caller omits a limit.
Reads change neither tables nor SQLite `total_changes()` and work with SQLite
`PRAGMA query_only=ON`. Mutation batches retain inside-transaction CHECK assertions
and rollback. Planning uses its existing `plan` authorization path. The ordinary-job SELECT fence needs no migration; the bounded stream receipt
requires the additive stream revisions described below. The assertion table
remains required for writes.

`readD1ImportStream` reads its parent, ordered pages and ordered child statuses
in one SELECT-only batch. It checks current `read` authority on both sides of
that snapshot. Creation/replay uses `create`; resume uses `apply`; undo uses
`undo` and also checks the selected child's reversal action. Each return is
admitted using that same builder before a child can publish. SQL inputs contain
only scalar identity and revision bindings, never a JSON copy of every page or
job. The complete batch includes SQL, JSON escaping and authority parameters in
the existing 2 MiB ceiling (or a lower selected `maxBatchBytes`). Output arrays
still grow with page count; this is not a total Worker-memory qualification.

The additive `ik_d1_stream_revisions` schema is included at the end of the existing
`d1StreamOriginSchema` export, so the migration export order remains unchanged.
Install it in a new numbered migration; do not rewrite an applied migration.
Initialization captures existing parent identity once, after origin backfill,
and is idempotent. Reads still recompute the canonical layout hash and verify
contiguous ordinals, page totals, child binding and both origin directions.
A permanent parent-identity anchor and monotonic layout/progress revisions
survive deletion and reinsertion. Parent identity and layout, page membership,
child binding/operation and origin mutations invalidate the layout revision for
both old and new parents. Normal checkpoint, fence, state and erasure-count
writes advance progress only. A child fences immutable layout and its current
sibling order; the parent undo-direction transaction fences exact progress as
well and verifies its single intended increment after all cleanup mutations.
A fresh direct child ID also reads and validates the complete parent layout
before capturing its revision. Nested stream calls reuse only their own validated
request receipt, and every pre/postcondition still checks that revision and current
authority in SQL. Revisions convey snapshot identity, never authorization.

The read boundary accepts only Kit-constructed guards with associated scalar
predicates, never arbitrary DML or SQL text rewritten into SELECTs. Trusted
adapters must still provide inert scalar SQL and side-effect-free read methods;
this is not a sandbox for arbitrary adapter JavaScript or SQL. Tests cover actual
SQLite and local workerd D1 result shapes, zero changes, query-only reads, late
revocation/tampering, complete packed groups and request-budget rejection. These
are local runtime receipts, not hosted capacity evidence.

The product implements `D1TargetAdapter`:

- `read(database, destination, kind, id)` returns the complete native `TargetRow`.
- `matches(rowOrNull, destination, kind, id)` returns an exact SQL boolean snapshot
  predicate covering the native values, revision, attachment and related evidence,
  or complete absence for null. This is the optimistic concurrency fence. Checking
  only an ID or revision is insufficient; edits without revision changes must fail.
- Optional `matchesParts(rowOrNull, destination, kind, id)` splits the same exact
  snapshot into nonempty conjunctive predicates for large native entities. Every
  part and the scoped row counts are required in the same publication transaction.
  This preserves the existing scalar `matches` API for other consumers.
- `write(row)` and `remove(destination, kind, id)` return inert native SQL statements.
- `list(database, destination)` returns native rows for independent readback.
- Export requires either `matchesMany(rows, destination)`, with exact predicates
  covering the whole native set, or `matchesCount(count, destination)`, with a
  scalar SQL predicate for its exact cardinality. With `matchesCount`, the engine
  also guards every listed row through `matches`. The count must cover the same
  destination and native entities as `list`, including when the snapshot is empty.
  `matchesMany` must return at least one predicate even for an empty snapshot.
  Missing both methods or returning an empty predicate array fails export with
  `IMPORT_NATIVE_SET_GUARD_REQUIRED`.

`D1Sql` is `{ sql: string, params?: any[] }`; parameters are D1 bind values.
Adapters supply SQL; they must not send, enqueue, activate access, run lifecycle
hooks, mutate the import tables or contact external services. Use one effect to
represent a complete atomic native entity and its evidence. Contact normalization,
source consolidation, suppression precedence and historical consent interpretation
are product responsibilities. The planner rejects duplicate native target keys.

Planning checks current `plan` authority using SELECT-only fences before mapping
and before returning the reviewed plan. It does not require `read` authority and
does not write assertions or any other database state. Stream planning uses the
same boundary.

Call `await planD1Import(source, mapping, context)`, show its diagnostics and
fingerprint for review, then pass that exact `reviewedFingerprint` to
`createD1ImportJob`. `applyD1ImportBatches(jobOrId, context)` publishes bounded
batches. `batchSize` defaults to 100 source records, maximum 1000; each batch has
at most 1000 effects. Choose smaller batches for the product's native statement
count, D1 binding/statement limits and Worker CPU budget. `maxBatches` bounds work
per request. The port does not retry failed SQL, conflicts or revoked authority.
A failed batch leaves its prior committed checkpoint intact; the product can fix
the condition and explicitly call `resumeD1Import`, or `rollbackD1Import`.

Each batch guards the immutable plan, actor binding, checkpoint and durable fence
inside the same D1 transaction as native rows, originals, source ownership,
provenance and journal. Successful batches increment the fence. Unlike Node's
long-lived synchronous lease, D1 has no work outside its optimistic batch which
needs a lease: concurrent requests have one winner, and stale snapshots abort.
Terminating a request leaves either the entire D1 batch committed or none of it.
A later Worker instance resumes from the retained plan without parser, mapping,
credentials or source download. The native adapter and current auth context must
be provided again.

Reusing a source claim validates its owner's immutable plan, committed checkpoint
and recorded decision, including entries with no native effects. The owner,
plan chunks, decision and claim are fenced in the publication transaction, so
an unrelated owner or a concurrent edit cannot publish a reused decision.
Previously reused owners' complete selected decision, claim, active provenance and
history sets also carry cardinality guards, including empty sets, so an insertion
after preflight cannot escape their full-field snapshots.
Previously reused source-only entries also revalidate their claim's existence,
owner, source hash, mapping version and effects hash before each publication;
snapshotting an already corrupted claim cannot authorize another checkpoint.
An owner's reversal may release its source-only claims while retaining valid
historical replay decisions.

`undoD1Import` requires a completed job; `rollbackD1Import` reverses partial work.
Reversal intent is committed first, preventing subsequent forward apply even if
a native edit blocks undo. Effects reverse in exact reverse plan order. Each
bounded transaction compares native values and revisions, restores before images,
releases only owned source claims, and records reversal. A stale native snapshot
inside the batch produces `IMPORT_ATOMIC_GUARD`; an edit visible at preflight
produces `NATIVE_EDIT_CONFLICT`. Both preserve the edit. Resume dispatches to the
persisted reversal mode. Completed reversal is idempotent. Originals and all
provenance, including reversed before images, remain durable.

Strict reversal without a transport policy (`maxBatchBytes` omitted) verifies the
complete inventory, originals and plan in the intent transaction or in the first
batch it runs, then records in `ik_d1_reversal_fences` the job's
`ik_d1_custody_states` revision after that batch's own writes. While the job's
revision, fence and binding still equal that row, later batches and later requests
read only their own provenance rows, native targets and predecessors, and fence
the revision instead, so a batch costs the same in a large import as in a small
one. Any change to the job's custody, plan, originals, decisions, claims, history,
journal or indexed tombstones advances the revision, and the next request verifies
the complete inventory again before it writes. Jobs that reuse another job's
entries, jobs without a current erasure index, preserved-conflict undo and bounded
transport keep the complete per-batch verification. A fenced batch releases only
its own sources' claims; the final batch releases the job's remaining claims,
including sources with zero effects. Install the additive `d1ReversalFenceSchema`
(the fence table and indexes on provenance by job and by source, history by
source and claims by job) in a new numbered migration after `d1CustodyStateSchema`.

`readD1ImportJob`, `readD1ImportOriginals`, and `readD1PersistedImport` expose status,
digest-checked original bytes and native/provenance readback. Custody is also
rechecked inside each publication batch. The D1 tests use actual Miniflare/workerd
D1, durable runtime disposal/reopen, concurrent invocations, native contact and
evidence tables, same-revision edits, atomic SQL failure, and authority revocation
between preflight and commit. A Worker entry test loads this graph in workerd.
These tests establish local provider-runtime behavior, not a live deployment.

## Sequential imports and ownership history

A distinct source may replace a target attributed to a completed D1 import when
its reviewed effect contains the exact current `expectedRevision`. The engine
compares the complete native value and attachment with the previous provenance,
then atomically moves that unchanged provenance into `ik_d1_history` while
publishing the successor and its full before-image. The active-target unique
index remains enforced. Changed content under the SAME source identity still
fails; products must use stable identities that distinguish distinct exports.

The additive `ik_d1_history` table and index are included in `d1Schema`; apply
these CREATE IF NOT EXISTS statements in the next product-owned migration for
existing D1 installations. No existing schema or provenance needs rewriting.

Undo or rollback of a superseded job fails with `IMPORT_SUPERSEDED` before
changing its state. Undo the newest layer first: this restores the exact native
before-image AND the predecessor's active provenance and source ownership. Then
the predecessor can itself be undone. Native edits and concurrent successors
still fail their exact snapshot guards. Partial reversal resumes across requests,
and original source bytes, before-images and attribution remain retained across
all layers. An import still applying or reversing cannot be superseded
(`TARGET_OWNER_BUSY`). The synchronous SQLite API retains its original behavior.

## D1 request and storage limits

The bounds below follow [D1 limits](https://developers.cloudflare.com/d1/platform/limits/).
Bulk text insertion uses the supported [D1 JSON SQL functions](https://developers.cloudflare.com/d1/sql-api/query-json/).

The port counts SQL executions over the entire public invocation (including
nested resume reads and every atomic batch statement), caps them at 1,000 and
returns durable partial progress as the budget approaches exhaustion. Explicit
`IMPORT_D1_REQUEST_LIMIT`, `IMPORT_D1_STATEMENT_LIMIT` and
`IMPORT_D1_VALUE_LIMIT` failures publish no portion of the rejected batch.
Create, apply and reversal reserve both return reads before publication, including
the read of a chunked plan. Each successful batch retains its checked job state;
if a later batch's preflight or exact SQL count exhausts the same request budget,
the invocation returns that durable partial progress. Other adapter, integrity
and authority failures still reject. A first batch that cannot fit rejects before
native publication; reversal intent may already be persisted to prevent forward
apply, but no native effect or reversal journal advances.
Adapters must make bounded reads: implement `list` with set queries, not one
query per entity. Optional `matchesMany(rows, destination)` supplies exact bounded
set predicates for export, including native cardinality and every related field;
otherwise `matchesCount` supplies the cardinality fence and the engine packs the
individual `matches` predicates. A count alone does not protect native values,
and individual row predicates alone cannot detect insertions into the set.
Native writes must still provide their exact preconditions and postconditions.
Adjacent atomic guards are packed into balanced boolean trees so packing adds
logarithmic expression depth. Every adapter predicate must itself be valid D1
SQL; packing preserves its complete contents and bound-parameter order. Binding,
SQL-byte and value-byte ceilings still apply to each packed statement.

Install the additive `ik_d1_plan_chunks` and `ik_d1_original_chunks` CREATE
statements from `d1Schema` in a new product migration. Existing inline plans and
version-1 inline and version-2 binary-chunk original sets remain readable.
Plans larger than 128 KiB are stored as a digest/count/size manifest and ordered
UTF-8 chunks. New original sets use version 3: metadata rows have NULL bytes,
and canonical base64 TEXT is retained in ordered 128 KiB chunks in the existing
`ik_d1_original_chunks.bytes` column. SQLite BLOB affinity preserves bound TEXT;
no SQL binary decoder is required. Empty retained originals have one empty TEXT
chunk. Unavailable originals have no chunks. Version 3 needs no new migration
when both chunk tables already exist. It does require this reader version; do
not roll back to a binary-only reader after creating version-3 jobs.

Plan chunks are inserted with bounded `json_each` batches, each bound JSON
string at most 1,900,000 UTF-8 bytes including escaping and array punctuation.
For chunked plans, original chunks are copied from that exact base64 text using
bounded positional descriptors and SQLite `substr`, inside the same transaction.
Offsets account for Unicode code points and split boundaries. Original bytes
remain independently stored; they are not reconstructed from parsed source data.
Inline plans bind their small original chunks directly. Publication guards first
compare all plan chunks to exact snapshots, then compare original chunks with
those same guarded positions, plus complete metadata and cardinality checks.
This avoids sending two complete copies of base64 in one D1 batch request. Reconstruction
verifies complete bytes, ordinals, counts and digests. Publication also compares
the exact manifest and every chunk, including original bytes; a stored digest is
never the concurrency fence. Erasure integrations must delete both new chunk
tables along with existing job custody. These tables retain audit data through
undo and rollback.


## Erasing normalized personal records

`eraseD1ImportTarget(kind, id, nativeStatements, context)` erases an entity's
normalized source payload, effect, record-scoped diagnostics and provenance
before-images across all destination jobs, including inline and chunked plans.
It executes the supplied inert native SQL and the kit reconciliation in one D1
batch. Supply the native erasure and anonymized audit statements; the adapter's
complete current snapshot and absent postcondition must cover all dependent
native rows. Current `erase` authority is checked at both ends of the batch;
this destination-level permission is independent of the original importing actor.
The result contains the native statement results and number of erased decisions.
Job status reports `erasedRecords`, so reversal can disclose records already erased.

By default this primitive covers one entity per source record with unavailable
original artifacts and no retained attachments. Shared source records and retained
original bytes fail explicitly before any mutation. A product can deliberately
select `context.originalErasurePolicy = 'withdraw-affected-artifacts'` for erasure.
This privacy policy withdraws the entire original artifact containing an erased
source, including other people's raw records in the same file/page; their normalized
records, native rows and resumable job remain. It also withdraws byte-identical
artifact copies and originals referenced by duplicate occurrences of that source.
Other original artifacts retain their exact bytes. For a mapping assembled from
multiple pages, select `context.originalErasurePolicy = 'withdraw-all-job-artifacts'`
to withdraw every artifact in each affected job. Use that conservative policy when
one record location cannot describe all contributing raw pages, as with event and
invitee snapshots. All normalized peers and their native rows still remain.
The first explicit withdrawal binds the job's policy; selecting a different
explicit policy later fails with `IMPORT_ORIGINAL_ERASURE_POLICY_CONFLICT`.
Attachments and composite source effects remain unsupported and fail before native mutation.

The selected policy is snapshotted for the request and recorded in the changed
job's fingerprint-bound `plan.erasure.originalPolicy`. `readD1ImportOriginals`
returns affected artifacts with `{ status: 'unavailable', reason:
'unavailable-after-erasure', policy: <selected policy> }`. Artifact
names/keys and associated source/provenance locations become opaque, hash-derived
metadata; bytes are deleted, never rewritten into a claimed original. Original
hash and size remain as non-content custody evidence. Artifact-scoped and unscoped
diagnostics are removed when withdrawal occurs. Unaffected artifacts, mapping
values and ordinary peer undo semantics remain intact. No migration is needed.

This is lineage-based erasure: the consuming reader must account for every
personal record in the retained artifact and associate it with its artifact hash
and location, including repeated occurrences. The kit cannot infer a person's
identity in arbitrary raw text, other records' payloads or unrelated native
stores. The caller must select every affected native entity and supply its full
native deletion/anonymization SQL. Original privacy policy does not authorize
reading or rewriting any external source. Both ordinary and transport-bounded
D1 erasure use these policies; size/admission limits still fail before publication.
Scope personal diagnostic text to its source
kind and ID or source location so the same erasure removes it. Do not place personal
record data in unscoped diagnostics or other records' metadata.

### Related copies of the erased person

`eraseD1ImportTarget(kind, id, nativeStatements, context, related)` also erases
retained copies the product recognizes outside the target's own lineage.
`related.source(record)` is asked about every unerased source record in the
destination, including rejected rows and records that map to other targets;
`related.original(bytes, { name, sha256 })` is asked about every retained original
artifact. Matching records are erased like the target's own sources, bound to the
erased target's identity, with their effects severed: their provenance and
before-images are deleted, so undo leaves the native rows they produced in place.
`nativeStatements` must redact those rows in the same batch. Matching originals
are withdrawn under the selected `originalErasurePolicy`, which is required. The
exact destination job set and fingerprints read by the scan are fenced, so a job
created or changed before the batch aborts it with `IMPORT_ATOMIC_GUARD`. The scan
reads every retained plan and original in the destination; bounded erasure
(`maxBatchBytes`) refuses `related` with `IMPORT_RELATED_ERASURE_UNBOUNDED_ONLY`.
Matchers receive decoded personal data: they must be pure and must not log it.

Erasure replaces the record with an explicit zero-effect tombstone, keeps source
identity and ordinal, and rehashes the plan, source claims and committed decisions.
The changed plan fingerprint fences any apply or reversal prepared before erasure.
It preserves other records and their exact undo guards. Original operation-key
replay returns the same redacted job, while a new operation involving an erased
source or native target is refused before retaining its plan. Hash-only tombstones prevent
replay even after undo releases transient source claims. Resume continues ordinary
remaining work; undo skips the erased effect and remains idempotent. Never remove
retained jobs as an erasure shortcut.

Existing installations must apply the separate additive `d1ErasureSchema` (the
original `d1Schema` is unchanged), then call `upgradeD1ImportErasureIndex(context)`
in bounded pages until `complete`. This indexes prior inline and chunked plans
without changing them. Each page requires current erase authority and fences the
exact source plan. Erasure fails before any native change while that destination
has unindexed jobs. New jobs write their index atomically, so subsequent erasure
looks up only affected copies and their inventory owners. Indexes contain hashed
source/target identities, ordinals, job IDs and plan fingerprints, never personal
answers or raw source/native IDs. Set reads and writes share one publication
snapshot across repeated imports, preserving all existing inventory guards.

Every insertion, update or deletion of a derived index row invalidates that job's
completeness receipt through database triggers. The bounded upgrade rebuilds the
rows from the exact source plan and writes its receipt last in the same transaction.
Apply, resumed create and reversal also check live source and target tombstones
independently of the derived index; only explicit erased zero-effect entries are
exempt. A stale or corrupted private plan can never use a missing index row to
resurrect a tombstoned source.


## Erasing a person from import custody

`eraseD1ImportTarget` removes a person's records but leaves any header the
product filled from that person: a job or stream namespace, an operation key,
an original's name, a private stage's header. `retireD1ImportCustody(person,
nativeStatements, context, checkpoint)` erases the person from all of import
custody and writes one erasure journal row, in a single D1 batch with the
supplied native statements, the restore checkpoint and current `erase`
authority at both ends.

```ts
const native = await erasure.nativeStatements('person', { id });   // modules/erasure
const checkpoint = (await portability.redactionStatements()).map(toD1Sql);
const entry = await retireD1ImportCustody(
  { target: { kind: 'person', id }, values: [person.email] }, native.statements, context, checkpoint);
await erasure.deleteBlobs(native.operationId);
```

Matching uses exact identifiers only, never substrings. `target` names the
native entity the person's records were imported into; those records become
zero-effect tombstones in every job exactly as `eraseD1ImportTarget` makes them,
so other people's records in the same job still resume and undo. `values` are
exact strings, typically the person's email: every job, stream, stream origin,
revision anchor, private stage and original whose header cell equals one of
them is found. Such a job cannot be renamed without changing the source
identities that undo and resume rely on, so its whole custody is retired: plans,
originals, provenance, history, reversal reports, entries, sources, stages,
indexes and the job row are deleted, and `ik_d1_retirements` keeps a plain row
(job or stream id, destination, receipt id, time). Retirement closes over every
stream the job belongs to, jobs whose entries reuse its sources and preserved
history in both directions, and the commit refuses if that closure changed after
it was read. Native rows a retired job already wrote stay; it can no longer be
resumed or undone, and `readD1ImportJob`, `resumeD1Import` and `undoD1Import`
throw `IMPORT_JOB_RETIRED` (streams: `IMPORT_STREAM_RETIRED`) carrying the
`receiptId`. A retired stream's revision anchor keeps its counters and layout
fields and drops its namespace and operation key, so its id can never be reused.
The batch then proves that no header cell in the destination still equals a
named value. Originals holding the person are withdrawn under
`context.originalErasurePolicy`, which defaults to `withdraw-affected-artifacts`
here. Pass `related` as for `eraseD1ImportTarget` to sever other records that
mention the person; a name is not an identifier, so do not pass one as a value.

Rewriting a job into tombstones keeps its row keys, so per-row restore
tombstones cannot fence it, and an archive made before the erasure may hold the
person only in import custody (still pending, or only in a header). `checkpoint`
is the product's restore checkpoint for rewritten rows: portability's
`redactionStatements()`, as `D1Sql`. The batch carries it whenever it has a
`target` or retires anything, so `rer import --journal LATER_ARCHIVE` refuses any
older archive whole; it is required then (`IMPORT_RETIREMENT_CHECKPOINT_REQUIRED`).
A batch without it (nothing matched, so nothing is fenced) must also carry no
`nativeStatements`: `IMPORT_RETIREMENT_NATIVE_REQUIRES_CHECKPOINT` refuses before any
write, because SQL the kit cannot fence could rewrite a row it does not count, such as
another import's job. With the checkpoint, and nothing else to do, it must leave every
row of the retired scope unchanged, or it aborts.

The journal (`ik_d1_erasure_journal`) and `ik_d1_retirements` are append-only:
database triggers refuse updates, deletes and any insert over an existing key,
so `INSERT OR REPLACE` cannot rewrite a row. Each row holds the receipt id, destination, time, the number
of records erased, jobs and streams retired, and per store the number of rows
the erasure deleted or overwrote (`stores`). Counts come from row images taken
and compared inside the same transaction in `ik_d1_erasure_tally`, which is
empty again before the batch commits; bookkeeping counters, tombstones and the
journal itself are not counted. It holds no erased value. Read it with
`readD1ErasureJournal(context)`; the function returns the row it wrote.

Apply `d1RetirementSchema` as a new numbered migration after the other kit
schemas (existing applied migrations stay unchanged); `initializeD1ImportStorage`
applies it too. It adds the three tables and replaces the stream revision update
trigger with one that also admits the retirement transition. A store that
already applied an earlier `d1RetirementSchema` applies `d1RetirementAppendOnly`
(the two insert triggers) as its next numbered migration. Keep the journal and
retirements in archives. Restoring an archive made before the erasure brings
nothing back: the native rows are fenced by the erasure module's
`nativeStatements`, and import custody by the checkpoint. Without an operation
key this remains a single transaction. A bounded call supplies an operation key
as described below; omitting it with `maxBatchBytes` raises
`IMPORT_RETIREMENT_OPERATION_KEY_REQUIRED`.

`person.related.fields` can stand without a `target` (a person who appears only
inside other people's records), and `{ resumable: true }` spreads a redaction
across requests in bounded steps; see the kit README, "Erasing fields in
retained shared records".


### Resuming bounded retirement and erasure

Apply `d1ErasureProgressSchema` as a new numbered migration after the existing
kit schemas. `initializeD1ImportStorage` includes it. Keep completed progress
in archives alongside the erasure journal and retirement rows.

`retireD1ImportCustodyStep` (also `retireD1ImportCustody` with an `operationKey`
option) prepares oversized legacy custody over multiple requests. Reuse the
product's durable operation key, person, native SQL and checkpoint each time:

```ts
const result = await retireD1ImportCustodyStep(person, native, context, checkpoint, {
  operationKey: operation.id,
  maxStatements: 800,
});
return Response.json(result, { status: result.phase === 'pending' ? 202 : 200 });
```

Each invocation executes at most `maxStatements` SQL statements, including reads
(default and maximum 1000). Mutation batches fit the smaller of
`context.maxBatchBytes` and 2 MiB; omission chooses 2 MiB. Admission uses the shared
D1 transport's UTF-8, JSON escaping and whole-call string-width accounting. It
admits a useful future step and the indivisible publication, without charging
all future staging batches to this request. `admissionOnly: true` compiles using
real reads and writes nothing. Its pending descriptor does not create a job.

`phase: 'pending'` reports the operation id and verified index/value counts. A
later invocation recompiles the custody generation and obtains its offset from
`ik_d1_erasure_progress`. Each insertion verifies exact rows after triggers and
advances that journal in the same transaction. A failed transaction leaves the
previous offset; a lost acknowledgement resumes from the committed offset.
Changed custody starts a newly guarded generation. An offset disagreeing with
the stage binding, revision or physical counts is refused. Request options do
not accept numeric offsets. Progress stores digests and counts, never selected
values; private normalized values remain under native staging privacy guards.

Only final publication changes native rows, rewrites plans, retires the header
closure, advances the restore checkpoint and writes the receipt. It also removes
the private stage. Reusing a completed operation returns the same receipt without
applying its writes again. `nativeResults` is present on the publishing response
and empty on a completed retry. Different request bindings under the same key
raise `IMPORT_ERASURE_OPERATION_CHANGED`. Keep identities distinct for distinct
erasures. Provider errors preserve progress because a batch may have committed.
`cancelD1ErasureStaging` removes private staging under current erase authority;
a later invocation can begin preparation again.

`eraseD1ImportTargetStep(kind, id, native, context, options)` provides the same
journaled continuation for target-only erasure. Use retirement when headers and
restore checkpoints also apply. Existing one-invocation erasure remains usable.

For additional product custody rewrites, such as removing legacy mapper
`source.options`, use `stageD1ImportErasure(context, options, binding, build)`.
`build(id, boundedContext)` returns `D1ErasureStageSpec`: deterministic,
authenticated fragments, native snapshot guards, target publication declarations
and final predicates. Always use its supplied context for database work. The
binding identifies immutable product erasure intent. Optional `publication`
statements surround the compiled writes; its `tail` runs after retained values
are consumed and the stage is discarded, in the same transaction. This supports
product journals, restore fences and work-item completion. `survivingJobs`
excludes jobs deleted by that tail from final custody seals. Exact replay,
privacy membership and native publication checks still apply.

`NativeStageRows` accepts an opaque `VerifiedNativeStageOffset` from
`verifyNativeStageOffset`. Supply the journal's counts and authentic journal
guards. Verification binds it to the context, generation, baseline, table and
canonical columns. Replay starts at the beginning and skips only that verified
prefix. Its optional checkpoint callback runs atomically with each insertion;
a forged object or truncated replay is refused.

## Normalized paged imports

`planD1ImportStream(source, mapping, context)` supports normalized sources with
unique record identities, unavailable original bytes, no attachments and no
unscoped source diagnostics. Other sources continue to use `planD1Import`.
Input records may be an array-compatible lazy view. Mapping runs once per record;
normalization uses the shared planner, then partitions at 100 records, 100
native units (one per effect, at least one per record) and about 512 KiB of
normalized source/effect content. Undo cost grows with a child's units, so many
tiny effects split into more children. Partitioning counts shared literals
once, including short statements and small dictionaries, so repeated immutable
metadata does not multiply a dictionary across pages. An individual native entity remains
atomic. Distinct source identities with equal values remain distinct records.

The returned compressed pages are lossless normalized plans. A one-page view
computes the same complete canonical fingerprint as the eager planner, including
source order, diagnostics and counts. During initial review, fresh bounded source
copies are checked against the canonical hashes observed during normalization;
this avoids reconstructing native tables twice just to read their source records.
The returned stream retains no original source or mapping closure. Canonical size and integrity comparisons
do not allocate complete expanded JSON strings. Creation can reuse that computed
fingerprint only while every page byte, length and header field matches its
private in-memory snapshot; changed input receives full canonical verification. Review this fingerprint, then call
`createD1ImportStream` with `reviewedFingerprint`. Creation publishes all pages, original availability, hash-only erasure indexes
and the sealed parent layout atomically after bounded private custody staging. No native effect is published during creation. Publication groups each child
identity once with its index rows and derives shared original-availability
receipts from the atomic page membership in SQL, avoiding repeated metadata
in the D1 transport. The normal request budget
and exact authority checks apply; insufficient budget publishes no parent or page.
The input and compressed pages still occupy memory in the creating request;
products must qualify their actual admitted source limits. Publication transport
retains only one bounded row set, not every SQL value for the complete stream.

`readD1ImportStream`, `resumeD1ImportStream` and `undoD1ImportStream` accept the
parent ID. Each resume or undo invocation operates on one bounded child plan,
using the existing native engine, custody, source reuse and optimistic guards.
The parent retains the reviewed fingerprint; each child retains its own canonical
plan fingerprint. Resume needs neither uploaded files nor the original mapper.
Reversal proceeds in reverse page order and persists its direction before native
changes. Direct child entry points also enforce parent direction and ordering.
Version-4 original availability receipts bind normalized child custody to a live
parent even if its membership and parent rows are both removed. Ordinary version-1/2/3
receipts keep their existing interpretation and arbitrary operation keys.
Page membership mutations invalidate the parent; ordinary erased-record progress
does not alter the sealed layout. Erasure redacts every affected child and reports
aggregate erased decisions in parent status. Original-operation replay returns
that same parent; new operations still honor source and target tombstones.

Large child pages use digest-checked compressed encoding inside the existing exact
chunk manifests. The manifest and every compressed byte remain publication
preconditions. Compression does not replace snapshot fencing or source identity.
Existing eager create retains its pure admission and one atomic original-custody
publication; private normalized stream staging is a separate operation.


A native adapter may provide a versioned `planCodec` with pure `encode(plan)` and
`decode(payload)` operations. This is a lossless storage representation, not a
mapping callback: every source record, native field and order must reconstruct
exactly. The kit verifies the full canonical plan fingerprint on encode roundtrip
and every decode, rejects an absent or mismatched codec, and applies the same
manifest/chunk byte fences. Consumers must retain every shipped decoder version.
Compact derived-value encodings can keep complete normalized custody small even
when native tables expand accepted input substantially. All encoded pages and
indexes still publish atomically before native apply; no original input or mapper
closure is needed after an interruption. Erasure replaces the entire affected
encoded page and removes its old chunks, including all encoded source copies.

Normalized streams account for all repeated literals when choosing retained
pages, including short Unicode statements. When repeated metadata dominates a
plan's expanded JSON, adjacent normalized pages share a bounded dictionary
working set before compression, bounded by two input pages and 8 MiB of
expanded JSON; a larger individual input page is processed on its own. Mapping still runs once per source record;
canonical fingerprints, exact native values and transactional guards are
unchanged. Ordinary pages retain their expanded-byte boundary. Erasure rebuilds
the dictionary from the redacted plan and retains compressed custody, so
removed values cannot survive in unused dictionary entries and a privacy action
does not materialize the entire expanded plan in SQL buffers.

Dictionary version two also shares literals inside exactly serialized JSON
strings. A native JSON answer and its normalized source can therefore share an
immutable statement while decoding to their original distinct JSON types. The
decoder reconstructs each serialized string once per page, validates every
reference, and rejects unused or duplicate entries. The envelope records the
exact encoded payload size; embedded text is size-checked before reconstruction
so a small reference graph cannot allocate text beyond that bound. Strings with significant
formatting retain their original literal bytes. First-version dictionaries
remain readable; erasure rebuilds all tables from the remaining values.

Canonical hashing uses a 64 KiB byte buffer and a per-call cache bounded to
4,096 strings and 1 MiB of encoded literals. It traverses mutable containers on
every visit and preserves the same canonical UTF-8 bytes and fingerprint.
### Pure creation admission before product changes

Call `preflightD1ImportJob(plan, context)` before adopting native schema or
permissions. It is synchronous and needs no database or target adapter. The
`D1CreatePreflightContext` includes destination, namespace, mappingVersion,
authorization, reviewedFingerprint and optional operationKey. Use the complete
actual reviewed plan and operation key and the same pure authorization predicate
used by production creation; the predicate builds SQL and must have no side effects.
Admission does not execute that predicate or authorize anything. Production
evaluates current authority inside the initial and final atomic transactions.

The returned `D1CreatePreflight` contains `mutationStatements`,
`totalExecutions`, `maxSqlBytes`, `maxBoundBytes` and `maxParameters`. It uses the
production serialization, chunking and guard packing, reserving initial authority,
operation-key lookup, job return and chunked-plan return reads. The 36-byte
placeholder UUID has exactly the encoded size of the production-generated UUID.
Invalid plans/bindings and `IMPORT_D1_REQUEST_LIMIT`,
`IMPORT_D1_STATEMENT_LIMIT`, `IMPORT_D1_VALUE_LIMIT` throw before any product
mutation. The limit is the actual encoded work, not a synthetic artifact-count or
file-byte ceiling. Admission covers fresh job creation; replay depends on the
existing job inventory, and later native effect batches require their own bounds.
It does not predict provider storage availability, Worker memory/CPU allowance or
validity of consumer SQL. The product must check those independent boundaries.
New plans or submitted creation plans over the inclusive 300,000,000 canonical
content-byte `PLAN_BYTES_LIMIT` are refused with `IMPORT_PLAN_TOO_LARGE` before
encoding/cloning mapper effects or encoding the retained plan. Content excludes
the fingerprint field; the complete stored plan reserves 81 more bytes. Source,
repeated entry source, effects, metadata, diagnostics, base64 and JSON escaping
all count. The error carries numeric measured `bytes`, `limit` and `subject`;
show these in preview before writes. The cumulative planner stops at the mapping
that exceeds the cap. Creation guards again after awaited boundaries, including
operation-key replay, without replacing current-authority or reviewed-fingerprint
checks. See "Plan-size cap" in the kit README for current memory/request evidence:
the 4 GiB Node heap ceiling does not bound total RSS, and hosted capacity remains
unproven.


The default D1 suite includes twenty distinct 10 MiB originals with native
publication, replay, undo and exact retained-byte readback. The local
Miniflare/workerd capacity probe uses the real D1 binding and meters each entire
public invocation. This is local D1 SQL/custody evidence: a 200 MiB source and
its complete plan have substantial Node/runtime memory costs. It does not
establish hosted Worker memory, CPU, multipart upload or product parser limits.
Nor does it establish capacity for twenty maximum-content CSVs: a valid example
with 105 rows/table, two fields and 10 MiB/file requires 910,382,498 plan content
bytes. The kit cap is an additional preview/storage bound; retain the product's
separate table/field/file/row/cell limits and never strip custody or parsed data to
make admission pass.

## Adopting preserved-conflict undo (evolution 0001)

Existing installations must apply every statement exported as
`preservedUndoEvolution` from `src/d1/evolutions/0001-preserved-undo.ts` in a NEW
product-owned numbered migration. Do not edit an applied migration. The evolution
adds the durable policy, its job/fingerprint binding, report cursor and report
hash to `ik_jobs`, and adds `ik_d1_reversal_reports` and the transaction-local
creation snapshot table/triggers `ik_d1_creation_plans`. Existing jobs default to
`strict`; their provenance and custody are unchanged. `d1Schema` contains the
complete fresh-install schema including this evolution (its ALTER statements are
one-time DDL). `initializeD1ImportStorage` applies the evolution on first setup.
The synchronous SQLite engine is unchanged and does not use these D1 columns.

New jobs may select `context.reversalPolicy = 'preserve-conflicts'` at creation.
Omission creates a strict job. Later calls may omit the option to use the durable
policy, or pass the same value; a different explicit value rejects with
`REVERSAL_POLICY_CONFLICT`. The policy and its binding are fenced alongside the
job, original custody and plan. Creation admission measures these statements,
all exact custody guards, initial authorization, and the actual return reads.

Strict undo retains its full native revision/value/attachment comparison and
`NATIVE_EDIT_CONFLICT`. Preserved undo still requires completed imports and the
current ordinary `undo` authorization in every transaction, including batches
which only record conflicts and the terminal batch. The same selected policy applies to rollback of partially published work;
its report covers only the effects already committed. Undo and rollback cannot change an existing reversal mode.
A job beneath any successor still rejects with `IMPORT_SUPERSEDED`.

To support preserved undo, implement `targets.preparePreservedUndo(db,
destination, effects)` and, only when native effects own disjoint parts of a
shared snapshot, `targets.ownedContent(row)`. The types are exported from
`undo-types.ts`. `effects` supplies each active effect's immutable applied row,
original provenance and whether its retention is already durable. Return exactly
one observation per effect, its complete current row or null, native entity
labels, optional domain evidence, explicit dependencies, and nonempty scalar SQL
predicates guarding the complete current native/dependency snapshot. These
predicates must cover values, revisions, ownership markers, related evidence,
outside incoming dependencies, absent edges and cardinalities. The engine also
guards each observation through `targets.matches`; an observation may supply an
exact `guard` for domain-specific absence such as a deleted native row whose
ownership marker remains. These guards are evaluated inside the same atomic
transaction as native reversal, attribution, dispositions and progress.

`ownedContent` is a pure projection which Kit applies to both expected and
observed rows; its semantics must remain stable for existing jobs and reports.
It may exclude unrelated wrapper revisions but must retain every owned value,
attachment and owned revision. The complete current snapshot is still fenced.
An effect with a before-image uses the full revision/value/attachment comparison
to preserve exact predecessor restoration. Products own the relation, required
field and label rules; Kit does not infer them from table or field names.

A dependency says `requiredEffectId` must remain while `dependentEffectId`
remains, and the dependent must reverse first. An omitted dependent identifies
an outside live reference which protects the required effect. Retention is
transitive. `order: false` declares a retention-only edge, for example a retained
owner requiring a separately owned MANY join while removal must still order that
join before its owner. Kit validates references and rejects unsupported removal
cycles. It neither disables foreign keys nor deletes schemas or cascades.

`snapshot.atomicGroups` can declare effects that must reverse together, such as
an owner and its required MANY joins. Overlapping groups form one unit. Retaining
any member protects the unit, with explicit atomic-dependency evidence. Kit
orders both units and their members by dependencies. A batch normally processes
at most `batchSize` effects; an indivisible first unit may exceed that count and
is still bounded by the exact D1 statement/request limits. It is never split to
make a batch fit. This prevents an owner edit between batches from retaining a
required-empty owner after its join was already reversed. Edited
effects and protected dependencies get durable retained dispositions while their
original provenance, applied hash/revision, source claim, originals and any
predecessor history remain unchanged. Unchanged independent effects reverse,
and exact predecessors reactivate only for effects actually reversed. Conflicts
advance the durable cursor and fence, so mixed and all-conflicted imports reach
terminal `undone`. A repeated undo does not reconsider a retained decision.

`readD1ImportUndoReport(id, context, { offset, limit })` returns the policy, job,
full cursor/total, a disposition page and `nextOffset` (null at the end). The
default page has at most 100 entries; the explicit maximum is 1000. Every page
validates the full custody, provenance, source, history and report integrity
closure before returning. Each disposition identifies job/effect, native entity,
field/target where supplied, edited/dependency reason, original and observed
revision/value/attachment/owned-content hashes and values, domain evidence and
protecting causes. The original mapping plan remains retained and unchanged.
Report rows are bounded by the ordinary D1 statement/value limits; the engine
counts all snapshot reads and guards against the request's remaining budget.
Accumulated dispositions use byte-bounded JSON set comparisons for their exact
provenance IDs, owner IDs and reversal flags, including SQL storage types. Exact
report bytes, positions and a scoped row count remain fenced in the same batch;
the report hash is not a substitute for those comparisons. Resuming undo and
reading terminal pages therefore do not add one SQL predicate per prior effect.
For large native sets, a snapshot can supply `nativeRows`, the complete
destination set returned by `targets.list`. Kit requires `targets.matchesMany`,
rejects duplicate or differently bound rows, and compares each observation's
complete canonical value to that set (including absence). Kit then uses the
adapter's exact full-set predicates instead of repeating `matches` per effect.
Custom observation guards and the complete dependency guards still apply.
The native dependency snapshot and integrity closure are complete per batch,
so adapters must use bounded set queries; `batchSize` bounds mutations rather
than promising constant-memory reads of an arbitrarily large native graph.

`tests/d1-preserved-scale.test.ts` exercises 34,000 distinct effects in real
SQLite and local workerd D1. It measures complete public resume, report,
original-read and native-export calls, reopens durable databases, and injects
late changes in the final report/native chunks. The fixtures retain their plans
and originals through public creation, then seed completed history directly;
they test reversal and read scale without replaying 34,000 prior apply writes.
Separate small public apply/undo cases exercise mixed present, absent, edited,
unrelated and unchanged rows, typed values, authorization and publication
rollback. These are local provider and custody checks; hosted Worker resource
limits and a consumer's native adapter remain separate acceptance work.
The Node 26.8.1 SELECT-only read regression run measured at most 211 executed statements per
34,000-effect public invocation, 39 parameters per statement, 9,233 SQL bytes
and 1,832,989 aggregate bound bytes. A report page validated all 34,000 durable
dispositions in 103 statements; the complete native export returned 33,999
remaining rows and their provenance in 61 statements after one retained edit
and one actual reversal. Exact retained originals survived both reopen paths.

Creation binds each exact plan chunk once. A Kit trigger captures the original
INSERT `NEW.body` in `ik_d1_creation_plans`; its update/delete triggers reject
changes before release. Final guards compare every plan ordinal/body and count
against that protected snapshot and every original byte against its exact plan
positions. Only after all guards pass does the engine release and delete the
snapshot, in the same D1 batch. A malformed plan/original INSERT trigger therefore
rolls back job, originals and snapshots together. No staging survives successful
creation, and no custody crosses transactions. This avoids a second full plan
copy in D1's serialized batch request while retaining exact byte comparisons.

## Bounded private stream custody

### Acknowledged preparation and cold verification

Consumers may split normalized creation across two requests. Review still calls
`planD1ImportStream` and computes the complete canonical fingerprint without
writing custody. After the operator acknowledges that report,
`prepareD1ImportStream(source, mapping, context)` requires its fingerprint as
`reviewedFingerprint` and stores independently checked normalized pages under a
private generation. Its `ImportPreparation` result has `state: 'preparing'`,
`checkpoint: 0` and `expectedFingerprint`; it is explicitly unverified and is
never accepted as a public import job. No native form or response exists yet.

`publishD1ImportPreparation(id, context)` reads bounded private chunks and hashes
every value in the original canonical order, checks the expected fingerprint,
header, codec, complete page and privacy inventories, and current authority.
Only then does the existing atomic publication create the public stream. The
consumer can call `resumeD1ImportStream` with the same `d1RequestContext` to apply
the first bounded native batch without resetting the request budget. Cold
verification needs no uploaded files, parser or mapper.

`readD1ImportPreparation` is owner scoped and distinguishes complete private
preparation from `preparation-interrupted`. Interrupted writes can be retried
with the reviewed source or removed by `cancelD1ImportStaging(operationKey,
context)`. A sealed private generation cannot be overwritten by a stale retry.
Cancellation, erasure and verified publication cascade private headers and
receipts. An authority failure can leave private custody for an authorized
owner to inspect or cancel; it cannot confer publication permission.

### Preparation across requests

A large normalized file costs more CPU than one Worker request allows when it
is reviewed, prepared and verified in one go (a 100,000-row Typeform export
took about 55 s of hosted CPU to review and 62 s to prepare; Workers Paid allows
30 s). `prepareD1ImportSteps(source, mapping, context)` does the same work in
bounded steps instead. Call it with the same source, mapping and
`operationKey`, once per request, until its result is `reviewable`:

- `preparing`: each step maps, normalizes and stages 5,000 records as a private,
  sealed generation under the usual privacy admission (registered identities
  before payload, erasure tombstones checked at every write).
- `verifying`: each step reads 125 staged pages, makes cold verification's checks
  (page projection, privacy index, index and policy hashes, counts) and writes
  them into the complete canonical fingerprint with a SHA-256 whose state is
  kept in D1 (`ik_d1_preparation_steps`), fenced by every generation's
  revision. A changed, missing or reordered page fails the step or changes the
  fingerprint; nothing the caller sends can supply hash state. The last step
  copies the generations into one sealed preparation in SQL.
- `reviewable`: carries `fingerprint`, `counts` and `diagnostics`, the review.
  The fingerprint equals the one `planD1ImportStream` computes for the same
  source. Show it, and after the owner acknowledges call
  `publishD1ImportPreparation(id, { ...context, reviewedFingerprint })`, which
  publishes atomically without reading the pages again while the preparation is
  still at the revision the steps verified (any change sends it through full
  cold verification). Then apply with `resumeD1ImportStream` as before.

Unlike `planD1ImportStream`, this review is computed from private custody, so
the steps write before the owner acknowledges; that custody is private,
owner-scoped and removed by `cancelD1ImportStaging(operationKey, context)`,
destination field erasure, or header retirement of its operation key. A step
generation erased or changed between steps ends the preparation with
`IMPORT_PREPARATION_INTERRUPTED`: cancel and prepare again. A step whose request
ended after it staged its slice but before recording it is staged again by the
next call, and cancellation or retirement removes every generation the steps
staged, recorded or not. A different upload under the same operation key is
refused (`OPERATION_KEY_CONFLICT`), before and after publication: the published
stream keeps the steps' source digest (`ik_d1_stream_sources`), so only the same
upload returns `published`, and a key an import published in one request is
refused here. Measured on default-seed's people
adapter: 100,000 rows in 40 requests, none above 2 s of Node CPU against
15 s for the one-request review (see the kit test
`tests/d1/preparation-steps.test.ts`, run at full size in `verify:scale`).
Apply `d1PreparationStepsSchema` as a new numbered migration after
`d1PreparationSchema`; `initializeD1ImportStorage` applies it.

Apply the additive `d1PreparationSchema` after `d1CustodyStateSchema`.
`initializeD1ImportStorage` applies that order and preserves existing jobs.
The new tables contain safe normalized header custody and its exact receipt;
consumers must include both in their archive and erasure inventories. The
ordinary `createD1ImportStream` and legacy job APIs retain their original
verification and publication behavior.

Apply the separate additive `d1StagingSchema` after the erasure and preserved
undo evolutions. It adds private parents, pages, chunks, original availability
metadata and a hash-only identity index. It does not modify an applied migration,
the synchronous SQLite protocol, or ordinary D1 original-byte custody.

Each attempted stream is bound to the exact reviewed fingerprint, principal,
destination, namespace, mapping version, operation key and random generation.
The whole request reserves its statement budget before private writes. Every
stage transaction checks current authority at both ends, the exact binding,
expected mutation revision and source/target tombstones. Complete page identity
indexes are registered before payload storage. Any page/index mutation
invalidates registration; changing an index also removes its private payload
so interrupted custody cannot lose privacy discovery. Any child mutation
invalidates sealing. Final seal
checks page membership, ordinals, chunk sizes and counts against the reviewed
manifests. Each inserted row is read back inside its transaction and compared
column-for-column with the intended bytes. Exact revision fences then bind
those verified rows and hashes to publication,
including mutations that restore the original value.

Each transport contains at most 2 MiB of conservatively counted serialized JSON,
including SQL, escaping, binary binding expansion and actual authority guards.
A row set is at most 768 KiB before the outer transport encoding; payload chunks
are at most 64 Ki UTF-16 code units and do not split surrogate pairs. Statement
binding/value limits and the shared 1,000-execution request budget still apply.
One decoded normalized page and one pending row set are retained at a time.

Publication is a small guarded SQL-only transaction. It copies private custody
into existing job, stream, original and index tables with `INSERT ... SELECT`,
compares every copied set against its exact unchanged source, releases the
transaction-local creation snapshots, and removes the private generation.
There is no public job, stream or native effect before publication. Existing
published read, apply, replay and undo paths keep their custody and native guards.

Fresh creation, cold publication and exact replay return the public status from
one read-only SQL transaction under current create authority. Parent binding,
ordered complete pages, child status and immutable origin membership belong to
that same snapshot, with authority checked again at its end. No assertion cleanup
or other write follows those reads. The receipt's transaction commit is the
return boundary; a later change does not invalidate that completed snapshot.
Admission reserves the actual packed receipt and the reachable generation-only
cleanup, winner lookup and second authorized receipt before publication. Fresh
creation includes that same reservation before its first private write.

A fatal interruption may leave incomplete private custody. It is not a resumable
job and exposes no payload through public job APIs. A fresh authorized create
with the same binding replaces that private generation; stale writers and
cleanup cannot modify the replacement. `cancelD1ImportStaging(operationKey,
context)` removes only the current operator's exact private generation under
current create authority. An optional reviewed fingerprint fences cancellation.
The same owner can cancel an older mapping version in that namespace without
reopening its payload or adopting the older mapping implementation.
Cancellation and retry never remove a published import. Failed cleanup after
lost authority leaves inaccessible custody for an authorized retry, cancellation
or destination privacy erasure; it is not silently treated as a completed import.
No time-based automatic deletion is introduced.

Destination privacy erasure discovers staged target/source identities across
importing principals. The same atomic erasure transaction writes permanent
tombstones and cascades deletion through every intersecting private generation,
including partial payloads. Registration after an earlier erasure fails on those
tombstones. Other destinations remain outside the operation.

Ordinary version-three original-byte receipts without structural stream origin
remain usable and unchanged. Every initialization, including fresh installations,
atomically runs `d1StreamOriginSchema` and `d1LegacyStreamReceiptUpgrade`:
actual stream-page membership is captured durably, and only actual members move
from version three to version four. Captured origins and version-four receipts
reject missing or changed parent membership. If all structural evidence for a
legacy version-three job was already lost before origin capture, its stream
history cannot be classified; IDs, payloads and nullable reversal-policy bindings
are never used to guess it. Setup and conversion are idempotent; products retain
ownership of numbered migrations.

Preserved-conflict undo reports can contain cross-effect native evidence.
Privacy erasure validates their existing report and policy bindings, fences the
exact prior report rows, then records explicit redacted dispositions. Erased
effects retain only source/effect digests and their prior disposition. Remaining
destination reports lose free-form evidence, native IDs, values and value hashes;
their disposition/reason remains visible and their report hash is rebound in the
same transaction. Erasure never trusts a newly read hash as authority for changed
report contents. Strict undo keeps its existing native edit conflict behavior.

Canonical string escaping emits slices of at most one Mi UTF-16 code units,
with surrogate pairs kept together. Fingerprints and retained chunks consume
those slices without joining a whole large literal. Planning size walks share
only immutable string byte counts, bounded to 4,096 entries and 512 Ki UTF-16
code units per plan; container reads and size-limit checks remain fresh.
Mapper properties are captured once in canonical order before admission,
credential checks and retained cloning; shared container graphs are not expanded
before their logical size is admitted. Ordinary source cloning keeps its eager
canonical JSON semantics, while stream pages share immutable source strings.

The eager planner's content-size cap applies to each normalized child plan.
The complete stream is fingerprinted as one ordered logical plan without
applying the eager representation's aggregate cap to the compressed stream.
Products must still qualify their complete upload, expansion, CPU and Worker
memory limits. The bounded transport is not a claim of measured total isolate
high-water or hosted resource acceptance.


## Optional bounded native transport

`maxBatchBytes` is an explicit consumer policy. Omission preserves ordinary
D1 transport behavior and original-custody semantics. With a policy, apply and
strict reversal gather fewer whole source entries when their complete prepared
SQL, guards and progress writes would exceed the serialized batch ceiling. The
configured record cap remains an upper bound; one native entity stays atomic.

A native entity can exceed that transport ceiling even when its reviewed source
is admitted. An adapter supporting this case supplies `retainedSql(parts, params,
indexes)` for safe normalized text values and exact native-value predicates.
The literal SQL parts define the placeholders structurally. The engine never
rewrites arbitrary SQL, quoted question marks, comments or numbered parameters.
Original uploads, credentials, session tokens, capability bindings and arbitrary
environment parameters must never be marked as retainable.

Install the separate additive `d1NativeStagingSchema`. Its private parent, complete
hashed source/target index and ordered value chunks are bound to the current job,
plan fingerprint, principal, destination, namespace, action, fence and generation.
Related retained plan snapshots contribute their entire privacy membership. The
complete index is verified before any value bytes are accepted. Every bounded
write checks current authority, the originally observed custody/native revisions,
and tombstones; inserted columns are read back exactly before the next write.
All statements, readbacks, cleanup and return reads are admitted against the one
request SQL budget before a private write. Every stage batch has at most 2 MiB
of serialized transport, including SQL, JSON escaping and all guard parameters.

`stagingRead` captures a mutation stamp before reading native values. Its guards
must cover all fields and absence, including edits that retain a public revision.
`stagingPublication` protects the exact post-state while private bytes are removed.
It installs a known receipt only after full native checks, releases its temporary
fences, and supplies scalar final predicates for that expected state. Every native
mutation, including direct stamp changes, must invalidate the receipt; a receipt
cannot be installed without its matching publication fence. The engine does the
same for custody. Read-only final checks and current authority follow every
cleanup mutation. There is no assertion-table cleanup after those checks.

Final effects read retained values through SQL and commit native rows, provenance
and checkpoint in one bounded atomic transaction. No partially written native
entity or private staging object is returned as a public job. Fatal requests may
leave indexed private values; cold retry replaces only that owner's generation.
`cancelD1NativeStaging(jobId, context)` requires current owner authority and removes
private values without changing native progress. Reversal also cancels abandoned
forward stages. Destination privacy erasure discovers stages across importing
principals and removes every affected private copy together with its tombstones.
An old writer cannot republish after cancellation, erasure or a winning retry.

The additive custody revision table backfills existing jobs without rewriting their
plans, original versions, provenance or reversal policy. Product adapters must
ship their own additive native mutation-stamp and publication-fence schema in the
same evolution. Existing published reads, ordinary original custody and default
adapters keep their existing public contracts.

Stream page admission computes exact expanded size and dictionary weight once
per unchanged private page and reuses those local measurements for coalescing.
It does not cache mutable plans or change ordinary planner admission limits.

With the transport policy enabled, maintained native mutations carry explicit
intended row changes. Final checks compare the complete known provenance, source
claims, decisions, journal, job fields and custody metadata against the validated
preflight plus those changes. A receipt is installed only after these exact
comparisons. Transactions that fit directly perform read-only comparisons and
current-authority checks after assertion cleanup as well. Ordinary consumers
without the policy retain their existing transaction contract.

Privacy erasure uses the same bounded value transport when replacement plans or
native guards make its complete transaction larger than the selected ceiling.
Private preparation does not delete native data or redact public custody. Its
index covers all retained preflight and replacement values, including related
reports, before payload writes. Destination privacy authority is checked for
every write. The final SQL transaction removes native data, replaces/redacts all
affected custody and reports, installs tombstones and removes private copies
together. Interrupted private erasure can be retried under current privacy
authority. `cancelD1ErasureStaging(jobId, context)` removes only that privacy
operator's private erasure generation; it does not require ownership of the
other operator's public import or change that import. Original upload and
attachment bytes are refused by default. Explicit `withdraw-affected-artifacts`
erasure may stage the original-bearing plan under its complete privacy identity
index (either explicit withdrawal policy); the final transaction withdraws the selected originals and clears every
private copy. Attachment bytes remain refused. Ordinary apply/undo staging does
not adopt this erasure-only exception.

Stream resume and undo reserve their outer authority and complete snapshot
return before a child can retain or publish values. Nested calls share the
same 1,000-statement request counter, including cleanup and final read-only checks.


### Bounded privacy publication

For a consumer that sets `maxBatchBytes`, erasure groups small retained jobs
using numeric stored-byte and expanded-plan measurements of their complete
ownership closure. Set reads share a bounded page; larger closures occupy a
page alone. Every payload read requires the monotonic custody revisions captured
with those measurements, so a concurrent write cannot enlarge a measured page.
Decoded and rewritten plans are released before advancing to the next group.
Only small job headers, ownership IDs, ordered hashes, compact SQL references
and numeric transport reservations span those pages. A
direct transaction is retained only while it fits the configured whole batch
bound; a larger transaction uses the existing private native value tables.
There is no new default transport limit on ordinary omitted-policy APIs.

The additive `d1CustodyStateSchema` installs `ik_d1_custody_states` and monotonic
mutation triggers covering job fields, plan and original copies, provenance,
claims, decisions, history, journal, reversal reports and stream membership.
Prior jobs receive counters without changes to their public bytes. A counter is
captured before each retained report is validated; old cross-effect evidence is
never copied into private transport. The report's existing policy and content
hashes are validated before its explicitly redacted replacement is constructed.

The first read pass reserves both later read passes and all exact writes,
authority guards, readbacks, publication and cleanup before any private write.
A second pass reproduces the reviewed page hashes and registers all privacy
identities. Only then can a third pass retain bounded value chunks. Each batch
checks current authority and the original job, custody and native revisions;
each stored row is compared exactly with the intended bytes before registration
or sealing can progress. Private transport batches, including SQL, JSON escaping
and guards, never exceed 2 MiB or a lower configured bound. All passes share the
same 1,000-query request budget.

The final SQL-only transaction removes the native entity and all retained source
copies together, updates fingerprints and redacted reversal bindings, writes
privacy tombstones and discards private generations. Exact expected state is
checked before sealing. Read-only receipt, tombstone, absence and current
authority checks follow the last cleanup mutation. A fatal request may retain an
indexed, unpublished private generation; a cold retry rebuilds it against current
state. `cancelD1ErasureStaging(jobId, context)` removes only the current principal's
private erasure generations in that destination and namespace, without changing
public jobs or native values. Destination privacy erasure discovers matching
private generations across principals and deletes every retained copy atomically.

Configured bounded creation repeats its complete job, original custody and plan
index membership checks after assertion cleanup. The same final read-only
checks are included in its query admission. Intended-state tracking preserves
explicit absence checks for deleted provenance and history primary keys,
including related owners, until final publication has been sealed.

Erasing a formerly effectful entry retains its original target digest in the
fingerprinted plan as `erasedTargetIdentity`. Cold bounded reads, apply and undo
compare the exact original source/target index membership and require both
privacy tombstones. A source that never produced an effect keeps its separate
empty-target membership. An earlier unaccepted erased snapshot without the
original digest cannot be promoted into a new bounded binding by guessing or
trusting a replacement index row; it fails closed. Prior ordinary live custody
needs no conversion: erasure captures its reviewed target before removing values.
Omitted-policy ordinary APIs retain their behavior.

All privacy replay passes reuse the original numeric page/owner descriptors and
revision predicates. They never rediscover a changed ownership graph. A page's
first successful snapshot also records its exact read count; later snapshots
refuse an extra read before executing it. Admission conservatively reserves two
full first-pass read costs, including initial measurements that replay does not
repeat, plus every private write, final publication and failure discard. Thus a
concurrent native import can invalidate erasure without consuming its cleanup
reservation by expanding the newly visible history graph.
