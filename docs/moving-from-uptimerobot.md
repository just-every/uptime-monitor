You can bring your UptimeRobot monitors into Uptime without risking either service. Uptime reads a saved copy of your monitor list, shows you what it would create and what it cannot reproduce, and creates nothing until you apply. Every imported monitor arrives paused, so your UptimeRobot checks keep running until you have checked the Uptime ones and turned them on.

## Save your monitor list

Uptime does not sign in to UptimeRobot or call it. You save the list yourself from UptimeRobot's API v3 `GET /monitors` endpoint, using a read-only API key:

```sh
curl -s -H "Authorization: Bearer $UPTIMEROBOT_READ_ONLY_KEY" "https://api.uptimerobot.com/v3/monitors?limit=200" > uptimerobot-monitors.json
```

The response is one page, `{"data": [...], "nextLink": ...}`. If `nextLink` is not null, fetch that link too and append the next page to the same file. Uptime reads pages written one after another, or a JSON array of pages. Keep the file private: it can contain your heartbeat URLs and request settings.

## Preview and apply

Open **Definitions** in the console, choose the file and select **Preview**. Uptime detects the UptimeRobot format; you can also choose it under **Source**. The preview counts entries that are importable, importable with listed differences, and unsupported, then shows each monitor with its notes. Apply creates the importable monitors, paused. You can undo the whole import from the batch list. From a terminal, `node scripts/definitions.mjs preview --input uptimerobot-monitors.json` does the same.

## What moves

- **HTTP(S) monitors** become HTTP(S) monitors with the same URL and method. UptimeRobot's default method is HEAD; a QUERY method is unsupported.
- **Keyword monitors** keep their condition: alert-when-missing requires the text (`bodyIncludes`), and alert-when-present requires its absence (`bodyExcludes`). Uptime matches the text exactly, so a case-insensitive keyword is listed as a difference for either condition. An unspecified or unrecognised case setting is also named for review before enabling.
- **Expected status codes.** Exact codes, sets and inclusive ranges move without narrowing the accepted responses. For example, `200, 201, 204`, `200-299` and UptimeRobot's default `2xx, 3xx` keep every accepted status. An unrecognised code or range makes the entry unsupported; the importer never replaces it with 200.
- **Timeouts** move in seconds, within Uptime's range of 0.05 to 10 seconds. A longer timeout becomes 10 seconds, and the preview says so.
- **Intervals** move unchanged from seconds to milliseconds. An interval under 5 seconds or over one day is shown with its number and not imported. If an interval ever had to change, the preview would state it as a cadence change.
- **Heartbeat monitors** become Uptime heartbeats that expect a call within the interval plus UptimeRobot's grace period, up to one day in all. When the interval plus grace is longer than one day, such as a daily heartbeat with a grace period, Uptime keeps the interval and shortens the grace so the total is one day; the preview names the original and the shortened numbers. Their old heartbeat URL stops working for Uptime: issue a new token on the monitor's page and repoint your jobs to it. The preview lists the old URL so you know which jobs to change.

Every imported monitor checks from probe `local-a` on your own runner, and its evidence counts as fresh for at least three intervals. Uptime needs two independent vantages before it calls a target down. With one runner network, enabling a monitor takes the installation's External check URL (change it on Runner / archive), or add a second runner network or give that monitor its own external check URL in its settings.

## What does not move

The preview lists each of these on the monitor it affects. Nothing is dropped without a note.

- **Ping, port, DNS, UDP, API-assertion, visual comparison and dependency monitors** are unsupported by this importer. It maps HTTP(S), keyword and heartbeat monitors. Browser checks are outside the supported public feature set.
- **Authentication, custom request headers and POST bodies** are not imported, because they can hold secrets. The monitor is created with a local credential reference such as `uptimerobot-123456`, which you set up on your own runner before enabling it.
- **Locations and regions.** UptimeRobot's own vantage points are not reproduced. Uptime checks from your runners.
- **Alert contacts and notification settings.** Reconnect destinations in Uptime under **Notifications**. Importing sends nothing to anyone.
- **Status pages** are a different product. Lamp is the public status page product.
- **Maintenance windows.** Set maintenance on the monitor in Uptime.
- **Tags and groups**, and certificate or domain expiry reminders.
- **Ignored certificate errors and turned-off redirects.** Uptime fails a check on a certificate error and follows redirects, validating each hop. The preview notes monitors where UptimeRobot did otherwise.

## Your history stays in UptimeRobot

UptimeRobot's log export is a CSV of past events. Uptime recognises it and counts its rows in a preview, for example "120 rows, provenance: UptimeRobot log export, not stored as evidence". It does not store the file. No row becomes an Uptime check, incident or state, so old history can never confirm a failure, close an incident or change the 24-hour and 7-day figures. Your Uptime history starts with your first Uptime check.

## What has been tested

The mapping follows the field names in UptimeRobot's published v3 API description. The synthetic-file journey covers preview, paused apply, reading the saved definitions, export and undo, including status sets/ranges and both keyword conditions. Live account extraction, alert-contact mapping and status-page handoff are not implemented by this importer. It has not been tested with an export from a real UptimeRobot account. UptimeRobot does not publish its log export columns; Uptime looks for a date or time column and an event column.

## Owner-approved moves through Business

Business can drive this same saved-monitors mapping through Base's optional import door. It starts off. In **Runner / archive → Business imports**, enable **Allow owner-approved imports from Business** and save. The installation must already have its owner-claim issuer, installation ID, app origin and public signing-key ring configured. Open Runner / archive first to establish its local runner. No UptimeRobot credential is sent to Uptime.

The door accepts saved v3 pages as NDJSON (one `{data: [...], nextLink: ...}` page per line), with a stable `id` on every source monitor, up to 2 MiB and 500 source records. It uses the existing importer’s supported types, differences, credential references and validation. A name already used by a monitor outside this move is refused for review in Definitions. Imported monitors count toward the installation’s ordinary monitor limit and arrive paused. Catch-ups retain source IDs; Base refuses native edit conflicts and reports missing source records without deleting them.

The signed-grant protocol is documented in [the vendored Base door](https://github.com/runeditrun/base/blob/0ed18342a1ca240f1ae698bb50e18385b6f509ec/modules/import-kit/src/door/README.md). Each request needs a fresh short-lived ES256 grant. Upload checks every chunk and the complete bundle hash. Plan writes no native monitor or evidence; apply requires the retained plan’s exact reviewed fingerprint and signed owner approval. Reconcile reads the actual monitors and import provenance. Original bytes, plans and grants stay in this installation and are excluded from portable archives; native definitions and revisions use Uptime’s existing archive.

Business imports appear in **Definitions → Applied batches** with “Approved via Business by” the owner, the approval time and fingerprint. **Undo this batch** remains available even after Business is switched off or its signing key is revoked. Undo keeps imported monitors paused and preserves their revisions and evidence. A native edit makes strict Business undo refuse instead of overwriting it. Undo snapshots newest first; a fresh move after undo needs a new namespace.

Erasing a monitor also withdraws the indivisible source export and retires its move’s retained plans and undo authority, in the same database transaction. Other monitors from that export remain in Uptime with their own revisions and evidence.

To revoke access, switch Business imports off or expand **Grant signing keys**, select the key ID and save. Revoking a key is permanent for that ID, including after re-enabling imports; configure a new signing-key ID to grant access again. A synthetic public-door journey covers off-by-default 404, upload, zero-write native plan, fingerprint-checked apply, reconciliation, history, undo and persistent revocation. No real customer export was used.
