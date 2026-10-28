# Feature request: store expanded recipe YAML as a task artifact

Status: requested; this document does not implement the c2j change.

Change c2j's root-source resolution output to store the expanded recipe YAML in
a durable JobDB artifact. New results should omit the inline `recipe_yaml`
field. Readers must prefer the artifact and fall back to the existing
`recipe_yaml` field when the artifact is absent, so recorded jobs remain
replayable without rewriting their application payloads.

## Problem and evidence

`pkg/worker/compiler/root_source.go` currently calls `ResolveInlineRecipes`,
marshals `expanded.Recipe` as YAML, and returns it in
`ResolvedRecipeSource.RecipeYAML` through `jobdb.NewTaskData(source)`.
`root-source` therefore records the entire expanded source as application data.

In the incident database, 50 of 53 chapters above JobDB's 256 KiB application
budget are root-source results. Their `recipe_yaml` fields account for more than
99.9% of their application bytes. There are seven distinct expanded recipes
repeated across 50 jobs; raw YAML ranges from 496,317 to 1,241,072 bytes. The
largest becomes a 1,260,320-byte JSON string. Much of the volume is indentation
in deeply expanded recipe structures, which still needs to be durably available
for deterministic replay.

See the local [size analysis](ANALYSIS-Chapter-Application-Payload-Sizes.md).
These application values are preserved by the JobDB migration tool; removing
inline artifact bodies does not remove or relocate `recipe_yaml`.

## Requested producer behavior

1. Continue resolving and expanding recipes exactly as today. Capture the exact
   resulting YAML bytes as a task-output artifact through the normal JobDB
   artifact API, using a stable reserved logical name such as
   `_c2j/resolved-recipe.yaml`.
2. Keep resolution metadata in the task's JSON value: source kind, submitted and
   resolved selectors, commit, and pinning information.
3. Add a small artifact-name locator, proposed as
   `recipe_yaml_artifact: "_c2j/resolved-recipe.yaml"`. Resolve it against the
   task output's artifact collection. JobDB supplies the durable storage key;
   c2j must not predict a storage ordinal or construct a parallel blob store.
4. Omit `recipe_yaml` from newly recorded results. Do not retain both the complete
   inline value and artifact, including inside error diagnostics or metadata.
5. Apply this consistently to `root-source`, `root-source-load`, and any other
   producers of `ResolvedRecipeSource`. Preserve unrelated output artifacts.

Keep the existing `RecipeSourceResolution.ArtifactName` meaning intact: it can
identify the originally submitted recipe artifact and is not necessarily the
expanded, resolved recipe snapshot.

## Reader compatibility and precedence

Use one artifact-aware loading path wherever resolved recipe source is consumed:

- Find the expanded-source artifact using the recorded locator or reserved
  logical name. Read its bytes through the task artifact reader, then parse the
  recipe using the existing parser. Artifact content takes precedence if both
  representations exist.
- If the artifact is absent from the result, load the legacy `recipe_yaml` field
  when it is nonempty. Preserve this read fallback for existing histories;
  existing inline-only records require no application-data migration.
- If neither representation exists, preserve the existing explicitly supported
  legacy source-resolution behavior. The older `root-source-load` recovery task
  must also accept artifact-backed output. Read-only replay must not introduce
  network resolution or execute a new task to compensate for missing history.
- Distinguish an absent artifact from a broken advertised reference. An artifact
  read, authorization, or parse error must remain visible rather than silently
  selecting potentially different inline source. An empty or malformed artifact
  must not be treated as a valid recipe.

`ParseResolvedRecipeSourceTaskData` currently reads only `GetData()`. It needs
access to the output artifacts as well. `ParseResolvedRecipeSourceJSON` alone
cannot resolve an artifact; callers needing executable source must retain the
TaskData/artifact context. A context-aware loader can preserve JSON parsing for
metadata-only consumers without embedding artifact bytes in a serializable
`ResolvedRecipeSource` value.

The existing job-worker branch that chooses `LoadRecipe()` only when
`RecipeYAML` is nonempty must recognize artifact-backed source before deciding
to schedule `root-source-load`. Otherwise every new result could spuriously
trigger source resolution again.

## Replay, inspection, and ownership

JobDB continues to own artifact persistence, durable references, lifecycle, and
lazy reconstruction. c2j owns the interpretation of the resolved-recipe artifact
and the legacy application-field fallback.

Audit live execution, resume/replay, cached results, restart/continuation,
recipe story construction, and source-display paths. Preserve selectors, pinned
commits, inline expansion provenance, and recipe semantics. Stored source must
remain sufficient when the original repository is unavailable or its branch has
moved. Use the referenced artifact's actual key, including inherited artifacts
from earlier jobs or chapters.

Avoid changing task sites, logical identities, or command descriptors solely for
this representation change. Verify old history replay against the updated
worker rather than requiring recipes to rerun source-resolution tasks.

A story/detail response may still materialize YAML for an explicitly requested
source view if its API requires that field. Do not write that materialized value
back into execution history, or fetch every recipe artifact during ordinary job
listing. Metadata-only views should use artifact descriptors without opening
content.

This application-level fallback is separate from the JobDB hard storage/protocol
cutover. c2j still needs the compatible JobDB dependency and the local
[reference-only artifact migration patch](C2J-Reference-Only-Artifacts.patch),
described in the [companion guide](MIGRATION-C2J-Reference-Only-Chapter-Artifacts.md).
That patch fixes artifact size inspection; it does not implement this request.

## Tests and acceptance criteria

- A new root-source result has the full expanded YAML in one artifact and omits
  inline `recipe_yaml`. Verify byte-for-byte content and preserved source metadata.
- A large expanded recipe (at least the observed 1.24 MB raw YAML) records and
  replays with JobDB's default 256 KiB application limit. Assert the measured
  chapter application payload is small and independent of YAML size.
- Old inline-only results still load and replay without fetching repository
  contents. Cover migrated old histories as well as direct TaskData fixtures.
- When both forms are present and differ, the artifact wins. When the artifact
  entry is absent, the inline field works. Cover empty/malformed artifact content,
  failed reads, and neither representation being present.
- Test `root-source-load` output and ensure artifact-backed `root-source` does
  not unnecessarily invoke that recovery task.
- Test stored/lazy artifacts through SQLite and remote JobDB, including logical
  names that differ from stored key names, inherited keys, and worker restart.
- Exercise story/source views and cached output. Metadata-only enumeration must
  not open artifact content; actual recipe loading must return identical recipe
  semantics for the artifact and legacy inline forms.
- Test retry or interrupted-write recovery using JobDB's normal artifact/chapter
  persistence path; no successful source result may reference an uncommitted
  artifact.

New writers are artifact-only; updated readers support both application
representations. Deploy compatible readers/workers before allowing new writers
where old c2j processes might otherwise consume their results. This request
requires no new JobDB schema migration and does not change the JobDB payload
limit.
