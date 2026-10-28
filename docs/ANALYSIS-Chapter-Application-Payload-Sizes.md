# Application-payload size analysis: problem3

Read-only analysis of the converted incident copy, 2026-10-28. Sizes use the
implemented per-chapter application-byte measurement: input plus output and
reconstruction application fields, excluding artifact bytes and storage framing.
The runtime limit has not been changed by this analysis.

There are 53 oversized chapters among 4,346 stored chapters (1.22%), spread over
50 of the database's 130 jobs. All 53 are execution `result` records.

| Application bytes per chapter | Count |
| --- | ---: |
| >256–384 KiB | 1 |
| >384–512 KiB | 27 |
| >512–768 KiB | 9 |
| >768–1,024 KiB | 0 |
| >1,024–1,536 KiB | 16 |

Minimum: 373,991 bytes (365.2 KiB). Median: 512,054 bytes (500.1 KiB).
Maximum: 1,260,816 bytes (1.20 MiB).

## Expanded recipe YAML: 50 chapters

Every affected job records a `root-source` result at ordinal 3. More than 99.9%
of its measured application bytes are the JSON string `output.data.recipe_yaml`.
These are 50 occurrences of seven distinct recipe texts:

| SHA-256 prefix | Occurrences | Raw YAML UTF-8 bytes |
| --- | ---: | ---: |
| 6bd9aae27c94 | 1 | 496,317 |
| 1ce9452f3d5e | 19 | 498,293 |
| 30d7fc60e6ca | 2 | 499,637 |
| 092e28cf5941 | 3 | 502,567 |
| e7c1866fd740 | 9 | 534,150 |
| 45e28c1829b8 | 12 | 1,215,853 |
| bd728b0abc7d | 4 | 1,241,072 |

The c2j producer in `pkg/worker/compiler/root_source.go` resolves inline recipes,
marshals the expanded recipe as YAML, and returns the complete YAML as task data.
The source audit used c2j revision f72955c2f206d93419c59f0594cab93db2e589f7.

The largest recipe has 10,855 lines, 89 inline blocks, and 793,923 bytes of leading
spaces: 64.0% of its raw size. Its largest decoded scalar value is only 2,762
bytes. The volume comes from a deeply nested expanded workflow, repeated recipe
structure, and YAML indentation. Across all seven versions, leading spaces
account for 59.5–64.6%. JSON escaping adds further bytes: the largest recipe YAML
occupies 1,260,320 bytes as a JSON string.

The originally problematic job, `3LIsi58lrjR29QBCS9dMcUbaUBK`, has only one
application-limit violation: this root-source result at ordinal 3.

## Codex results: two chapters in one job

Job `3LGCrdKR9EZxRZWsFptYaDHNpRr`, ordinals 7 and 10:

| Ordinal | Input | Output | Total |
| --- | ---: | ---: | ---: |
| 7 | 21,677 | 352,314 | 373,991 |
| 10 | 19,385 | 410,526 | 429,911 |

`output.data.payload.output` contains both `events` and `items`. Large command
outputs are copied into both arrays; exact string comparisons confirmed the
copies. The commands read skills/documentation and fetched README files across
multiple repositories. The largest individual raw command outputs were 96,204
and 104,130 bytes, each appearing in both collections.

The `events` arrays consume 180,064 and 205,096 encoded bytes; `items` consume
164,686 and 197,850. Removing just the redundant `items` collection would put
both chapters below 256 KiB, before any other changes. This is producer-side
result duplication, separate from artifact storage.

## Serialized CONTEXT input: one chapter

Job `3LIeAglwSRxG36xoys9kJOwiy19`, ordinal 159: 502,643 application bytes,
comprising 502,286 input bytes and 357 output bytes.

`input.data.input.env.CONTEXT` dominates. It contains 453,842 bytes of serialized
JSON state, occupying 500,196 bytes after being encoded as a JSON string inside
the task input. Its largest members are `threads` (16 entries, about 420,970
compact JSON bytes) and `jobs` (10 entries, about 31,181 bytes). These inner
member sizes are measured by compact reserialization; the outer contribution is
measured from the actual stored application payload.

## Implication for the default

| Proposed per-chapter limit | Chapters in this database still rejected |
| --- | ---: |
| 256 KiB | 53 |
| 512 KiB | 25 |
| 1 MiB | 16 |
| 2 MiB | 0 |

256 KiB is too restrictive for the current c2j representation of ordinary
expanded recipe snapshots. A 2 MiB default would accommodate this observed
workload with headroom. This is evidence from one database, not an upper bound
on future workloads. Moving expanded recipe source to an artifact, avoiding
redundant Codex event/item logs, and narrowing serialized execution context are
separate producer improvements; none have been implemented in this analysis.
