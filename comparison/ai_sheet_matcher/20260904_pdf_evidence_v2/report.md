# PDF Evidence V2 — the deterministic schematic topology graph

Research only.  No model calls, no deploy, no shadow, no materialization, no production module changed.

Verdict: **B**.

## What the graph is

A deterministic graph of what the sheet *draws*.  A node is a wire, a bus, a device, a terminal, a junction, a label or a table port.  An edge is a relation with the drawn fact that proves it, or it carries `NO_CLAIM` and proves nothing.

Two rules decide everything downstream.  **An intersection is not a connection** — a connection exists where the drawing says so, and where the drawing says the opposite, the crossover hop, that is recorded as proof of non-connection.  **A direction needs an arrow** — the contract admits exactly one kind of evidence for it.

## The graph, per document

| Document | Pages | Nodes | Edges | Proven | No claim | Bus | Feeder | Equipment | Junction dots | Crossings refused | Hops | Labels bound | Islands |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| IOS1.1/LEFT | 60 | 2647 | 3097 | 2988 | 109 | 31 | 722 | 426 | 256 | 3147 | 2001 | 146 | 19 |
| IOS1.1/RIGHT | 48 | 24864 | 28506 | 27968 | 538 | 108 | 6439 | 3086 | 1502 | 5060 | 235 | 1263 | 149 |
| IOS2.1/LEFT | 52 | 53 | 53 | 53 | 0 | 0 | 22 | 2 | 15 | 479 | 6 | 0 | 9 |
| IOS2.1/RIGHT | 63 | 1608 | 1767 | 1760 | 7 | 3 | 262 | 152 | 16 | 1332 | 45 | 73 | 3 |
| IOS3.1/LEFT | 26 | 21 | 33 | 33 | 0 | 0 | 4 | 11 | 1 | 798 | 22 | 0 | 1 |
| IOS3.1/RIGHT | 29 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1800 | 22 | 0 | 0 |

Across the corpus: **29193** nodes, **33456** edges, of which **32802** assert a connection and **654** assert nothing.  **49** carry a proven direction.

## Which sheets these are

Pages are not selected; every page of every document is processed and then profiled from what it turned out to hold.

| Profile | Pages | Welded edges | Proven conductors | Nodes | Proven edges | Bus nodes | Labels | Labels bound |
|---|---|---|---|---|---|---|---|---|
| NO_VECTOR_GEOMETRY | 120 | 0 | 0 | 0 | 0 | 0 | 4621 | 0 |
| SPARSE_GEOMETRY | 78 | 976 | 0 | 0 | 0 | 0 | 10233 | 0 |
| TABLE_SHEET | 46 | 37314 | 195 | 421 | 445 | 0 | 19817 | 15 |
| DRAWING_WITHOUT_A_BUS | 9 | 3478 | 0 | 0 | 0 | 0 | 3804 | 0 |
| SINGLE_LINE_SCHEME | 10 | 3137 | 1489 | 4227 | 5008 | 47 | 1799 | 311 |
| SCHEME_WITH_TABLES | 15 | 21353 | 7888 | 24545 | 27349 | 95 | 8138 | 1156 |

## Negative controls

A control that can only pass is still measured, because a construction can be changed by a later edit and a number notices.

| Control | Observation |
|---|---|
| A — crossings seen | 15002 |
| A — refused as not a connection | 12616 |
| A — refused by a drawn hop | 2331 |
| A — joined by a drawn dot | 55 |
| A — refused crossings whose edges share a run anyway | 0 |
| B — table lattice edges | 15826 |
| B — of those, conducting | 0 |
| C — frame edges | 577 |
| C — of those, conducting | 0 |
| C — bus nodes spanning the sheet | 0 |
| D — rules drawn under a word | 1642 |
| D — of those, conducting | 0 |
| E — labels with a conductor within five ems | 6984 |
| E — labels bound by a drawn relation | 1482 |
| E — labels attributed by proximity | 0 |
| F — islands | 181 |
| F — islands carrying a bus | 55 |
| F — proven edges between two islands | 0 |
| G — signatures used more than once | 772 |
| G — nodes sharing a signature | 10756 |
| G — distinct nodes behind them | 10756 |

## Validation

| Guard | Violations |
|---|---|
| A_LABEL_BINDS_TO_ONE_NODE | 0 |
| CLOSED_VOCABULARIES | 0 |
| CONNECTION_REQUIRES_DRAWN_EVIDENCE | 0 |
| DIRECTION_REQUIRES_AN_ARROWHEAD | 0 |
| NO_EDGE_SPANS_TWO_PAGES | 0 |

Replay: 51 of 51 representative pages rebuilt from the PDF byte-identically (SHA-256 over the node and edge tables).

| Structural check | Count |
|---|---|
| edges | 33456 |
| edges_claiming_without_evidence | 0 |
| edges_naming_a_node_outside_the_graph | 0 |
| edges_spanning_two_pages | 0 |
| labels_bound_to_two_nodes | 0 |
| no_claim_edges | 654 |
| nodes_without_an_island | 0 |
| proven_edges | 32802 |

## Naming a node

V1's rule for ownership, unchanged, aimed at a run instead of a region.

| Outcome | Strings |
|---|---|
| AMBIGUOUS | 226 |
| BOUND | 1482 |
| UNBOUND | 46704 |

| Channel | Strings |
|---|---|
| INSIDE_SINGLE_SYMBOL_BOX | 37 |
| RUNS_ALONG_SINGLE_CONDUCTOR | 1445 |

|  | Strings |
|---|---|
| both | 1224 |
| only_v1 | 13837 |
| only_v2 | 258 |
| v1_fragment_local | 15061 |
| v2_bound | 1482 |

offset_em is the perpendicular distance from the string to the conductor that took it, in ems of the string's own size; 0.0 means the conductor passes inside the string's own box

## Symbols, discovered

|  | Count |
|---|---|
| signatures | 913 |
| used more than once | 408 |
| occurrences | 11593 |
| seen on more than one document | 37 |

| Signature | Occurrences | Distinct nodes | Pages | Names the sheet gave |
|---|---|---|---|---|
| sym_6e6d2ee5d746fcfb | 2398 | 2388 | 22 | — |
| sym_f55c04f193e1fa10 | 1214 | 1208 | 27 | — |
| sym_0794f02a788a4316 | 837 | 833 | 22 | 4 |
| sym_5602193f1c36a82e | 506 | 506 | 17 | — |
| sym_2b0e93eeacc44734 | 489 | 489 | 14 | — |
| sym_69f6918aefcec195 | 448 | 448 | 17 | — |
| sym_ca2d073bd26a54dc | 346 | 346 | 18 | — |
| sym_e95f5bd74ca491d6 | 345 | 345 | 1 | — |
| sym_4df7516b93e707c8 | 343 | 343 | 1 | — |
| sym_9d7487f832245ee6 | 218 | 218 | 7 | — |
| sym_15eac6f340f86d96 | 190 | 190 | 13 | — |
| sym_af152faacc3a8c36 | 172 | 172 | 27 | — |

a signature says two clusters were drawn from one block and says nothing about what the block means; a name arrives only from a label bound by a drawn relation

## Direction

|  | Count |
|---|---|
| arrowheads found | 13 |
| edges given a proven direction | 49 |
| nodes a keyword rule would have directed | 22 |
| of those carrying a counter-name on the same conductor | 19 |
| edges directed from a keyword | 0 |

The trap, in the drawing's own words — one conductor, two names:

| Sheet | Direction word | The same wire's own line number |
|---|---|---|
| IOS1.1/LEFT p.26 | Резервный ввод М2-ППГнг(А)-FRHF 5х4; L=10м | ARTМ-03 (D)PBR.R; |
| IOS1.1/RIGHT p.21 | ВРУ1 ввод 1 - Корпус 1,2 | ГРЩ1-РП1-1 3хППГнг(А)-HF 5х150мм² |
| IOS1.1/RIGHT p.21 | ВРУ1 - ввод 2 Корпус 1,2 | ГРЩ1-РП2-1 3хППГнг(А)-HF 5х150мм² |
| IOS1.1/RIGHT p.21 | ВРУ2 ввод 1  - Встроенные помещения корпуса 1,2 | ГРЩ1-РП1-2 ППГнг(А)-HF 5х120мм² |
| IOS1.1/RIGHT p.21 | ВРУ2 - ввод 2 Встроенные помещения корпуса 1,2 | ГРЩ1-РП2-2 ППГнг(А)-HF 5х120мм² |
| IOS1.1/RIGHT p.21 | ВРУ3 ввод 1  - Корпус 3 | ГРЩ1-РП1-3 2хППГнг(А)-HF 5х150мм² |

only an arrowhead proves a direction; a direction word names the far end of the wire and would invert every outgoing feeder

## The control sheet, walked

`IOS1.1/RIGHT` page 21 — the ГРЩ single-line diagram, 582 printed strings against 34 in the recognized Markdown.  Measured by the same code as every other page.

|  | Count |
|---|---|
| welded edges | 1907 |
| proven conductors | 649 |
| junction dots joining conductors | 110 |
| crossings refused | 265 |
| of those, refused by a drawn hop | 81 |
| series gaps a device fills | 137 |
| strings bound by a drawn relation | 73 |
| strings recorded by alignment only | 43 |
| feeders named by their cable mark | 30 |
| of those reaching a bus | 30 |

| Nodes | Count |
|---|---|
| BUS | 7 |
| CONNECTOR | 458 |
| EQUIPMENT | 172 |
| FEEDER | 471 |
| LABEL_ANCHOR | 116 |
| TERMINAL | 529 |

One feeder, end to end — `ГРЩ1-РП1-15 ППГнг(А)-HF 5х185мм²`:

  FEEDER → EQUIPMENT → FEEDER → CONNECTOR → BUS

Also bound to that same conductor: `180.0кВАр180.0кВАр272.7А`.

Recorded beside it by alignment and claiming nothing: `АУКРМ №1`.

## Function Passport — read-only

Three regimes on the same read of the same pages: V1's ownership rule, this package's binding to a proven node, and what alignment would additionally reach — the last one recorded and never claimed.

| Regime | Values with a fragment-local home |
|---|---|
| V1_REGION | 737 |
| V2_TOPOLOGY | 58 |
| V2_WITH_ALIGNMENT | 72 |

| Field | Documented | V1_REGION | V2_TOPOLOGY | V2_WITH_ALIGNMENT |
|---|---|---|---|---|
| building | 103 | 6 | 0 | 0 |
| consumers | 746 | 0 | 0 | 0 |
| corpus | 103 | 6 | 0 | 0 |
| cross_sheet_functional_references | 16 | 7 | 0 | 0 |
| downstream | 1945 | 4 | 0 | 0 |
| equipment_roles | 1644 | 56 | 18 | 18 |
| floors | 708 | 283 | 12 | 12 |
| section | 62 | 2 | 0 | 0 |
| serviced_object | 165 | 8 | 0 | 0 |
| stable_entities | 2957 | 216 | 18 | 23 |
| systems | 3212 | 125 | 10 | 19 |
| upstream | 1074 | 1 | 0 | 0 |
| zone | 151 | 23 | 0 | 0 |

## What only a graph can say

`upstream` and `downstream` are printed literally once in 1 074 and sixteen times in 1 945.  No extraction rescues them because they are not printed.  A graph does not need them printed — it needs the wire drawn.

|  | Count |
|---|---|
| functions in the frozen inventory | 313 |
| joined to a node set by their own printed mark | 13 |
| of those, joined to exactly one node | 4 |
| of those, joined to several | 9 |
| with a proven neighbour outside themselves | 13 |
| whose own wires include a bus | 0 |
| with a device in series | 13 |
| reaching a bus | 9 |

a passport function is a switchboard and a node is one of its wires; the join is a set, and the board's own mark is what makes it

## Identity from structure

| Node kind | Nodes | Distinct signatures | Largest group | Singletons |
|---|---|---|---|---|
| BUS | 142 | 140 | 27 | 138 |
| CONNECTOR | 7797 | 2428 | 469 | 999 |
| EQUIPMENT | 3677 | 2706 | 149 | 2257 |
| FEEDER | 7449 | 2806 | 1777 | 2095 |
| JUNCTION | 76 | 58 | 17 | 46 |
| TERMINAL | 7916 | 2847 | 689 | 1924 |

Convergence candidates (three or more runs meeting at one node): 288.  Series pairs: 11262.  A convergence is a shape, not a merge; the track's own rule refuses a shared target as proof of one.

## Function Lineage — read-only

|  | Count |
|---|---|
| functions | 313 |
| whose page binds any mark to a node | 85 |
| whose primary mark is bound to a node | 13 |

| Tier | before | after |
|---|---|---|
| AUTO_MERGED_CERTIFIED | 0 | 0 |
| AUTO_ONE_TO_ONE_CERTIFIED | 0 | 0 |

## Merge, split, distribution — read-only

| Relation | Tasks | Left functions | on a node | Right functions | on a node | Every side on a node |
|---|---|---|---|---|---|---|
| CONTINUED_1_TO_1 | 40 | 40 | 0 | 216 | 5 | 0 |
| CONTINUED_1_TO_1+SPLIT_1_TO_N | 99 | 99 | 0 | 949 | 66 | 0 |
| MERGED_N_TO_1 | 67 | 134 | 0 | 125 | 8 | 0 |

a shared target does not prove a merge; a convergence is a shape on one sheet and a merge is a claim about two

## Storage

| Stage | Coordinate floats | Bytes | Verdict |
|---|---|---|---|
| research raw | 26951824 | 215614592 | never persisted; read, measured, released |
| normalized | 4477064 | 35816512 | research artifact only |
| topology graph | 29193 nodes / 33456 edges | 2752941 | the production shape: identifiers, kinds, claims, one evidence reference |

## Verdict

**B**.  32802 proven edges over 278 pages, 25 of which are schematics.  Leaks across all controls: 0.  Producer guards clean: True.  Replay byte-identical: True.

No deploy.  No shadow.  No materialization.  Model calls: 0.
