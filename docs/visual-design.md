# GossipGrid visual design system

This document defines the **visual conventions** for all current and future GossipGrid documentation,
diagrams, and the eventual native visualization features (Milestone 6B). It is the single reference for
color, typography, shape, line-style, and layout choices. The purpose is **educational clarity**: every
diagram should be readable on its first pass, communicate its architectural meaning without decorative
noise, and remain understandable in grayscale.

This is a design document, not an implementation. It does not change application behavior, does not
implement web UI styling, and does not modify the pinned Mermaid CLI version. The Mermaid rendering
configuration that operationalizes these conventions for diagram rendering is
[`docs/diagrams/mermaid-config.json`](diagrams/mermaid-config.json).

## Design principles

1. **Meaning over decoration.** Every visual element (color, shape, line style) must carry an
   architectural or pedagogical meaning. Remove elements that do not.
2. **Restrained and professional.** A modern, low-chroma aesthetic. Subtle tints, clear borders, generous
   spacing. No gradients, shadows, or decorative icons.
3. **Color is one signal among many.** Never rely on color alone to communicate meaning. Pair color with
   labels, shapes, line styles, and annotations. The diagram must remain legible in grayscale.
4. **Consistency beats novelty.** One palette, one set of shapes, one set of conventions across all
   diagrams. A reader who has seen one GossipGrid diagram should be able to read the rest without a new
   legend.
5. **Focus one concept per diagram.** A focused diagram that explains one important idea well is
   preferable to a large diagram that attempts to explain everything. Split complex topics into focused
   companion diagrams and link them.

## Semantic color palette

The following six colors are the **preferred semantic accent colors**. They are *accents*, not mandatory
text colors: derive accessible text, border, and background variants as needed. Favor lightly tinted
backgrounds, meaningful borders, and highly readable text.

| Semantic meaning | Accent | Light tint (fill) | Accessible text (on tint) |
| --- | --- | --- | --- |
| Healthy, ready, authorized, successful | `#16a34a` (green) | `#f0fdf4` | `#166534` |
| Communication, replication, data transfer | `#3b82f6` (blue) | `#eff6ff` | `#1e40af` |
| Transition, synchronization, degraded, waiting | `#d97706` (amber) | `#fffbeb` | `#92400e` |
| Failure, rejection, revocation, fencing | `#dc2626` (red) | `#fef2f2` | `#991b1b` |
| Authority, identity, security, trust boundaries | `#8b5cf6` (violet) | `#f5f3ff` | `#5b21b6` |
| Infrastructure, storage, neutral components | `#64748b` (slate) | `#f8fafc` | `#334155` |

### Derivation rules

- **Accent** is used for borders, key labels, and emphasis — not as a large fill.
- **Light tint** is the default node background for nodes in that semantic category.
- **Accessible text** is the text color placed on a light-tint background. It is chosen to meet WCAG AA
  contrast (≥ 4.5:1) on the tint.
- For a **neutral** node (no strong semantic category), use the slate light tint `#f8fafc` with
  `#334155` text and a `#cbd5e1` border.
- For the **diagram canvas** (the overall background), use an opaque light `#ffffff` (white). This keeps
  readability reliable across GitHub's native Mermaid renderer, committed SVG/PNG, and the Mermaid CLI.
  Do not claim dark-mode compatibility; the light canvas is the supported baseline.

### Grayscale rule

Every semantic distinction must survive a grayscale conversion. Verify that:

- Shape (rounded vs. database cylinder vs. decision diamond) still distinguishes component types.
- Line style (solid vs. dotted vs. dashed) still distinguishes control-plane vs. data-plane and
  observed vs. inferred relationships.
- Labels and annotations still carry the meaning that color alone would have.

If a diagram relies on "green vs. red" to convey "ready vs. failed" with no other cue, it fails the
grayscale rule. Add the state label, shape, or annotation.

## Typography

- **Font family:** The system UI stack — `-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
  Helvetica, Arial, sans-serif`. This is set centrally in
  [`mermaid-config.json`](diagrams/mermaid-config.json) and is not overridden per diagram.
- **Base font size:** `14px` for node and edge labels (set centrally). Subgraph titles may be one step
  larger; legends and captions may be one step smaller.
- **Hierarchy:**
  - Diagram title (if present): largest, bold, `#0f172a`.
  - Subgraph boundary label: medium, semibold, the subgraph's semantic accent.
  - Node label: base size, regular weight, the node's accessible-text color.
  - Edge label: base size or one step smaller, in `#475569` (slate-600) on a white edge-label background.
- **Label length:** Keep node labels short. A label longer than roughly 40 characters should be split
  into a short primary label plus a secondary line, or moved to a note/annotation. Long labels cause
  node stretching and clipped text.
- **No all-caps body text.** Use title case or sentence case for labels. All-caps is reserved for
  state-machine state names (e.g. `READY`, `SYNCING`), which are already identifiers.

## Component shapes

| Component | Shape | Semantic accent |
| --- | --- | --- |
| Generic replica / application / process | Rounded rectangle (`("...")`) | slate (neutral) or the replica's lifecycle state |
| Replica in a specific lifecycle state | Stadium (`(["..."])`) or rounded rectangle, tinted by state | see "Replica and lifecycle state" below |
| Database / persistent store | Cylinder (`[("...")]`) | slate (infrastructure) |
| Ephemeral store (pod-lifetime `emptyDir`) | Cylinder, annotated `emptyDir` | slate, with a "pod-lifetime" annotation |
| Decision / admission check | Diamond (`{"..."}`) | the semantic category of the decision (e.g. violet for authority) |
| External system (Vault, GitLab CI) | Rounded rectangle, in an "External" subgraph | slate |
| Kubernetes control-plane resource (ConfigMap, Secret, ServiceAccount) | Rounded rectangle, in a "Kubernetes" subgraph | violet (authority/trust) or slate |
| Network boundary / trust boundary | Subgraph with a colored border | violet for trust boundaries |
| Lifecycle-state component | Native stadium shape (`(["..."])`) | tinted by the state's semantic category |

**Corner radii (generated assets):** ordinary component (rounded-rectangle) corners use 8 px;
organizational container (subgraph) corners use 10 px; sequence participant boxes use 8 px; and
sequence notes use 6 px in Chromium (see "Shape-aware semantic styling and native shapes" —
Chromium support is documented, not claimed universal across every SVG viewer).

## Replica and lifecycle-state representation

A replica is a rounded rectangle tinted by its **lifecycle state**. This is the single most important
semantic mapping in GossipGrid diagrams.

| State | Semantic category | Accent / tint |
| --- | --- | --- |
| `STARTING`, `DISCOVERING`, `GENESIS`, `SYNCING` | transition / waiting | amber `#d97706` / `#fffbeb` |
| `READY` | healthy / ready / authorized | green `#16a34a` / `#f0fdf4` |
| `DEGRADED` | degraded (still ready) | amber `#d97706` / `#fffbeb` |
| `RECOVERY_REQUIRED` | failure / lockout | red `#dc2626` / `#fef2f2` |

- A replica that is **not** in a lifecycle state (lifecycle disabled) is shown as a neutral slate
  rounded rectangle.
- A replica that is an **authoritative bootstrap source** (READY or DEGRADED, matching epoch) may be
  emphasized with a green border in addition to its state tint.
- The state name is always written inside or beside the node (e.g. `gossipgrid-2 · SYNCING`), so the
  meaning is not color-only.

## Storage representation

- **Persistent (in this context: durable local binding, ConfigMap metadata):** cylinder or a
  Kubernetes-resource shape. The `local_metadata` durable binding and the ConfigMap metadata are
  "durable within their scope" and are drawn as cylinders (for SQLite) or Kubernetes resources (for
  ConfigMap).
- **Ephemeral (pod-lifetime `emptyDir` SQLite):** cylinder with an explicit `emptyDir (pod-lifetime)`
  annotation. This is the single most important storage distinction in the recovery diagrams: a rollback
  or pod replacement **loses** the `emptyDir` contents but does **not** lose the ConfigMap metadata.
  Always label the ephemeral store so the reader understands what a rollback does and does not restore.
- **Metadata-only records (ConfigMap):** never store message bodies. Label them "metadata only".

## Trust and authorization boundaries

- A **trust boundary** is a subgraph whose border is violet `#8b5cf6`. Examples: the workload-PKI trust
  domain, the browser-auth credential boundary, the lifecycle-metadata authority boundary.
- A **component inside a trust boundary** is connected to the boundary's owner with a violet edge.
- **mTLS** is a transport-layer trust mechanism: draw it as a violet dotted edge (see
  "Communication arrows") with a `mTLS` label. mTLS proves *identity*, not *epoch membership* — annotate
  this distinction when epoch fencing is in scope.
- **Authorization decisions** (admission checks, `CheckBootstrapFetch`, `CheckReplicationOutbound`) are
  violet diamonds. A rejection is a red edge from the diamond to a "rejected / blocked" node.

## Communication arrows

| Relationship | Line style | Color | Notes |
| --- | --- | --- | --- |
| Control plane (metadata read, RBAC, operator action) | solid | violet `#8b5cf6` | ConfigMap watch, RBAC binding, operator Job |
| Data plane (message, sync, inventory fetch) | solid | blue `#3b82f6` | gossip fanout, anti-entropy, bootstrap baseline |
| mTLS transport | dotted | violet `#8b5cf6` | peer mTLS, browser HTTPS-to-workload |
| Observed relationship (measured, verified) | solid | the plane color | a verified edge |
| Inferred / potential relationship | dashed | the plane color, lighter | a relationship that is designed but not yet wired |
| Planned / not implemented | dashed, labeled `planned` | slate `#64748b` | Stages 3C–3E, M5C, M6B, M9 features |
| Asynchronous event | dotted, arrow head | blue `#3b82f6` | callback re-enqueue, poll detection |
| Rejection / failure path | solid, red | red `#dc2626` | 409, 403, 503, permanent rejection |
| Success path | solid, green | green `#16a34a` | 200, authorized, committed |

- **Edge labels** are short, on a white background (`edgeLabelBackground: #ffffff`), in `#475569`
  text. A label longer than ~24 characters should be split or moved to a note.
- **Direction** is always explicit: use arrow heads. A `---` (undirected) edge is reserved for
  "belongs to / part of" relationships (e.g. a node inside a subgraph).
- **Self-loops** (e.g. a scheduler re-firing) are drawn as a dotted self-edge with a label.

## Line thickness and visual clarity

The generated assets (the committed SVG/PNG) use **thicker, clearer lines** than Mermaid's default
1 px, so that edges and boundaries read without zooming. The conventions are applied through the
centralized renderer configuration: a `themeCSS` block in
[`mermaid-config.json`](diagrams/mermaid-config.json) that targets the actual generated SVG elements
(`.edgePaths path.flowchart-link` for flowchart edges, `g.node.<semantic> > rect.label-container` /
`path.outer-path` / `polygon.label-container` for node borders, `g.cluster > rect` for subgraph
borders, `rect.actor` / `rect.note` / `line.actor-line` for sequence participants and notes,
`line.messageLine0` / `line.messageLine1` for sequence messages, and `.edgePaths path.transition`
for state transitions). The per-diagram sources retain only the semantic `class` names (e.g.
`class X guard`) — no `classDef` color declarations — so the same canonical source renders
theme-neutrally inline and semantically in the generated assets (see "Rendering hosts"):

- **Component borders and flowchart edges:** 1.8 px. Every semantic node (regardless of whether
  its visible shape is a rounded rectangle, a database cylinder, or a decision polygon) and
  every flowchart edge uses 1.8 px.
- **Organizational containers (subgraphs):** 1.6 px. An ordinary organizational container — a
  grouping that collects related components (a stage, a plane, a sub-system) — uses the
  restrained 1.6 px border.
- **Genuinely important trust boundaries:** may warrant stronger emphasis. A trust boundary that
  is a real security or authority partition (the workload-PKI trust domain, a credential
  boundary, the lifecycle-metadata authority boundary) is a subgraph whose border is violet
  (`#8b5cf6`) and may be drawn at a heavier weight than the 1.6 px ordinary-container weight
  to signal that it is a *boundary*, not merely a group. This distinction — 1.6 px ordinary
  container vs. a stronger, violet trust boundary — is what keeps "grouping" from looking the
  same as "trust partition" in grayscale.
- **Sequence participants and notes:** participant boxes use an 8 px corner radius and a 1.6 px
  border; notes use a 6 px corner radius (Chromium, see "Shape-aware semantic styling") and a
  1.4 px border; lifelines are 1.1 px dashed.
- **Sequence messages:** solid messages are 1.8 px; return messages are 1.25 px.
- **Clearly visible arrowheads:** arrowheads are rendered at the edge's line weight so they are
  visible but not heavy.
- **Readable dashed and dotted relationships:** the dashed (inferred / planned) and dotted (mTLS /
  asynchronous) line styles keep the same weight as the corresponding solid edge, so the dash pattern
  is what distinguishes them, not a thin line.
- **Subtle fills and strong text contrast:** node fills are lightly tinted; the label text is a
  high-contrast color on the tint so labels stay readable (the palette's "accessible text" column).

The **semantic color** rules are **not** applied to the native inline (GitHub) rendering: GitHub's
renderer uses its own theme and never sees the local `themeCSS` block or the semantic `class` names
in a way that forces the generated-asset palette. The inline diagram remains legible through its
labels, shapes, and line *styles* (solid / dashed / dotted) rather than through a fixed tint. Do not
add per-diagram `classDef` color overrides expecting them to change the inline appearance — they
would pin the inline rendering to one theme.

**Theme-neutral native line weight.** Unlike the fixed-color rules, the *line-weight* convention
**is** applied natively to the embedded flowcharts. Every canonical flowchart source carries a
theme-neutral declaration:

    linkStyle default stroke-width:1.8px

This sets only the stroke *width* of every flowchart edge (1.8 px) and imposes **no** fixed color,
so it is safe under the host's adaptive light/dark theme. It is supported by the pinned renderer
(mmdc v11.4.1, verified: the generated edge paths carry the 1.8 px width, and dashed / dotted edges
keep their `stroke-dasharray` pattern) and does not interfere with invisible layout edges
(`~~~`), dashed relationships, or intentionally emphasized connections. The declaration changes
line thickness only; it does not change colors. Native state diagrams and sequence diagrams do not
support the same `linkStyle` directive, and this convention is therefore not forced onto them —
their line weights and participant / note corners depend on the hosting renderer and remain
documented as a limitation, not a fixed value.

## Native shape syntax (correct)

Native Mermaid flowchart node shapes each have distinct syntax and distinct rendered geometry.
Do not conflate them:

| Shape | Native syntax | Example | Meaning |
| --- | --- | --- | --- |
| Square rectangle | `["label"]` | `A["Component"]` | A plain box with square corners (renders as a `rect`) |
| Rounded rectangle | `("label")` | `A("Component")` | A box with rounded corners (renders as a `rect`) |
| Stadium | `(["label"])` | `A(["READY"])` | A fully rounded, pill-shaped box (renders as a nested `path`) |
| Database cylinder | `[("label")]` | `A[("SQLite")]` | A cylinder (renders as a `path`) |
| Decision diamond | `{"label"}` | `A{"Authorized?"}` | A decision (renders as a `polygon`) |

A common mistake is to write `["Component"]` (square) and call it a rounded rectangle. The
**native rounded-rectangle syntax is `("Component")`**. GossipGrid canonical flowchart sources
use `("...")` rounded rectangles for ordinary application components, services, process steps, and
infrastructure components; `(["..."])` stadiums for lifecycle-state components; and preserve
`[("...")]` cylinders and `{"..."}` diamonds where the shape is intentional (storage, decisions).
Subgraph titles (`subgraph X["..."]`), edge labels, and the invisible layout edge `~~~` have
different syntax and are **not** converted.

**Generated-asset shape-aware selectors.** The semantic color groups are applied to the **actual
visible component shapes**, and the `themeCSS` block targets each of the four distinct shapes so
every semantic node receives a consistent fill, border color, text color, and 1.8 px border width.
The four selectors (verified against the actual SVG structure produced by mmdc v11.4.1):

- `g.node.<class> > rect.label-container` — a rounded rectangle or a square rectangle
  (Mermaid `("label")` / `["label"]`); both draw their visible body as a `rect`.
- `g.node.<class> > path.outer-path` — a database cylinder (Mermaid `[("label")]`); the visible
  body is a **direct** `path` child of the node group, carrying the `outer-path` class.
- `g.node.<class> > g.outer-path > path` — a stadium (Mermaid `(["label"])`); the visible body is
  a `path` **nested inside** a `g` wrapper that carries the `outer-path` class (the stadium `g`
  is `class="basic label-container outer-path"` containing a `path`). This nested structure is
  why the old `g.node.<class> > path.outer-path` selector did not match the visible stadium path.
- `g.node.<class> > polygon.label-container` — a decision diamond (Mermaid `{"label"}`).

The six semantic groups (green, blue, amber, red, purple, slate) each map to a fixed set of
classes (see the "Semantic color palette" table and the `themeCSS` in
[`mermaid-config.json`](diagrams/mermaid-config.json)); **planned** components keep their separate
neutral dashed-border treatment and are not part of the six groups. A neutral 1.8 px border
fallback (slate `#cbd5e1`) is applied to visible cylinder paths (`path.outer-path`), nested
stadium paths (`g.outer-path > path`), and decision polygons that carry no semantic class, so an
unstyled shape still reads with the same border weight. Inaccessible label rectangles and
unrelated nested elements are **not** styled.

**Native adaptive inline rendering vs. fixed generated-asset presentation.** The same canonical
source renders two ways. Inline (GitHub) uses the host's adaptive theme: the **native shape
syntax** (rounded rectangle, stadium, cylinder, diamond) and the theme-neutral `linkStyle` line
weight carry the meaning, with no fixed tint. Generated assets (committed SVG/PNG) use the fixed
light theme plus the `themeCSS` block: the semantic colors, the shape-aware selectors above, the
8 px / 10 px corner radii, the 1.8 px component borders, and the sequence note 6 px radius are all
applied there. The two must remain independently legible (see "Rendering hosts"). The general
generated-asset 8 px rectangle styling is applied to `rect.label-container` only, so it **never
overrides** the intentional stadium, cylinder, or diamond geometry (those shapes are drawn as
`path` / `polygon` data, not as a `rect`). The canonical sources remain theme-neutral: they carry
semantic `class` names, the native shape syntax, and the theme-neutral `linkStyle` declaration, but
no fixed color `classDef` declarations.

## Control-plane vs. data-plane

GossipGrid has two clearly separable planes, and diagrams should keep them visually distinct:

- **Control plane (violet):** the lifecycle-metadata authority (ConfigMap watch, RBAC, operator
  genesis/finalization Jobs), readiness gating, and authorization decisions. These are *decisions* and
  *coordination* — they do not move message data.
- **Data plane (blue):** the actual movement of message content — gossip fanout, anti-entropy
  inventory/fetch, bootstrap baseline transfer.

A single diagram that shows both planes should use violet for control edges and blue for data edges, and
should not mix the two colors on the same edge. If a relationship spans both planes (e.g. "admission check
gates the data fetch"), draw the control edge (violet) into a diamond, then the data edge (blue) out of
the diamond.

## Observed vs. inferred relationships

- **Observed** (solid): the relationship is implemented and verified by a test or an in-cluster
  observation.
- **Inferred / potential** (dashed, lighter): the relationship is part of the design but depends on a
  not-yet-implemented stage, or is a conditional path.
- **Planned** (dashed, slate, labeled `planned`): the relationship belongs to a future milestone (Stages
  3C–3E, M5C, M6B, M9). Always label planned edges so a reader does not mistake them for implemented behavior.

## Planned vs. implemented functionality

- **Implemented** (solid, semantic color): the code exists, is committed, and is locally tested.
- **Planned** (dashed, slate, labeled `planned`): the design is documented but the code does not exist.
- Do not render a planned feature with the same visual weight as an implemented one. If a diagram
  contains too many planned concepts, keep the implemented overview solid and move the planned detail
  into a focused companion diagram or into the documentation text.

## Successful vs. unsuccessful operations

- **Success** (green edge or node): `200`, authorized, committed, `READY`.
- **Failure / rejection** (red edge or node): `409` (epoch mismatch, permanent), `403` (revoked),
  `503` (unready / fail-closed), `RECOVERY_REQUIRED`, `PeerBlockedError`.
- A **degraded-but-functional** state (e.g. `DEGRADED`, a partitioned peer that will heal) is amber,
  not red. Reserve red for lockouts and permanent rejections.

## Asynchronous events

- An asynchronous event (a callback, a poll detection, a re-enqueue) is a **dotted, blue** edge with an
  arrow head and a short label (e.g. `onNewMessage re-enqueues`, `poll detects change`).
- Do not draw asynchronous events as solid edges; solid edges imply a synchronous, in-line call.

## Transaction and concurrency boundaries

- A **transaction boundary** (e.g. the single-transaction bootstrap install) is a subgraph with a slate
  border labeled with the lock or transaction it spans (e.g. `single transaction: lifecycle guard → DB →
  SQLite write → commit`).
- A **concurrency boundary** (the linearization point where a revocation can interleave) is a violet
  diamond labeled with the guard (e.g. `CommitUnderBootstrapAuthorization`). The two legal orderings
  (revocation before commit vs. commit before revocation) are drawn as two separate branches from the
  diamond, each with its own outcome (rejected vs. committed-and-preserved).
- Lock ordering (`c.mu RLock → m.mu RLock → Engine.mu Lock`) is annotated as text, not as a diagram
  element; it is a contract, not a flow.

## Failure and recovery timelines

- A **failure/recovery timeline** is a sequence diagram or a left-to-right flowchart. Time flows
  left-to-right (or top-to-bottom for sequence diagrams).
- Each **event** on the timeline is a node labeled with the event (e.g. `pod replaced`, `ConfigMap read`,
  `bind local_metadata`, `install baseline`, `409 epoch mismatch`).
- **Stale-epoch rejection** is a red edge from the stale peer to the rejecting replica, labeled
  `409 epoch mismatch (permanent, no retry, data not merged)`.
- **Operator recovery (M5C)** is a dashed, slate, `planned` edge from `RECOVERY_REQUIRED` back to
  `DISCOVERING`, labeled `operator recovery (5C)`.

## Diagram legends

- Include a legend **only where it adds value** — i.e., when the diagram uses 3 or more distinct
  semantic colors or line styles that are not self-evident from the labels.
- A legend is a small, separate subgraph or a row of labeled swatches in the corner. It is never the
  main content of the diagram.
- If a diagram uses only 1–2 colors and the labels are clear, omit the legend.

## Diagram captions and accessibility

- Every committed diagram has a **caption** in the surrounding documentation: a one- or two-sentence
  sentence that states what the diagram shows and which architectural question it answers.
- The caption is the first thing a reader reads; the diagram is the second. The caption should not
  merely restate the title.
- **Alternative text:** where a diagram is referenced as an image link (the SVG/PNG fallback links), the
  link text should describe the diagram's subject (e.g. "Gossip and anti-entropy replication
  diagram"). For the embedded native-Mermaid blocks, the surrounding caption serves as the accessible
  description.
- **No color-only meaning:** the caption and labels must carry the full meaning; color is a secondary
  cue.

## Layout conventions

- **Direction:** Flowcharts default to `TB` (top-to-bottom) for architectural overviews and `LR`
  (left-to-right) for pipeline and delivery diagrams. Sequence diagrams use their natural top-to-bottom
  time flow.
- **Spacing:** Generous. The centralized Mermaid config sets `flowchart.padding`, `sequence.actorMargin`,
  and `sequence.boxMargin` to keep nodes from crowding. A diagram that requires excessive zooming to
  read is too dense — split it.
- **Subgraph hierarchy:** At most three levels of nested subgraphs. A deep nesting (4+ levels) usually
  means the diagram is trying to show too much.
- **Group related components** into meaningful boundaries (a StatefulSet subgraph, a trust-domain
  subgraph, a pipeline-stage subgraph). A boundary should represent a real architectural unit, not a
  visual convenience.
- **Avoid giant diagrams.** A diagram wider or taller than roughly 1600 px at the committed PNG width
  should be reconsidered. The committed PNG width is 1600 px; a diagram that fills that width with
  unreadable text is too wide.

## Grayscale verification

Before a diagram is considered done, verify it in grayscale:

1. Convert the rendered PNG to grayscale.
2. Check that component types are still distinguished by shape.
3. Check that plane/relationship types are still distinguished by line style.
4. Check that state and outcome meanings are still carried by labels and annotations.

If any meaning is lost, add a non-color cue.

## Application to the native visualization interface (Milestone 6B)

The native visualization interface (M6B) — the cluster-status page, interactive topology, telemetry, and
replication/lifecycle visualization — should **reuse the same semantic color and state conventions**
defined here, where practical:

- **State colors:** a `READY` replica is green, `SYNCING`/`DEGRADED` is amber, `RECOVERY_REQUIRED` is red
  — the same mapping as the diagrams.
- **Plane colors:** control-plane elements (metadata, RBAC, authorization) are violet; data-plane
  elements (gossip, anti-entropy, bootstrap transfer) are blue.
- **Trust boundaries:** the workload-PKI and credential boundaries are violet.
- **Planned vs. implemented:** M6B features that are still in design are shown with the dashed, slate,
  `planned` style; implemented features are solid.
- **Observed, not fabricated:** live visualization must represent **actual observed application events**
  (from the existing `PeerStatusInfo` and the proposed Stage 2 telemetry), not fabricated animations.
  An animation that does not correspond to a real event is a violation of the design.

This section is a **reference** for M6B, not an implementation. M6B's four-stage roadmap (cluster
status/topology, native telemetry foundation, replication visualization, lifecycle/resilience
visualization) is unchanged; see [cluster-visualization.md](cluster-visualization.md) and
[ADR-0004](decisions/0004-cluster-view.md). No frontend code, telemetry collection, new endpoints, or
application visualization features are implemented by this document.

## Rendering hosts

The diagrams are rendered in two distinct ways, and the visual-design system treats them
differently:

**Native adaptive inline experience (GitHub).** The canonical `.mmd` sources are embedded directly in
the documentation as ` ```mermaid ` blocks, which GitHub renders with **its own** Mermaid renderer and
**its own** theme (GitHub does not read the local `mermaid-config.json`). The sources therefore contain
**no fixed light-theme colors** (no hardcoded `classDef` fills, strokes, or text colors, and no
`rect rgb(...)` sequence-rectangle colors) that would pin the inline rendering to one theme. They rely
on the **semantic labels, shapes, and meaningful line styles** to carry meaning, so the inline diagram
adapts to the host's light or dark theme. This is the primary reading surface, and it is deliberately
theme-adaptive rather than a fixed palette. (Actual GitHub light/dark appearance is verified by the
software architect and project owner, not claimed here.)

**Self-contained generated assets (committed SVG/PNG).** `make architecture` renders each canonical
source with the pinned Mermaid CLI (mmdc v11.4.1, configured by
[`mermaid-config.json`](diagrams/mermaid-config.json)) to a committed SVG and a 1600 px-wide PNG. The
generated assets use a fixed, self-contained light theme (the config's `theme: base` with light
`themeVariables`) so they are deterministic and do not depend on a viewer's theme; the PNG is
rendered with an explicit opaque white canvas (`-b white`), matching the SVG's white background and
keeping text contrast coherent. Because the generated assets are not theme-adaptive, the richer
semantic palette and the thicker line weights are applied to them through **centralized
renderer-specific configuration**: a `themeCSS` block that mmdc appends to the generated `<style>`
block. That block carries (a) the stroke-width rules for the actual generated elements (flowchart
edges, node borders, subgraph borders, sequence message lines, state transitions), (b) the
corner-radius rules (8 px component corners, 10 px container corners, 8 px sequence participants,
6 px sequence notes in Chromium), and (c) the semantic node colors, keyed by the semantic `class`
names that the canonical sources retain. The node-color rules are shape-aware:
`g.node.<class> > rect.label-container` (rounded / square rectangle), `g.node.<class> >
path.outer-path` (cylinder, a direct path child), `g.node.<class> > g.outer-path > path`
(stadium, a path nested inside an `outer-path` `g` wrapper — the structure that the earlier
`> path.outer-path` selector missed for stadiums), and `g.node.<class> > polygon.label-container`
(decision diamond), plus `g.node.<class> .nodeLabel` for the text. A neutral 1.8 px border fallback
applies to unclassified visible shapes. The canonical sources carry **no** per-diagram `classDef`
color declarations — only the semantic `class` names, the native shape syntax, and the theme-neutral
`linkStyle default stroke-width:1.8px` declaration — so the same source renders theme-neutrally in
the inline block and semantically in the generated assets. These renderer-specific color styles
live in the generated assets only — they do **not** force the inline rendering, because the inline
blocks are rendered by GitHub's renderer, which never sees the local config.

**Library-wide application and verification.** The shape-aware semantic styling (including the
nested stadium-path selector), the native rounded-rectangle / stadium shape syntax, the theme-neutral
`linkStyle` line weight, and the corner-radius rules have been applied to, and regenerated for,
**all 18 canonical diagrams** in the library (the seven core architecture diagrams and the eleven
concept diagrams). The actual GitHub / GitLab light/dark appearance of the inline blocks is **not**
verified here — that verification, and any further tuning, remains the architect's and project
owner's responsibility. Do not claim universal SVG viewer compatibility or verified GitHub
light/dark appearance; the supported baseline is the light generated-asset canvas plus the
theme-adaptive inline rendering.

The committed SVG/PNG are the **authoritative** visual (the reference rendering); the native inline
block is a **theme-adaptive convenience** that may differ in spacing and tint. A reader who opens the
SVG/PNG sees the fixed light palette, the semantic node colors, and the thicker lines; a reader who
opens the documentation inline sees the host-themed, adaptive version (neutral node fills, host
theme text) with the same labels, shapes, and line styles. Both must remain legible.

## Readability conventions

### Standardized adjacent artifact links

Every embedded ` ```mermaid ` block must be preceded by the standardized artifact-link line,
immediately above the `<!-- DIAGRAM:<base> -->` marker (one intervening blank line allowed):

    [View full-size SVG](<rel>/<base>.svg) · [View PNG](<rel>/<base>.png)

where `<rel>` is the relative prefix from the containing document to
`docs/diagrams/generated/` (empty for `docs/*.md` and `README.md`, `../` for `docs/concepts/*.md`).
The `diagrams_check.sh` Check 3 enforces this adjacency, the base match, and the existence of both
artifact files. A link elsewhere in the document does not satisfy the requirement.

### Natural SVG vs. generated PNG dimensions

The committed SVG is rendered at its **natural** (unspecified) width; the committed PNG is rendered
at a fixed 1600 px width (`-w 1600` in `make architecture`). A diagram whose natural SVG width
exceeds ~1200 px will appear small when the native inline block is scaled to fit a typical
Markdown content width (~800–1200 px). Use the natural SVG width (read from the `viewBox`
attribute) to estimate effective text size, not the PNG width.

### Effective font size

The effective on-screen font size for a diagram displayed at a given viewing width is:

    effectiveFontSize = nominalFontSize × min(1, viewingWidth / naturalSVGWidth)

where `nominalFontSize` is 14 px for flowcharts and 13 px for sequence diagrams (set in
`mermaid-config.json`). Target an effective font size of ~12–14 px at a 1200 px viewing width.
A diagram whose effective font drops below ~10 px at 1200 px is too wide and should be
recomposed (split, reoriented, or reduced in node count), not simply re-scaled.

### Compact composition preference

Prefer a focused, compact diagram over an oversized one. A diagram that communicates one
concept clearly at a readable size is better than a diagram that packs multiple concerns into a
single oversized canvas. When a diagram exceeds ~1200 px natural width, split it into
companion diagrams (e.g., separate the gossip-dissemination path from the anti-entropy
reconciliation detail) and cross-reference them in the adjacent prose.

### linkStyle: authored vs. rendered

The `linkStyle default stroke-width:1.8px` declaration is **authored** in the canonical `.mmd`
source and is **renderer-dependent**: the pinned mmdc renderer (configured via `themeCSS` in
`mermaid-config.json`) applies the stroke-width to generated flowchart and state-diagram edges;
GitHub's native inline renderer may apply its own default edge weight and does not read the local
config. The declaration is present for the generated-asset rendering and is theme-neutral
(no color), so it does not pin the inline rendering to a fixed style. Do not claim a
specific edge weight for the inline rendering; the generated-asset weight is the documented
one.
