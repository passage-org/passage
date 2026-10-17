#!/usr/bin/env node
// Build the deck as a .pptx from the same content as project/slides/*.html.
//
// The HTML sources are the master copy; this script mirrors them so the folder
// can produce a PowerPoint without going through the published page. Run:
//     node passage-qlever/slides/build-pptx.js
//
// Style follows WWW2025-Passage: a full-bleed #0084D1 banner with a white
// Roboto Slab title, Roboto body, green for good and red for bad.

const pptxgen = require("pptxgenjs");

const BLUE = "0084D1";
const GREEN = "38761D";
const RED = "FF0000";
const GREY = "595959";
const BLACK = "000000";
const WHITE = "FFFFFF";
const PALE = "F2F8FD";
const HILITE = "FFF2CC";

const HEAD = "Roboto Slab";
const BODY = "Roboto";
const MONO = "Roboto Mono";

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE"; // 13.333 x 7.5 inches
pres.title = "QLever as a Passage backend";
pres.theme = { headFontFace: HEAD, bodyFontFace: BODY };

const BANNER_H = 1.26;
const L = 0.72; // left margin
const W = 11.9; // content width
const TOP = 1.52; // first line under the banner

pres.defineSlideMaster({
  title: "BANNER",
  background: { color: WHITE },
  objects: [
    { rect: { x: 0, y: 0, w: 13.333, h: BANNER_H, fill: { color: BLUE } } },
    {
      placeholder: {
        options: {
          name: "title", type: "title",
          x: 0.3, y: 0.22, w: 12.73, h: 0.82,
          fontFace: HEAD, fontSize: 32, bold: true, color: WHITE,
          align: "center", valign: "middle", margin: 0,
        },
        text: "",
      },
    },
  ],
  slideNumber: { x: 12.4, y: 6.95, w: 0.6, h: 0.3, fontFace: BODY, fontSize: 10,
                 color: GREY, align: "right" },
});

// ── helpers ──────────────────────────────────────────────────────────────────

function slide(title, notes) {
  const s = pres.addSlide({ masterName: "BANNER" });
  s.addText(title, { placeholder: "title" });
  if (notes) s.addNotes(notes);
  return s;
}

/** A paragraph of rich text. `runs` is a string or an array of {text, options}. */
function para(s, runs, y, opts = {}) {
  const body = Array.isArray(runs) ? runs : [{ text: runs }];
  s.addText(body, {
    x: L, y, w: W, h: opts.h || 0.4, isTextBox: true, margin: 0,
    fontFace: BODY, fontSize: opts.fontSize || 15, color: opts.color || BLACK,
    bold: opts.bold || false, align: "left", valign: "top",
    lineSpacingMultiple: opts.lineSpacing || 1.2,
    fill: opts.fill ? { color: opts.fill } : undefined,
    line: opts.outline ? { color: opts.outline, width: 1.5 } : undefined,
    ...(opts.fill || opts.outline ? { margin: 10 } : {}),
  });
}

/** Bulleted lines: [{text, options}] per line. */
function bullets(s, lines, y, opts = {}) {
  const rows = lines.map((l, i) => ({
    text: l.text,
    options: {
      bullet: true, breakLine: i < lines.length - 1,
      fontSize: l.size || opts.fontSize || 17,
      bold: l.bold !== undefined ? l.bold : false,
      color: l.color || BLACK,
      indentLevel: l.sub ? 1 : 0,
      paraSpaceAfter: l.sub ? 4 : 8,
    },
  }));
  s.addText(rows, {
    x: L, y, w: W, h: opts.h || 2.5, isTextBox: true, margin: 0,
    fontFace: BODY, valign: "top",
  });
}

/**
 * A table whose first row is a blue header.
 * `rows` is an array of arrays of string | {text, options}.
 */
function table(s, head, rows, y, colW, opts = {}) {
  const header = head.map((h, i) => ({
    text: h,
    options: { bold: true, color: WHITE, fill: { color: BLUE },
               align: i === 0 ? "left" : (opts.align || "left") },
  }));
  const body = rows.map((r, ri) =>
    r.map((c, i) => {
      const cell = typeof c === "string" ? { text: c } : c;
      return {
        text: cell.text,
        options: {
          color: cell.color || BLACK,
          bold: cell.bold || false,
          fontFace: cell.mono ? MONO : BODY,
          align: cell.align || (i === 0 ? "left" : (opts.align || "left")),
          fill: { color: cell.fill || (ri % 2 ? PALE : WHITE) },
        },
      };
    })
  );
  s.addTable([header, ...body], {
    x: L, y, w: W, colW,
    fontFace: BODY, fontSize: opts.fontSize || 12,
    border: { type: "solid", color: "BFBFBF", pt: 0.5 },
    margin: opts.cellMargin !== undefined ? opts.cellMargin : 6,
    valign: "middle", autoPage: false,
  });
}

// ── the slides ───────────────────────────────────────────────────────────────

// 1
{
  const s = slide("Why change the backend?",
    "Blazegraph carries tuple counts inside its B+tree nodes, so \"how many matches\" and \"which match sits at rank k\" are both a tree descent. Its own uniform sampler is built straight on keyAt. QLever has neither — that is the whole question in one line.");
  bullets(s, [
    { text: "Blazegraph is no longer maintained", bold: true, color: RED, size: 20 },
    { text: "QLever is maintained, and six times smaller", bold: true, size: 20 },
    { text: "283 MB against 1.7 GB, for the same 10,916,457 triples", sub: true, size: 16 },
    { text: "But it has no augmented B+tree", bold: true, color: RED, size: 20 },
    { text: "no rangeCount, no keyAt(rank)", sub: true, size: 16 },
    { text: "which are exactly the two questions a continuation asks", sub: true, size: 16 },
  ], TOP + 0.3, { h: 4.2 });
}

// 2
{
  const s = slide("Wikidata reached the same conclusion",
    "Source: the Wikidata SPARQL query service backend replacement proposal. Worth saying plainly that it is a proposal open for community review rather than a finished switch, and that Blazegraph-specific services and functions will not carry over, so queries using them must be rewritten.");
  para(s, "The Wikidata Platform Team proposes QLever as the replacement for Blazegraph in the Wikidata Query Service. Their reasons, quoted:", TOP, { h: 0.5 });
  para(s, [
    { text: "“an open source project that has gone " },
    { text: "unmaintained", options: { bold: true } },
    { text: " since being acquired by Amazon in 2018”\n“" },
    { text: "unable to scale", options: { bold: true } },
    { text: " to meet the growth targets of Wikidata”\n“the cause of much of the " },
    { text: "volatility", options: { bold: true } },
    { text: " of the Wikidata Query Service”" },
  ], TOP + 0.6, { h: 1.3, fill: PALE, fontSize: 14 });
  table(s, ["when", "what"], [
    ["Oct 2025 – Mar 2026", "benchmarking and production-replay testing of the candidates"],
    ["November 2025", "triple store evaluation report published"],
    ["April 2026", "contractor engaged for documentation and query rewriting"],
    [{ text: "17–23 Oct 2026", bold: true, fill: HILITE },
     { text: "Query Service Days: the migration from Blazegraph to QLever", bold: true, fill: HILITE }],
  ], TOP + 2.1, [3.0, 8.9], { fontSize: 12 });
  para(s, [
    { text: "Our backend problem is " },
    { text: "the same problem the largest public SPARQL endpoint is solving", options: { color: GREEN } },
    { text: " — which is why porting Passage to QLever is worth doing now." },
  ], TOP + 4.5, { h: 0.6, bold: true, fontSize: 14 });
}

// 3
{
  const s = slide("A third-party evaluation",
    "Say the highlighted row out loud rather than hoping nobody asks: the evaluator is independent, but this particular query set was assembled by QLever's own authors. The author's verdict over all four benchmarks is still that QLever is the fastest of the four engines. Two caveats of his worth knowing: all four deviate from the SPARQL standard somewhere, and Virtuoso silently returns at most 1,048,576 answers per query, which accounts for most of its divergences on WDBench.");
  para(s, "Patel-Schneider, Benchmarking SPARQL Engines on Wikidata Queries, Wikidata Workshop at ISWC 2025. Wikidata of October 2024 — ~20 billion triples, 192 GB of RAM, 10-minute timeout. Four benchmarks, not one:", TOP, { h: 0.6, fontSize: 13 });
  table(s, ["benchmark", "what it is", "whose queries"], [
    [{ text: "WDBench", bold: true }, "fragments of WDQS log queries that timed out, in 5 classes — 2,658 queries", "Angles et al., ISWC 2022"],
    [{ text: "WGPB", bold: true }, "850 queries, 17 graph patterns instantiated 50 times", "Hogan et al., ISWC 2019"],
    [{ text: "WDQS", bold: true, fill: HILITE }, { text: "298 queries from Wikidata Query Service logs", fill: HILITE },
     { text: "QLever’s own evaluation page", bold: true, color: RED, fill: HILITE }],
    [{ text: "Scholia", bold: true }, "new, built from the Scholia interface’s query templates", "the author"],
  ], TOP + 0.7, [2.1, 5.6, 4.2], { fontSize: 11 });
  para(s, "The WDQS benchmark, median and mean per query, in milliseconds:", TOP + 2.7, { h: 0.3, bold: true, fontSize: 13 });
  table(s, ["engine", "median", "mean", "timeouts"], [
    [{ text: "QLever", bold: true, fill: PALE }, { text: "103", bold: true, color: GREEN, align: "right", fill: PALE },
     { text: "4,583", bold: true, color: GREEN, align: "right", fill: PALE }, { text: "0", bold: true, color: GREEN, align: "right", fill: PALE }],
    ["Virtuoso", { text: "461", align: "right" }, { text: "14,645", align: "right" }, { text: "2", align: "right" }],
    ["Blazegraph", { text: "511", align: "right" }, { text: "12,560", align: "right" }, { text: "1", align: "right" }],
    ["MillenniumDB", { text: "588", align: "right" }, { text: "103,271", align: "right" }, { text: "43", color: RED, align: "right" }],
  ], TOP + 3.1, [4.1, 2.6, 2.6, 2.6], { fontSize: 12, align: "right" });
  para(s, [
    { text: "Loading the same dump: QLever " },
    { text: "about 4.5 hours", options: { color: GREEN } },
    { text: ", Blazegraph " },
    { text: "over 10 days — first attempt failed", options: { color: RED } },
    { text: "." },
  ], TOP + 4.95, { h: 0.4, bold: true, fontSize: 14 });
}

// 4
{
  const s = slide("An index on disk",
    "Two things to point at. The per-block metadata lives at the END of the permutation file, not in the sidecar: the reader seeks to the last offset and deserialises it at start-up. And the .meta sidecar is 992 bytes for PSO but 32 bytes for SPO — because SPO has 521,585 relations that each live inside a single block and are addressed by an offset within it.");
  table(s, ["file", "what it holds", "size"], [
    ["watdiv10m.index.pso", "compressed blocks, then the per-block metadata at the end of the same file", { text: "47.5 MB", align: "right" }],
    [{ text: "watdiv10m.index.pso.meta", bold: true }, "per-relation metadata", { text: "992 B", align: "right" }],
    ["… and the same for 5 other permutations", "POS, SPO, SOP, OPS, OSP", { text: "258 MB", align: "right" }],
    ["watdiv10m.vocabulary.words.external", "the strings themselves, plus a file of offsets into them", { text: "36 MB", align: "right" }],
    ["watdiv10m.vocabulary.words.internal", "the words kept in RAM, plus their ids and codebooks", { text: "53 kB", align: "right" }],
    ["watdiv10m.internal.index.pso / .pos", "QLever’s own triples (patterns, graph names)", { text: "1.7 MB", align: "right" }],
    ["watdiv10m.meta-data.json", "locale, prefixes, index-rows-per-block, format version", { text: "809 B", align: "right" }],
  ], TOP, [4.6, 5.6, 1.7], { fontSize: 11 });
  para(s, [
    { text: "10,916,457 triples — " },
    { text: "285 MB in total", options: { color: GREEN } },
    { text: ", against 1.7 GB for the same data in Blazegraph." },
  ], TOP + 4.5, { h: 0.4, bold: true, fontSize: 15 });
}

// 5
{
  const s = slide("Before the triples: the Id",
    "The point that matters downstream: a bound component in a scan is an Id, and resolving it is a vocabulary lookup — unless it is a number or a date, which cost nothing. It is also where the absent-value trap lives: a string not in the vocabulary has no Id at all, and that has to be reported rather than silently turned into a wildcard.");
  para(s, "A permutation stores no strings, only 64-bit Ids: 4 bits of datatype and 60 bits of value — 13 datatypes defined of the 16 the tag allows.", TOP, { h: 0.5, fontSize: 15 });
  // the bit-layout bar
  const barY = TOP + 0.65, barH = 0.62;
  s.addShape(pres.ShapeType.rect, { x: L, y: barY, w: 2.6, h: barH, fill: { color: BLUE }, line: { color: BLACK, width: 1 } });
  s.addText("4 bits\ndatatype", { x: L, y: barY, w: 2.6, h: barH, isTextBox: true, margin: 0,
    fontFace: BODY, fontSize: 13, bold: true, color: WHITE, align: "center", valign: "middle" });
  s.addShape(pres.ShapeType.rect, { x: L + 2.75, y: barY, w: W - 2.75, h: barH, fill: { color: "D9EAD3" }, line: { color: BLACK, width: 1 } });
  s.addText("60 bits — a value, or an index into the vocabulary", { x: L + 2.75, y: barY, w: W - 2.75, h: barH,
    isTextBox: true, margin: 0, fontFace: BODY, fontSize: 13, bold: true, color: BLACK, align: "center", valign: "middle" });
  table(s, ["datatype", "where the value lives"], [
    ["Int · Double · Bool · Date · GeoPoint", { text: "inside the Id — no lookup, and they compare and sort directly", color: GREEN, bold: true }],
    ["EncodedVal", "IRIs under a configured prefix, encoded in the Id itself"],
    ["VocabIndex · LocalVocabIndex · BlankNodeIndex", { text: "an index into the vocabulary — a lookup", color: RED }],
  ], barY + 0.85, [4.8, 7.1], { fontSize: 12 });
  para(s, "The vocabulary is split: internal in RAM (53 kB here) and external on disk (27.7 MB of words plus 8.4 MB of offsets). Ids are assigned so that their order is the strings’ order, which is what makes a prefix or a range a contiguous scan.", barY + 2.5, { h: 0.7, fontSize: 13 });
  para(s, "Published: wiki, “Internal and external vocabulary” and “QLever’s IDs for IRIs and literals” — conceptual, dated 2022. The 4/60 split is from the source.", barY + 3.2, { h: 0.4, fontSize: 11, color: GREY });
}

// 6
{
  const s = slide("Id order is value order",
    "Every Id on this slide was read from the fig2 index by asking QLever for the string behind it. The right-hand column is the honest catch: ordering by Id groups by datatype first, in enum order, so it is not SPARQL's comparison — that lives in ValueIdComparators and is a different function. An offset into a scan is a position in THIS order.");
  para(s, "Permutations are sorted by a plain 64-bit integer compare on the Id. No string is ever compared. The order still comes out right, because the Ids are handed out that way.", TOP, { h: 0.5, fontSize: 14 });
  const half = (W - 0.3) / 2;
  // left table
  s.addTable([
    [{ text: "Count the payload up, read the strings out", options: { bold: true, color: WHITE, fill: { color: BLUE }, colspan: 2, align: "center" } }],
    ...[["4000000000000000", "<http://ex.org/H1>"],
        ["4000000000000001", "<http://ex.org/H2>"],
        ["4000000000000002", "<http://ex.org/H3>"],
        ["4000000000000003", "<http://ex.org/Human>"],
        ["4000000000000004", "<http://ex.org/O1>"],
        ["4000000000000006", "<http://ex.org/O3>"],
        ["4000000000000007", "<http://ex.org/occupation>"],
        ["4000000000000008", "<http://ex.org/type>"]].map(r =>
      r.map(c => ({ text: c, options: { fontFace: MONO, fontSize: 9.5 } }))),
  ], { x: L, y: TOP + 0.6, w: half, colW: [half * 0.45, half * 0.55], fontFace: MONO, fontSize: 9.5,
       border: { type: "solid", color: "BFBFBF", pt: 0.5 }, margin: 3, valign: "middle", autoPage: false });
  // right table
  s.addTable([
    [{ text: "The 4-bit tag decides the group", options: { bold: true, color: WHITE, fill: { color: BLUE }, colspan: 2, align: "center" } }],
    ...[["1000000000000001", "\"true\"^^xsd:boolean", BLACK],
        ["200000000000002A", "\"42\"^^xsd:int", GREEN],
        ["20000000000003E8", "\"1000\"^^xsd:int", GREEN],
        ["3800000000000000", "\"-0.0\"^^xsd:decimal", BLACK],
        ["4000000000000000", "<http://ex.org/H1>", BLACK]].map(r =>
      [{ text: r[0], options: { fontFace: MONO, fontSize: 9.5, color: r[2] } },
       { text: r[1], options: { fontFace: MONO, fontSize: 9.5, color: r[2] } }]),
  ], { x: L + half + 0.3, y: TOP + 0.6, w: half, colW: [half * 0.45, half * 0.55], fontFace: MONO, fontSize: 9.5,
       border: { type: "solid", color: "BFBFBF", pt: 0.5 }, margin: 3, valign: "middle", autoPage: false });
  para(s, "Byte-wise lexicographic: H3 < Human because ‘3’ < ‘u’. For Int the payload is the number — 42 is …002A, and this index contains no integers at all.", TOP + 3.3, { h: 0.5, fontSize: 12 });
  para(s, "Largest Int Id 2FFFFFFFFFFFFFFF < smallest IRI Id 4000000000000000 — so every number sorts before every IRI. That is the storage order, not the SPARQL order.",
    TOP + 3.9, { h: 0.7, fontSize: 13, outline: RED });
}

// 7
{
  const s = slide("Six sorted permutations",
    "86 predicates against a million objects, for the same ten million triples: that asymmetry is why the block counts differ. Note that the bound components have to be a PREFIX of the key order — that constraint is what decides which of the six a given pattern uses.");
  para(s, "Every triple is stored six times, each time sorted by a different key order. The first column is called col0, and its distinct values are the relations.", TOP, { h: 0.5, fontSize: 15 });
  table(s, ["permutation", "answers", "relations (distinct col0)", "blocks"], [
    [{ text: "PSO · POS", bold: true }, "bound predicate", { text: "86", align: "right" }, { text: "366", align: "right" }],
    [{ text: "SPO · SOP", bold: true }, "bound subject", { text: "521,585", align: "right" }, { text: "234", align: "right" }],
    [{ text: "OPS · OSP", bold: true }, "bound object", { text: "1,005,832", align: "right" }, { text: "249", align: "right" }],
  ], TOP + 0.7, [2.6, 3.1, 3.6, 2.6], { fontSize: 13 });
  para(s, [
    { text: "A scan picks the permutation whose key order puts the bound components first. The matching triples are then a " },
    { text: "contiguous range", options: { color: GREEN, bold: true } },
    { text: " — which is what makes an offset meaningful at all." },
  ], TOP + 2.6, { h: 0.7, fontSize: 15 });
  para(s, "This much is published: Bast et al., Knowledge Graphs, 2023, §1.5.2. Everything from here on is not.", TOP + 3.4, { h: 0.4, fontSize: 12, color: GREY });
}

// 8
{
  const s = slide("Where a block ends",
    "The third rule is the one that matters for correctness later: because a block never splits triples that differ only by graph, a block's row count can be trusted without opening it — which is exactly the guard the offset patch checks.");
  para(s, [
    { text: "index-rows-per-block = 31,250 is a " },
    { text: "soft", options: { color: RED, bold: true } },
    { text: " target. Three rules bend it, and they explain the counts on the previous slide." },
  ], TOP, { h: 0.5, fontSize: 15 });
  bullets(s, [
    { text: "A large relation starts a new block", bold: true, size: 17 },
    { text: "PSO: 349 blocks of 31,250 rows, plus up to 85 relation boundaries → 366", sub: true, size: 14 },
    { text: "Small relations share a block, filled to 1.5 × the target = 46,875 rows", bold: true, size: 17 },
    { text: "SPO: 10,916,457 ÷ 46,875 = 233 → 234, for 521,585 relations", sub: true, size: 14 },
    { text: "each is found by offsetInBlock_, so it needs no entry of its own", sub: true, size: 14 },
    { text: "A block grows past the target rather than split triples equal but for their graph", bold: true, size: 17 },
  ], TOP + 0.7, { h: 3.6 });
}

// 9
{
  const s = slide("What is actually in a block",
    "The counter-intuitive row is PSO. Its col0 has only 86 distinct values and compresses to nothing, yet the file is larger than SPO's — because what matters is the other columns: sorted by predicate, the subjects inside one predicate are a long ascending run but the objects are near-random, while SPO gets two low-entropy columns. Every number here was read from the index.");
  para(s, "The first rows of a PSO block, as stored — four columns of 64-bit Ids, each column its own Zstd stream:", TOP, { h: 0.4, fontSize: 14 });
  const rows = [
    ["0", "4000000000100FF0", "40000000000979A6", "400000000009DB51"],
    ["1", "4000000000100FF0", "40000000000979A7", "400000000009DB51"],
    ["2", "4000000000100FF0", "40000000000979A8", "400000000009DB4F"],
    ["3", "4000000000100FF0", "40000000000979A9", "400000000009DB57"],
    ["4", "4000000000100FF0", "40000000000979AA", "400000000009DB56"],
  ].map(r => [
    { text: r[0], mono: true },
    { text: r[1], mono: true, color: GREEN },
    { text: r[2], mono: true, color: BLUE },
    { text: r[3], mono: true },
  ]);
  rows.push([
    { text: "…", mono: true },
    { text: "identical, 31,250 times", color: GREEN, bold: true },
    { text: "+1 every row", color: BLUE, bold: true },
    { text: "~10 distinct values", bold: true },
  ]);
  table(s, ["row", "col0 (P)", "col1 (S)", "col2 (O)"], rows, TOP + 0.5, [1.1, 3.6, 3.6, 3.6], { fontSize: 11 });
  para(s, "Row 0 spelled out: <…#type> · <…wsdbm/Product0> · <…wsdbm/ProductCategory11> — and a fourth column holds the graph.", TOP + 3.05, { h: 0.4, fontSize: 12 });
  para(s, "That shape is why it compresses, and why the six permutations do not compress alike:", TOP + 3.5, { h: 0.3, bold: true, fontSize: 13 });
  table(s, ["permutation", "col0 is", "on disk (333 MB raw)", "ratio"], [
    [{ text: "SPO", bold: true }, "subject — 521,585 values", { text: "28.4 MB", align: "right" }, { text: "11.7×", bold: true, color: GREEN, align: "right" }],
    [{ text: "PSO", bold: true }, "predicate — 86 values", { text: "45.3 MB", align: "right" }, { text: "7.4×", align: "right" }],
    [{ text: "OPS", bold: true }, "object — 1,005,832 values", { text: "47.9 MB", align: "right" }, { text: "6.9×", align: "right" }],
  ], TOP + 3.9, [2.2, 4.4, 3.3, 2.0], { fontSize: 11 });
}

// 10
{
  const s = slide("What is held in RAM",
    "firstTriple_ and lastTriple_ are what the binary search uses to find the range of blocks a pattern touches, without reading any of them. numRows_ is what lets an offset be resolved before any I/O. Both were already there; nothing in this work added a field.");
  const half = (W - 0.3) / 2;
  const mk = (x, head, pairs) => {
    s.addTable([
      [{ text: head, options: { bold: true, color: WHITE, fill: { color: BLUE }, colspan: 2, align: "center" } }],
      ...pairs.map(p => [
        { text: p[0], options: { fontFace: MONO, fontSize: 10, color: p[2] || BLACK, bold: !!p[2] } },
        { text: p[1], options: { fontFace: BODY, fontSize: 10 } },
      ]),
    ], { x, y: TOP + 0.1, w: half, colW: [half * 0.52, half * 0.48], fontSize: 10,
         border: { type: "solid", color: "BFBFBF", pt: 0.5 }, margin: 4, valign: "middle", autoPage: false });
  };
  mk(L, "CompressedBlockMetadata — one per block", [
    ["offsetsAndCompressedSize_", "one per column"],
    ["numRows_", "rows in this block", RED],
    ["firstTriple_", "col0, col1, col2, graph"],
    ["lastTriple_", "col0, col1, col2, graph"],
    ["graphInfo_", "graphs present, if few"],
    ["containsDuplicates…", "…WithDifferentGraphs_"],
    ["blockIndex_", "its place in the file"],
  ]);
  mk(L + half + 0.3, "CompressedRelationMetadata — one per relation", [
    ["col0Id_", "the relation itself"],
    ["numRows_", "its size"],
    ["multiplicityCol1_", "1.0 means functional"],
    ["multiplicityCol2_", "used by the planner"],
    ["offsetInBlock_", "where it starts in a shared block"],
    ["= uint64 max", "means: it owns its blocks"],
  ]);
  para(s, [
    { text: "Every block’s numRows_ is " },
    { text: "exact and already in memory", options: { color: GREEN } },
    { text: " — the whole offset mechanism rests on that one field." },
  ], TOP + 3.6, { h: 0.4, bold: true, fontSize: 15 });
  para(s, "366 blocks for PSO, 234 for SPO, 249 for OPS: a few thousand records for a ten-million-triple index.", TOP + 4.1, { h: 0.4, fontSize: 13 });
}

// 11
{
  const s = slide("A scan, step by step",
    "Step 4 is the forty-line patch: skipped blocks are counted, never opened, and the guard is that a block's row count can only be trusted when the block holds no duplicates differing by graph. Step 5 is why a per-thread cache of decompressed blocks is worth 38x on scan opening.");
  table(s, ["#", "what happens", "cost"], [
    ["1", "Resolve each bound component to an Id in the vocabulary", { text: "a lookup", color: GREEN }],
    ["2", "Pick the permutation whose key order puts them first", { text: "free", color: GREEN }],
    ["3", "Binary-search firstTriple_ / lastTriple_ for the block range", { text: "in RAM, no I/O", color: GREEN }],
    [{ text: "4", bold: true, fill: HILITE }, { text: "Walk numRows_ to the block holding the offset", bold: true, fill: HILITE },
     { text: "in RAM — this is the patch", bold: true, fill: HILITE }],
    ["5", "Read and decompress that block’s needed columns", { text: "one block, every time", color: RED, bold: true }],
    ["6", "Trim the first and last block, which may be partial", "a scan of the block"],
    ["7", "Stream the blocks in between, ~20 in flight", { text: "fast and parallel", color: GREEN }],
  ], TOP, [0.8, 7.1, 4.0], { fontSize: 12 });
  para(s, [
    { text: "Steps 1–4 are free. " },
    { text: "Step 5 is the whole cost of opening a scan", options: { color: RED } },
    { text: ", and it does not depend on how many rows you then read." },
  ], TOP + 3.9, { h: 0.5, bold: true, fontSize: 15 });
}

// 12
{
  const s = slide("Compatible anyway",
    "An index built before the patch is read by the patched engine unchanged. That matters more than the size of the patch: a mechanism needing the index format to change would not port to the next engine either.");
  bullets(s, [
    { text: "The block index holds exact row counts, in RAM", bold: true, size: 19 },
    { text: "an offset resolves before any block is read", sub: true, size: 15 },
    { text: "An exact cardinality exists too", bold: true, size: 19 },
    { text: "it decompresses the two boundary blocks — correct, not free", sub: true, size: 15 },
    { text: "40 lines of QLever, off by default, nothing in the index format", bold: true, size: 19 },
  ], TOP + 0.2, { h: 3.0 });
  para(s, "Everything a continuation depends on stays exact.\nWhat changes is the price, not the correctness.",
    TOP + 3.4, { h: 1.0, bold: true, fontSize: 18, outline: RED });
}

// 13
{
  const s = slide("What it costs instead",
    "Read the third row against the first two: an exact count is three orders of magnitude dearer than the estimate, and the block cache closes most but not all of that. The engine asks for the exact one wherever an answer depends on it, and for the estimate everywhere else.");
  table(s, ["QLever, per call", "?s ?p ?o", "?s a ?c"], [
    ["exact cardinality, no block cache", { text: "1,047 µs", align: "right" }, { text: "766 µs", align: "right" }],
    ["exact cardinality, cache of 16", { text: "33 µs", align: "right" }, { text: "14 µs", align: "right" }],
    ["the metadata estimate", { text: "0.9 µs", bold: true, color: GREEN, align: "right" }, { text: "0.2 µs", bold: true, color: GREEN, align: "right" }],
    ["re-enter at an advancing offset", { text: "1,812 µs", align: "right" }, { text: "14 µs", bold: true, color: GREEN, align: "right" }],
    ["re-enter at a scattered offset", { text: "3,063 µs", align: "right" }, { text: "476 µs", bold: true, color: RED, align: "right" }],
    ["one uniform draw", { text: "2,725 µs", bold: true, color: RED, align: "right" }, { text: "500 µs", bold: true, color: RED, align: "right" }],
  ], TOP + 0.3, [5.9, 3.0, 3.0], { fontSize: 13 });
  para(s, "WatDiv 10M · 10,916,457 and 136,215 matches · 100 calls each · skip is lazy, so every row reads a component afterwards",
    TOP + 4.0, { h: 0.4, fontSize: 11, color: GREY });
}

// 14
{
  const s = slide("Limitations",
    "Three of the four limitations are constants, and this is the one that is a difference in kind. If the random-walk estimators matter on this backend, the thing to build is a per-block distinct count or a cheap rank — not a bigger cache.");
  bullets(s, [
    { text: "A continuation re-enters where it stopped — 14 µs", bold: true, color: GREEN, size: 19 },
    { text: "it stays in the same block, which is what the block cache is for", sub: true, size: 15 },
    { text: "A random walk re-enters anywhere — 476 µs", bold: true, color: RED, size: 19 },
    { text: "the cache cannot help: 476 µs against 477 with it off", sub: true, size: 15 },
    { text: "depth d pays d exact counts and d scattered re-entries, where keyAt pays d tree descents", sub: true, size: 15 },
  ], TOP + 0.2, { h: 3.0 });
  para(s, "Preemption ports. Sampling pays.", TOP + 3.5, { h: 0.8, bold: true, fontSize: 22, outline: RED });
}

// 15
{
  const s = slide("Where this is written down",
    "This slide answers the obvious question from the audience: why read the source rather than the documentation. The honest answer is that for the storage layer there is none, and the one paper that describes a block structure describes one that no longer exists and that was explicitly uncompressed.");
  table(s, ["what", "documented where"], [
    ["The six permutations", { text: "Book chapter 2023, §1.5.2, with worked examples; also the wiki", color: GREEN, bold: true }],
    ["Internal / external vocabulary, Id ordering", { text: "Wiki, two pages — conceptual, dated 2022" }],
    [{ text: "The 4 + 60 bit layout of an Id", fill: HILITE }, { text: "nowhere — the wiki describes ranges, never the encoding", color: RED, bold: true, fill: HILITE }],
    [{ text: "Blocks, their size, their cutting rules", fill: HILITE }, { text: "nowhere", color: RED, bold: true, fill: HILITE }],
    [{ text: "Per-column Zstd, offsets and sizes", fill: HILITE }, { text: "nowhere — the wiki only says “compressed in a variety of ways”", color: RED, bold: true, fill: HILITE }],
    [{ text: "Block and relation metadata, their fields", fill: HILITE }, { text: "nowhere", color: RED, bold: true, fill: HILITE }],
    [{ text: "Metadata at the end of the permutation file", fill: HILITE }, { text: "nowhere", color: RED, bold: true, fill: HILITE }],
  ], TOP, [5.2, 6.7], { fontSize: 11 });
  para(s, "The CIKM 2017 paper does describe blocks — but of an older, different index: lists of subject + offset pairs, each block keyed by its lowest subject ID. On compressing them it says: “We choose to not do this.”", TOP + 3.9, { h: 0.6, fontSize: 12 });
  para(s, "No architecture page at docs.qlever.dev. The wiki’s “Overview over the QLever codebase” is “currently still a stub” and covers Turtle parsing only.", TOP + 4.5, { h: 0.5, fontSize: 12 });
}

// 16
{
  const s = slide("References",
    "This list is the one the upstream README asks for, not the one on Wikipedia, which misses the TGDK, GRASP and Sparqloscope papers and dates the autocompletion work to an arXiv preprint rather than CIKM 2022.");
  para(s, "QLever, as the project itself asks to be cited:", TOP, { h: 0.3, bold: true, fontSize: 13 });
  bullets(s, [
    { text: "Bast, Buchhold. QLever: A Query Engine for Efficient SPARQL+Text Search. CIKM 2017, 647–656.", size: 11 },
    { text: "Bast, Kalmbach, Klumpp, Kramer, Schnelle. Efficient SPARQL Autocompletion via SPARQL. CIKM 2022.", size: 11 },
    { text: "Bast, Brosi, Kalmbach, Klumpp. Knowledge Graphs (book chapter), 2023 — §1.5.2 covers triple permutations.", size: 11 },
    { text: "Bast et al. The dblp Knowledge Graph and SPARQL Endpoint. TGDK 2024.", size: 11 },
    { text: "Bast, Brosi, Kalmbach. Efficient Spatial Joins on Large Geometry Sets. SIGSPATIAL 2025, 466–476.", size: 11 },
    { text: "Walter, Bast. GRASP: zero-shot question answering on RDF graphs. ISWC 2025.", size: 11 },
    { text: "Bast, Kalmbach, Tsiapalis, Ullinger. Sparqloscope: a comprehensive SPARQL benchmark. ISWC 2025.", size: 11 },
  ], TOP + 0.35, { h: 2.0 });
  para(s, "Independent evaluations, and the migration:", TOP + 2.45, { h: 0.3, bold: true, fontSize: 13 });
  bullets(s, [
    { text: "Patel-Schneider. Benchmarking SPARQL Engines on Wikidata Queries. Wikidata Workshop at ISWC 2025.", size: 11 },
    { text: "Lam, Elvesæter, Martin-Recuerda. Evaluation of a Representative Selection of SPARQL Query Engines Using Wikidata. ESWC 2023, 679–696.", size: 11 },
    { text: "Wikidata Platform Team. WDQS backend update — Backend Replacement. wikidata.org.", size: 11 },
  ], TOP + 2.8, { h: 1.1 });
  para(s, "Talks: Bast, The QLever SPARQL Engine — DSG Seminar, Waterloo 2022 (recorded) · Wikidata Data Modeling Days 2023 · DBpedia Day keynote, SEMANTiCS 2025.   Docs: docs.qlever.dev · github.com/ad-freiburg/qlever/wiki",
    TOP + 4.0, { h: 0.6, fontSize: 11 });
}

const out = process.argv[2] || "QLever as a Passage backend.pptx";
pres.writeFile({ fileName: out }).then(() => console.log("wrote " + out));
