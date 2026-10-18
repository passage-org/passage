#!/usr/bin/env node
//
// Turn the deck's HTML sources into a PowerPoint, by reading them rather than
// by mirroring them — so the .pptx cannot drift from the slides.
//
//     node html2pptx.js [output.pptx] [project-dir]
//
// Needs pptxgenjs and node-html-parser. Neither is vendored:
//     npm install pptxgenjs node-html-parser
// or run with NODE_PATH pointing at an install elsewhere.
//
// It understands the subset these slides actually use: a pinned banner, a
// pinned title, a pinned page number, then flow children that are paragraphs,
// one-level lists, tables, and rows of cards. Anything else is skipped with a
// warning rather than guessed at.

const fs = require("fs");
const path = require("path");
const pptxgen = require("pptxgenjs");
const { parse } = require("node-html-parser");

const PX_PER_IN = 144; // 1920 px over 13.333 in
const px = (v) => v / PX_PER_IN;
const pt = (v) => v / 2; // 1920-px canvas at 13.333 in: 1 px = 0.5 pt

// ── reading inline styles ────────────────────────────────────────────────────

function styleOf(el) {
  const out = {};
  for (const decl of (el.getAttribute("style") || "").split(";")) {
    const i = decl.indexOf(":");
    if (i < 0) continue;
    out[decl.slice(0, i).trim()] = decl.slice(i + 1).trim();
  }
  return out;
}
const num = (s, dflt = 0) => (s ? parseFloat(s) : dflt);
const hex = (s) => (s && s.startsWith("#") ? s.slice(1).toUpperCase() : undefined);

/** padding: "a", "a b", "a b c" or "a b c d" → {top,right,bottom,left} in px. */
function padding(s) {
  if (!s) return { top: 0, right: 0, bottom: 0, left: 0 };
  const p = s.split(/\s+/).map(parseFloat);
  const [t, r, b, l] =
    p.length === 1 ? [p[0], p[0], p[0], p[0]]
    : p.length === 2 ? [p[0], p[1], p[0], p[1]]
    : p.length === 3 ? [p[0], p[1], p[2], p[1]]
    : p;
  return { top: t, right: r, bottom: b, left: l };
}

/** The family name without its fallbacks, unquoted. */
function family(s) {
  if (!s) return undefined;
  return s.split(",")[0].trim().replace(/^['"]|['"]$/g, "");
}

// ── inline markup → pptxgenjs runs ───────────────────────────────────────────

function runs(el, inherited = {}) {
  const out = [];
  const walk = (node, opts) => {
    for (const child of node.childNodes) {
      if (child.nodeType === 3) {
        const text = child.rawText
          .replace(/&nbsp;/g, " ").replace(/&amp;/g, "&")
          .replace(/&lt;/g, "<").replace(/&gt;/g, ">")
          .replace(/&#39;/g, "'").replace(/&quot;/g, '"')
          .replace(/\s+/g, " ");
        if (text) out.push({ text, options: { ...opts } });
        continue;
      }
      const tag = child.rawTagName ? child.rawTagName.toLowerCase() : "";
      if (tag === "br") { out.push({ text: "", options: { ...opts, breakLine: true } }); continue; }
      const next = { ...opts };
      if (tag === "b" || tag === "strong") next.bold = true;
      if (tag === "i" || tag === "em") next.italic = true;
      if (tag === "u") next.underline = true;
      const st = styleOf(child);
      if (hex(st.color)) next.color = hex(st.color);
      if (family(st["font-family"])) next.fontFace = family(st["font-family"]);
      walk(child, next);
    }
  };
  walk(el, inherited);
  // A trailing empty run with breakLine would add a blank line.
  while (out.length && out[out.length - 1].text === "") out.pop();
  return out.length ? out : [{ text: "", options: { ...inherited } }];
}

const plain = (el) => runs(el).map((r) => r.text).join("");

// ── height estimates, so flow children can be stacked ────────────────────────

/** Characters that fit on one line of `width` px at `size` px. */
const perLine = (width, size) => Math.max(1, Math.floor(width / (size * 0.5)));

function textHeight(text, size, width, lineHeight) {
  const hard = text.split("\n").length;
  const lines = Math.max(hard, Math.ceil(text.length / perLine(width, size)));
  // A little slack: the estimate is a character count, and a wrap that lands
  // one word early costs a whole line.
  return lines * size * lineHeight * 1.08;
}

// ── the conversion ───────────────────────────────────────────────────────────

function addSlide(pres, html, warn) {
  const root = parse(html);
  const section = root.querySelector("section");
  if (!section) throw new Error("no <section>");
  const sst = styleOf(section);
  const slide = pres.addSlide();
  slide.background = { color: hex(sst.background) || "FFFFFF" };

  const pad = padding(sst.padding);
  const gap = num(sst.gap, 24);
  const baseFamily = family(sst["font-family"]) || "Arial";
  const baseColor = hex(sst.color) || "000000";
  const inner = 1920 - pad.left - pad.right;

  const flow = [];
  for (const child of section.childNodes) {
    if (child.nodeType !== 1) continue;
    const tag = child.rawTagName.toLowerCase();
    if (tag === "aside") { slide.addNotes(plain(child).trim()); continue; }
    const st = styleOf(child);
    if (st.position === "absolute") { pinned(slide, child, st, baseFamily, warn); continue; }
    flow.push({ el: child, tag, st });
  }

  // Measure first, against a sink that draws nothing, so an over-full slide
  // can be scaled down the way the HTML renderer shrinks one.
  const nowhere = { addText() {}, addTable() {}, addShape() {}, addNotes() {} };
  const layout = (sink, scale) => {
    let y = pad.top;
    for (const item of flow) {
      y += render(sink, item, y, pad.left, inner, baseFamily, baseColor, warn, scale)
         + gap * scale;
    }
    return y - gap * scale;
  };

  const available = 1080 - pad.top - pad.bottom;
  const needed = layout(nowhere, 1) - pad.top;
  let scale = 1;
  if (needed > available) {
    scale = Math.max(0.78, available / needed);
    warn(`content needs ${Math.round(needed)} px for ${available}; scaled to ${scale.toFixed(2)}`);
  }
  const end = layout(slide, scale);
  if (end > 1080 - pad.bottom + 2) {
    warn(`still reaches ${Math.round(end)} px after scaling — shorten the slide`);
  }
}

/** A position:absolute child: the banner, the title, the page number. */
function pinned(slide, el, st, baseFamily, warn) {
  const tag = el.rawTagName.toLowerCase();
  const left = num(st.left), top = num(st.top);
  const w = num(st.width, 0), h = num(st.height, 0);
  const text = plain(el).trim();

  if (!text && hex(st.background)) {                       // the banner
    slide.addShape("rect", { x: px(left), y: px(top), w: px(w), h: px(h),
                             fill: { color: hex(st.background) } });
    return;
  }
  const size = num(st["font-size"], 32);
  const y = st.bottom !== undefined ? px(1080 - num(st.bottom) - size * 1.4) : px(top);
  slide.addText(runs(el), {
    x: px(left), y, w: px(w || 400), h: px(h || size * 1.6), isTextBox: true, margin: 0,
    fontFace: family(st["font-family"]) || baseFamily, fontSize: pt(size),
    bold: st["font-weight"] === "700" || st["font-weight"] === "bold",
    color: hex(st.color) || "000000", align: st["text-align"] || "left", valign: "top",
    lineSpacingMultiple: num(st["line-height"], 1.2),
  });
}

/** Render one flow child at `y`; return the height it took, in px. */
function render(slide, { el, tag, st }, y, x, width, baseFamily, baseColor, warn, scale = 1) {
  const sz = (v) => v * scale;
  const fill = hex(st.background);
  const border = st.border && !st.border.startsWith("0")
    ? { color: hex(st.border.split(/\s+/).find((t) => t.startsWith("#"))) || "000000",
        width: num(st.border) || 1 }
    : undefined;
  const boxPad = padding(st.padding);

  if (tag === "p" || tag === "h3") {
    const size = sz(num(st["font-size"], 32));
    const lh = num(st["line-height"], 1.4);
    const w = num(st.width, width);
    const h = textHeight(plain(el), size, w - boxPad.left - boxPad.right, lh)
            + boxPad.top + boxPad.bottom;
    slide.addText(runs(el), {
      x: px(x), y: px(y), w: px(w), h: px(h), isTextBox: true,
      margin: boxPad.top ? pt(boxPad.top) : 0,
      fontFace: family(st["font-family"]) || baseFamily, fontSize: pt(size),
      bold: st["font-weight"] === "700" || st["font-weight"] === "bold",
      color: hex(st.color) || baseColor, align: st["text-align"] || "left", valign: "top",
      lineSpacingMultiple: lh,
      fill: fill ? { color: fill } : undefined, line: border,
    });
    return h;
  }

  if (tag === "ul" || tag === "ol") {
    const size = sz(num(st["font-size"], 32));
    const lh = num(st["line-height"], 1.4);
    // the sub-lists use `padding-left`, not the `padding` shorthand
    const indent = (padding(st.padding).left || num(st["padding-left"])) * scale;
    const items = el.querySelectorAll("li");
    const w = width - indent;
    const body = [];
    let h = 0;
    items.forEach((li, i) => {
      // The bullet is a run of our own: pptxgenjs's `bullet` option splits a
      // multi-run paragraph at the wrong place, which silently breaks any item
      // holding a <b> or a coloured span.
      const r = runs(li, {
        bold: st["font-weight"] === "700" || st["font-weight"] === "bold",
        color: hex(st.color) || baseColor,
      });
      r.unshift({ text: "\u2022\u2003", options: { color: hex(st.color) || baseColor } });
      r.forEach((run, j) => {
        run.options = { ...run.options, fontSize: pt(size) };
        if (j === r.length - 1 && i < items.length - 1) {
          run.options.breakLine = true;
          run.options.paraSpaceAfter = pt(size * 0.35);
        }
      });
      body.push(...r);
      h += textHeight(plain(li) + "\u2022\u2003", size, w, lh) + size * 0.35;
    });
    slide.addText(body, {
      x: px(x + indent), y: px(y), w: px(w), h: px(h), isTextBox: true, margin: 0,
      fontFace: baseFamily, color: baseColor, valign: "top", lineSpacingMultiple: lh,
    });
    return h;
  }

  if (tag === "table") {
    const size = sz(num(st["font-size"], 28));
    const cellPad = sz(num(st.padding, 12));
    const rows = el.querySelectorAll("tr");
    const widths = [];
    rows[0].querySelectorAll("th,td").forEach((c) =>
      widths.push((num(styleOf(c).width) || 100 / rows[0].childNodes.length) / 100));
    const body = [];
    let h = 0;
    rows.forEach((tr) => {
      const rowFill = hex(styleOf(tr).background);
      const cells = tr.querySelectorAll("th,td");
      let tallest = 1;
      const out = cells.map((c, i) => {
        const cst = styleOf(c);
        const isHead = c.rawTagName.toLowerCase() === "th";
        const colW = widths[i] * width - 2 * cellPad;
        tallest = Math.max(tallest, Math.ceil(plain(c).length / perLine(colW, size)),
                           plain(c).split("\n").length);
        return { text: runs(c, { color: hex(cst.color) || (isHead ? "FFFFFF" : baseColor),
                                 bold: isHead || cst["font-weight"] === "700",
                                 fontFace: family(cst["font-family"]) || baseFamily }),
                 options: { align: cst["text-align"] || "left",
                            fill: { color: rowFill || "FFFFFF" } } };
      });
      body.push(out);
      h += tallest * size * 1.5 + 2 * cellPad;   // measured against the renderer
    });
    slide.addTable(body, {
      x: px(x), y: px(y), w: px(width), colW: widths.map((f) => px(f * width)),
      fontFace: baseFamily, fontSize: pt(size), margin: pt(cellPad),
      border: { type: "solid", color: "BFBFBF", pt: 0.5 }, valign: "middle", autoPage: false,
    });
    return h;
  }

  if (tag === "div") {
    // A row of cards, or the bit-layout bar: lay the children out side by side.
    const kids = el.childNodes.filter((n) => n.nodeType === 1 &&
      n.rawTagName.toLowerCase() !== "x-connector");
    if (!kids.length) return 0;
    const innerGap = num(st.gap, 24) * scale;
    const fixed = kids.map((k) => num(styleOf(k).width, 0));
    const flexible = fixed.filter((f) => !f).length;
    const spare = width - fixed.reduce((a, b) => a + b, 0) - innerGap * (kids.length - 1);
    let cx = x, h = 0;
    for (const kid of kids) {
      const kst = styleOf(kid);
      const kw = num(kst.width, 0) || spare / flexible;
      const kpad = padding(kst.padding);
      let inner_h = 0;
      for (const sub of kid.childNodes.filter((n) => n.nodeType === 1)) {
        const sst2 = styleOf(sub);
        const ssize = sz(num(sst2["font-size"], 30));
        inner_h += textHeight(plain(sub), ssize, kw - kpad.left - kpad.right,
                              num(sst2["line-height"], 1.3)) + 8;
      }
      inner_h = Math.max(inner_h, num(kst.height, 0)) + kpad.top + kpad.bottom;
      const kfill = hex(kst.background), kborder = kst.border
        ? { color: hex(kst.border.split(/\s+/).find((t) => t.startsWith("#"))) || "000000",
            width: num(kst.border) || 1 }
        : undefined;
      if (kfill || kborder) {
        slide.addShape("rect", { x: px(cx), y: px(y), w: px(kw), h: px(inner_h),
                                 fill: kfill ? { color: kfill } : { type: "none" },
                                 line: kborder });
      }
      let sy = y + kpad.top;
      for (const sub of kid.childNodes.filter((n) => n.nodeType === 1)) {
        const sst2 = styleOf(sub);
        const ssize = sz(num(sst2["font-size"], 30));
        const sh = textHeight(plain(sub), ssize, kw - kpad.left - kpad.right,
                              num(sst2["line-height"], 1.3));
        slide.addText(runs(sub), {
          x: px(cx + kpad.left), y: px(sy), w: px(kw - kpad.left - kpad.right), h: px(sh),
          isTextBox: true, margin: 0, fontFace: family(sst2["font-family"]) || baseFamily,
          fontSize: pt(ssize), bold: sst2["font-weight"] === "700",
          color: hex(sst2.color) || baseColor, align: sst2["text-align"] || "left",
          valign: "top", lineSpacingMultiple: num(sst2["line-height"], 1.3),
        });
        sy += sh + 8;
      }
      h = Math.max(h, inner_h);
      cx += kw + innerGap;
    }
    return h;
  }

  warn(`skipped a <${tag}>`);
  return 0;
}

// ── main ─────────────────────────────────────────────────────────────────────

const out = process.argv[2] || "Passage on QLever.pptx";
const dir = process.argv[3] || path.join(__dirname, "project");
const index = JSON.parse(fs.readFileSync(path.join(dir, "deck.json"), "utf8"));

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE";
pres.title = index.title;

let warnings = 0;
for (const id of index.order) {
  const file = path.join(dir, "slides", `${id}.html`);
  const warn = (m) => { warnings++; console.warn(`  ${id}: ${m}`); };
  addSlide(pres, fs.readFileSync(file, "utf8"), warn);
}

pres.writeFile({ fileName: out }).then(() => {
  console.log(`wrote ${out} — ${index.order.length} slides, ${warnings} warnings`);
});
