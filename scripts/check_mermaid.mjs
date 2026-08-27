// Validate every Mermaid diagram with Mermaid itself.
//
// MkDocs does not parse Mermaid, so a malformed diagram builds green and becomes
// an error box in the reader's browser. Guessing the grammar does not work: a
// hand-written heuristic flagged 27 diagrams here, of which 24 were fine -- commas
// inside square-bracket labels are legal, whatever intuition says. Asking the real
// parser gave 3, and all 3 were genuine.
//
//   npm install --no-save mermaid jsdom
//   node scripts/check_mermaid.mjs [--strict]
//
// Optional tooling, so it is a separate make target rather than part of `make check`.

import { readFileSync, readdirSync, statSync } from 'fs';
import { join, dirname, relative } from 'path';
import { fileURLToPath } from 'url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
// design/ is scanned too: a chapter can be regenerated from its spec, so a
// broken diagram left there comes back. Most specs use a bare ``` fence rather
// than ```mermaid, which is exactly why a tag-keyed scan misses them.
const TARGETS = [join(ROOT, 'docs'), join(ROOT, 'design')];

let JSDOM, mermaid;
try {
  ({ JSDOM } = await import('jsdom'));
  const dom = new JSDOM('<!doctype html><html><body></body></html>');
  global.window = dom.window;
  global.document = dom.window.document;
  mermaid = (await import('mermaid')).default;
} catch {
  console.log('mermaid/jsdom not installed. Run:');
  console.log('  npm install --no-save mermaid jsdom');
  process.exit(0);
}

mermaid.initialize({ startOnLoad: false, securityLevel: 'loose' });

function walk(dir) {
  return readdirSync(dir).flatMap((f) => {
    const p = join(dir, f);
    return statSync(p).isDirectory() ? walk(p) : p.endsWith('.md') ? [p] : [];
  });
}

const BLOCK = /^```(\w*)\n([\s\S]*?)^```/gm;
const KINDS = ['flowchart', 'graph', 'sequenceDiagram', 'classDiagram',
               'stateDiagram', 'erDiagram', 'gantt', 'journey'];

const looksLikeDiagram = (body) => {
  const first = body.split('\n').find((l) => l.trim())?.trim() ?? '';
  return KINDS.some((k) => first.startsWith(k));
};

let total = 0;
let bad = 0;

const files = TARGETS.filter((d) => { try { statSync(d); return true; } catch { return false; } })
                     .flatMap(walk).sort();

for (const file of files) {
  const text = readFileSync(file, 'utf8');
  BLOCK.lastIndex = 0;
  let m;
  while ((m = BLOCK.exec(text)) !== null) {
    const [lang, body] = [m[1], m[2]];
    if (lang !== 'mermaid' && !looksLikeDiagram(body)) continue;
    total++;
    const line = text.slice(0, m.index).split('\n').length;
    try {
      await mermaid.parse(body);
    } catch (e) {
      bad++;
      const msg = String(e?.message ?? e).split('\n').slice(0, 3).join(' | ');
      console.log(`FAIL ${relative(ROOT, file)}:${line}`);
      console.log(`     ${msg}\n`);
    }
  }
}

console.log(`${total} diagrams parsed by mermaid, ${bad} fail to render.`);
if (bad) {
  console.log('\nA common cause: `;` is a statement separator inside');
  console.log('sequenceDiagram, so it truncates message and Note text.');
}
process.exit(bad && process.argv.includes('--strict') ? 1 : 0);
