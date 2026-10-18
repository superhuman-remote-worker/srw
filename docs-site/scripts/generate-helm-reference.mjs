#!/usr/bin/env node
// Writes src/generated/helm-values.md, the "Helm values" reference page, from
// the chart's own files:
//
// - helm/values.yaml supplies the keys (in file order), their defaults and
//   their descriptions: the `# -- ` comment above a key, helm-docs style, or an
//   inline comment after its value;
// - helm/values.schema.json supplies types, allowed values, constraints and
//   required keys, and keys the schema accepts that values.yaml leaves unset.
//
// The page has one section per top-level key. Documented keys get their own
// heading (so they can be linked); undocumented keys of a section are listed in
// compact tables. Undocumented maps whose children carry no descriptions of
// their own (resources, selectors, ...) are shown as one entry with their YAML.
//
// Run by `npm run build` and `npm run dev` (see package.json). The output is
// gitignored; nothing here edits the chart.

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';

const SITE_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const REPO_ROOT = path.resolve(SITE_DIR, '..');
const VALUES_PATH = path.join(REPO_ROOT, 'helm/values.yaml');
const SCHEMA_PATH = path.join(REPO_ROOT, 'helm/values.schema.json');
const OUTPUT_PATH = path.join(SITE_DIR, 'src/generated/helm-values.md');
const REPO_URL = 'https://github.com/superhuman-remote-worker/srw';

/** Undocumented maps up to this many YAML lines are shown as a single entry. */
const COLLAPSE_MAX_LINES = 12;
/** Inline defaults longer than this are shown as a YAML block instead. */
const INLINE_DEFAULT_MAX = 60;

const values = YAML.parseDocument(readFileSync(VALUES_PATH, 'utf8'));
if (values.errors.length > 0) {
	throw new Error(`helm/values.yaml does not parse: ${values.errors[0].message}`);
}
if (!YAML.isMap(values.contents)) throw new Error('helm/values.yaml is not a mapping');
const schema = JSON.parse(readFileSync(SCHEMA_PATH, 'utf8'));

// ---------------------------------------------------------------------------
// Schema lookup

function resolveRef(node) {
	let current = node;
	const seen = new Set();
	while (current && typeof current === 'object' && current.$ref) {
		if (seen.has(current.$ref)) break;
		seen.add(current.$ref);
		const match = /^#\/(definitions|\$defs)\/(.+)$/.exec(current.$ref);
		const target = match ? schema[match[1]]?.[match[2]] : undefined;
		if (!target) break;
		const { $ref, ...rest } = current;
		current = { ...target, ...rest };
	}
	return current;
}

/** Schema node for `key` inside `parent` (both resolved), or undefined. */
function childSchema(parent, key) {
	if (!parent) return undefined;
	const declared = parent.properties?.[key];
	if (declared) return resolveRef(declared);
	if (parent.additionalProperties && typeof parent.additionalProperties === 'object') {
		return resolveRef(parent.additionalProperties);
	}
	return undefined;
}

function isRequired(parent, key) {
	return Array.isArray(parent?.required) && parent.required.includes(key);
}

// ---------------------------------------------------------------------------
// Comments

// `# -- text` starts a description; an indented `--` inside one is a dash.
const MARKER = /^ ?--(\s|$)/;

/** The helm-docs description (`# -- ...`) at the end of a comment block. */
function helmDocsDescription(comment) {
	if (!comment) return undefined;
	const lines = comment.split('\n');
	let start = -1;
	for (let i = lines.length - 1; i >= 0; i--) {
		if (MARKER.test(lines[i])) {
			start = i;
			break;
		}
	}
	if (start === -1) return undefined;
	const block = lines.slice(start);
	block[0] = block[0].replace(MARKER, '');
	return block;
}

/** Comment lines (without `#`) → Markdown paragraphs and code blocks. */
function commentToMarkdown(lines) {
	// yaml strips the `#`; the comment text keeps its leading spaces.
	const normalised = lines.map((line) => line.replace(/\s+$/, ''));
	const first = normalised[0].trim();
	const rest = normalised.slice(1);
	const indents = rest.filter((l) => l.trim()).map((l) => l.match(/^ */)[0].length);
	const base = indents.length > 0 ? Math.min(...indents) : 0;

	const blocks = [];
	// Paragraph items: { bullet, text } — wrapped lines are joined into one item.
	let items = first ? [{ bullet: false, text: first }] : [];
	let codeLines = null;
	const flushParagraph = () => {
		if (items.length > 0) blocks.push(formatItems(items));
		items = [];
	};
	const flushCode = () => {
		if (codeLines) blocks.push(['```text', ...codeLines, '```'].join('\n'));
		codeLines = null;
	};
	for (const raw of rest) {
		if (!raw.trim()) {
			flushParagraph();
			flushCode();
			continue;
		}
		const indent = raw.match(/^ */)[0].length - base;
		const text = raw.trim();
		const bullet = /^[-*] (.*)$/.exec(text);
		const inBullet = items.length > 0 && items.at(-1).bullet;
		if (bullet) {
			flushCode();
			items.push({ bullet: true, text: bullet[1] });
		} else if (indent >= 2 && !inBullet) {
			// Indented lines are commands or YAML examples.
			flushParagraph();
			codeLines ??= [];
			codeLines.push(raw.slice(base + 2));
		} else if (items.length > 0 && !codeLines) {
			items.at(-1).text += ` ${text}`;
		} else {
			flushCode();
			items.push({ bullet: false, text });
		}
	}
	flushParagraph();
	flushCode();
	return blocks.join('\n\n');
}

/** Paragraph items → Markdown; a list needs a blank line after text. */
function formatItems(items) {
	return items
		.map((item, i) => {
			const text = escapeInline(item.text);
			if (!item.bullet) return text;
			return `${i > 0 && !items[i - 1].bullet ? '\n' : ''}- ${text}`;
		})
		.join('\n');
}

/**
 * Comments are plain text, not Markdown: escape what Markdown or HTML would
 * interpret, except inside `code spans`, which the comments use deliberately.
 */
function escapeInline(text) {
	return text
		.split(/(`[^`]*`)/)
		.map((part, i) =>
			i % 2 === 1
				? part
				: part
						.replace(/\\/g, '\\\\')
						.replace(/&/g, '&amp;')
						.replace(/</g, '&lt;')
						.replace(/>/g, '&gt;')
						.replace(/([*_[\]|])/g, '\\$1')
						.replace(/^#/, '\\#'),
		)
		.join('');
}

// ---------------------------------------------------------------------------
// Rendering helpers

function code(text) {
	const value = String(text);
	const fence = value.includes('`') ? '``' : '`';
	const pad = value.startsWith('`') || value.endsWith('`') ? ' ' : '';
	return `${fence}${pad}${value}${pad}${fence}`;
}

function inferType(node) {
	if (YAML.isMap(node)) return 'object';
	if (YAML.isSeq(node)) return 'array';
	const value = YAML.isScalar(node) ? node.value : node;
	if (value === null || value === undefined) return 'null';
	if (typeof value === 'boolean') return 'boolean';
	if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'number';
	return 'string';
}

function typeLabel(node, schemaNode) {
	const declared = schemaNode?.type;
	let type = Array.isArray(declared) ? declared.join(' | ') : declared;
	if (!type && Array.isArray(schemaNode?.oneOf)) {
		type = schemaNode.oneOf.map((s) => resolveRef(s)?.type).filter(Boolean).join(' | ');
	}
	type ||= inferType(node);
	const items = resolveRef(schemaNode?.items);
	if (type === 'array' && items?.type) type = `array of ${[].concat(items.type).join(' | ')}`;
	return type;
}

function constraints(schemaNode) {
	if (!schemaNode) return [];
	const out = [];
	if (Array.isArray(schemaNode.enum)) out.push(`one of ${schemaNode.enum.map((v) => code(JSON.stringify(v))).join(', ')}`);
	if ('const' in schemaNode) out.push(`must be ${code(JSON.stringify(schemaNode.const))}`);
	const min = schemaNode.minimum ?? (schemaNode.exclusiveMinimum !== undefined ? `> ${schemaNode.exclusiveMinimum}` : undefined);
	if (min !== undefined && schemaNode.maximum !== undefined) out.push(`range ${min}–${schemaNode.maximum}`);
	else if (schemaNode.minimum !== undefined) out.push(`minimum ${schemaNode.minimum}`);
	else if (schemaNode.exclusiveMinimum !== undefined) out.push(`greater than ${schemaNode.exclusiveMinimum}`);
	else if (schemaNode.maximum !== undefined) out.push(`maximum ${schemaNode.maximum}`);
	if (schemaNode.minLength === 1) out.push('not empty');
	else if (schemaNode.minLength !== undefined) out.push(`at least ${schemaNode.minLength} characters`);
	if (schemaNode.maxLength !== undefined) out.push(`at most ${schemaNode.maxLength} characters`);
	if (schemaNode.pattern) out.push(`pattern ${code(schemaNode.pattern)}`);
	if (schemaNode.minItems !== undefined) out.push(`at least ${schemaNode.minItems} items`);
	if (schemaNode.maxItems !== undefined) out.push(`at most ${schemaNode.maxItems} items`);
	if (schemaNode.uniqueItems) out.push('unique items');
	const items = resolveRef(schemaNode.items);
	if (Array.isArray(items?.enum)) out.push(`items one of ${items.enum.map((v) => code(JSON.stringify(v))).join(', ')}`);
	return out;
}

function yamlOf(value) {
	return new YAML.Document(value).toString({ lineWidth: 0 }).trimEnd();
}

/** `{ inline }` for short defaults, `{ block }` (YAML) for long or structured ones. */
function defaultOf(node) {
	if (node === undefined) return { inline: '—' };
	const value = YAML.isNode(node) ? node.toJSON() : node;
	if (value === null || typeof value !== 'object') {
		const text = JSON.stringify(value);
		return text.length <= INLINE_DEFAULT_MAX ? { inline: code(text) } : { block: yamlOf(value) };
	}
	const flow = JSON.stringify(value);
	if (flow.length <= INLINE_DEFAULT_MAX && !flow.includes('`')) return { inline: code(flow) };
	return { block: yamlOf(value) };
}

function hasDescriptions(node) {
	if (!YAML.isMap(node)) return false;
	return node.items.some(
		(pair, i) =>
			helmDocsDescription(i === 0 ? node.commentBefore ?? pair.key.commentBefore : pair.key.commentBefore) ||
			pair.value?.comment ||
			hasDescriptions(pair.value),
	);
}

function schemaHasDetail(schemaNode) {
	if (!schemaNode?.properties) return false;
	return Object.values(schemaNode.properties).some((child) => {
		const resolved = resolveRef(child);
		return resolved.description || resolved.enum || 'const' in resolved || schemaHasDetail(resolved);
	});
}

// ---------------------------------------------------------------------------
// Tree → entries

/**
 * @typedef {{ path: string, node: any, schema: any, required: boolean, description?: string,
 *   group?: boolean, unset?: boolean }} Entry
 *   `group`: a map whose children are listed after it; `unset`: a schema-only key.
 */

/** Flattens a values map into entries in file order, then schema-only keys. */
function collect(map, prefix, parentSchema, out) {
	const seen = new Set();
	map.items.forEach((pair, index) => {
		const key = String(pair.key?.value ?? pair.key);
		seen.add(key);
		const keyPath = prefix ? `${prefix}.${key}` : key;
		const comment = index === 0 ? map.commentBefore ?? pair.key.commentBefore : pair.key.commentBefore;
		const lines = helmDocsDescription(comment);
		let description = lines ? commentToMarkdown(lines) : undefined;
		if (!description && pair.value?.comment) {
			description = escapeInline(pair.value.comment.trim().replace(/\s{2,}/g, ' '));
		}
		const schemaNode = childSchema(parentSchema, key);
		if (!description && schemaNode?.description) description = escapeInline(schemaNode.description);
		const required = isRequired(parentSchema, key);
		const node = pair.value;

		const recurse =
			YAML.isMap(node) &&
			node.items.length > 0 &&
			(hasDescriptions(node) || schemaHasDetail(schemaNode) || yamlOf(node.toJSON()).split('\n').length > COLLAPSE_MAX_LINES);
		if (recurse) {
			if (description || required) out.push({ path: keyPath, node, schema: schemaNode, required, description, group: true });
			collect(node, keyPath, schemaNode, out);
		} else {
			out.push({ path: keyPath, node, schema: schemaNode, required, description });
		}
	});
	collectSchemaOnly(parentSchema, prefix, seen, out);
}

/** Keys the schema declares but values.yaml does not set. */
function collectSchemaOnly(parentSchema, prefix, seen, out) {
	for (const [key, raw] of Object.entries(parentSchema?.properties ?? {})) {
		if (seen.has(key)) continue;
		const schemaNode = resolveRef(raw);
		const keyPath = prefix ? `${prefix}.${key}` : key;
		const required = isRequired(parentSchema, key);
		const description = schemaNode.description ? escapeInline(schemaNode.description) : undefined;
		if (schemaNode.properties && Object.keys(schemaNode.properties).length > 0) {
			if (description || required) out.push({ path: keyPath, node: undefined, schema: schemaNode, required, description, group: true, unset: true });
			collectSchemaOnly(schemaNode, keyPath, new Set(), out);
		} else {
			out.push({ path: keyPath, node: schemaNode.default, schema: schemaNode, required, description, unset: schemaNode.default === undefined });
		}
	}
}

// ---------------------------------------------------------------------------
// Entries → Markdown

function metaLine(entry) {
	const parts = [`**Type** ${code(typeLabel(entry.node, entry.schema))}`];
	if (entry.required) parts.push('**required**');
	if (!entry.group) {
		const def = entry.unset ? { inline: 'not set' } : defaultOf(entry.node);
		if (def.inline) parts.push(`**Default** ${def.inline}`);
	}
	const rules = constraints(entry.schema);
	if (rules.length > 0) parts.push(rules.join('; '));
	return parts.join(' · ');
}

function renderDocumented(entry) {
	const lines = [`### ${escapeInline(entry.path)}`, '', metaLine(entry), ''];
	if (!entry.group && !entry.unset) {
		const def = defaultOf(entry.node);
		if (def.block) lines.push('Default:', '', '```yaml', def.block, '```', '');
	}
	if (entry.description) lines.push(entry.description, '');
	return lines.join('\n');
}

function renderTable(entries) {
	const rows = entries.map((entry) => {
		const def = entry.unset ? { inline: 'not set' } : defaultOf(entry.node);
		const shownDefault = def.inline ?? code(JSON.stringify(YAML.isNode(entry.node) ? entry.node.toJSON() : entry.node));
		const notes = [entry.required ? 'required' : '', ...constraints(entry.schema)].filter(Boolean).join('; ');
		return `| ${code(entry.path)} | ${code(typeLabel(entry.node, entry.schema))} | ${shownDefault.replace(/\|/g, '\\|')} | ${notes.replace(/\|/g, '\\|')} |`;
	});
	return ['| Key | Type | Default | Constraints |', '| --- | --- | --- | --- |', ...rows, ''].join('\n');
}

function renderSection(title, entries) {
	const out = [`## ${title}`, ''];
	let pending = [];
	const flush = () => {
		if (pending.length > 0) out.push(renderTable(pending));
		pending = [];
	};
	for (const entry of entries) {
		if (entry.group && !entry.path.includes('.')) {
			// The section's own key: its facts go under the section heading.
			out.push(metaLine(entry), '');
			if (entry.description) out.push(entry.description, '');
			continue;
		}
		const longDefault = !entry.unset && !entry.group && defaultOf(entry.node).block;
		if (entry.description || entry.group || longDefault) {
			flush();
			out.push(renderDocumented(entry));
		} else {
			pending.push(entry);
		}
	}
	flush();
	return out.join('\n');
}

const top = [];
collect(values.contents, '', schema, top);

const general = [];
const sections = new Map();
for (const entry of top) {
	const [head] = entry.path.split('.');
	const isTopLevelLeaf = entry.path === head && !entry.group;
	const topNode = values.contents.get(head, true);
	const isSection = YAML.isMap(topNode) ? topNode.items.length > 0 : Boolean(resolveRef(schema.properties?.[head])?.properties);
	if (isTopLevelLeaf && !isSection) {
		general.push(entry);
		continue;
	}
	if (!sections.has(head)) sections.set(head, []);
	sections.get(head).push(entry);
}

const documented = top.filter((e) => e.description).length;
const body = [
	'---',
	'title: Helm values',
	'description: Every value of the SRW Helm chart with its type, default and description, generated from the chart.',
	'tableOfContents:',
	'  minHeadingLevel: 2',
	'  maxHeadingLevel: 2',
	'---',
	'',
	`This reference is generated from the chart on every docs build: keys, defaults and descriptions come from [\`helm/values.yaml\`](${REPO_URL}/blob/main/helm/values.yaml), and types, allowed values and required keys from [\`helm/values.schema.json\`](${REPO_URL}/blob/main/helm/values.schema.json). It lists ${top.length} entries, ${documented} of them with a description.`,
	'',
	'Override values with your own file (`helm upgrade --install srw <chart> -f my-values.yaml`) rather than editing the chart. The schema covers only part of the chart, and Helm ignores a misspelt key it does not check, so compare spellings with this page. The [Helm chart guide](' +
		`${REPO_URL}/blob/main/helm/README.md` +
		') explains installation and the larger configuration choices.',
	'',
	general.length > 0 ? renderSection('General', general) : '',
	...[...sections].map(([head, entries]) => renderSection(escapeInline(head), entries)),
	'',
].join('\n');

mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
writeFileSync(OUTPUT_PATH, body);
console.log(
	`helm values reference: ${top.length} entries (${documented} documented) in ${sections.size + (general.length > 0 ? 1 : 0)} sections → ${path.relative(SITE_DIR, OUTPUT_PATH)}`,
);
