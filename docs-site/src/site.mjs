// Shared facts about the docs site, used by astro.config.mjs, the content
// collection and the remark plugin so that page ids and link routes agree.

import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const SITE_URL = 'https://docs.superhuman-remote-worker.com';
export const WEBSITE_URL = 'https://superhuman-remote-worker.com';
export const REPO_URL = 'https://github.com/superhuman-remote-worker/srw';
/** Branch that links to repository files point at. */
export const LINK_BRANCH = 'main';
/** Branch that "Edit page" opens, where documentation changes land first. */
export const EDIT_BRANCH = 'develop';

export const SITE_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const REPO_ROOT = path.resolve(SITE_DIR, '..');
export const DOCS_DIR = path.join(REPO_ROOT, 'docs');
export const GENERATED_DIR = path.join(SITE_DIR, 'src/generated');

/**
 * Files under docs/ that are not site pages: the GitHub index (the site has
 * its own landing page) and the asset sources.
 */
export const DOCS_EXCLUDE = ['README.md', 'assets/**'];

function isExcludedDocsPath(relativePath) {
	return DOCS_EXCLUDE.some((pattern) =>
		pattern.endsWith('/**')
			? relativePath.startsWith(pattern.slice(0, -2))
			: relativePath === pattern,
	);
}

/**
 * Site route of a Markdown file, or undefined when the file is not a page.
 * docs/README.md is the GitHub index and maps to the site's landing page; the
 * generated references live under /reference/.
 */
export function routeForFile(absolutePath) {
	if (!/\.(md|markdown|mdx)$/i.test(absolutePath)) return undefined;
	const fromDocs = path.relative(DOCS_DIR, absolutePath);
	if (!fromDocs.startsWith('..') && !path.isAbsolute(fromDocs)) {
		const relative = fromDocs.split(path.sep).join('/');
		if (relative === 'README.md') return '/';
		if (isExcludedDocsPath(relative)) return undefined;
		return `/${docsEntryId(relative)}/`;
	}
	const fromGenerated = path.relative(GENERATED_DIR, absolutePath);
	if (!fromGenerated.startsWith('..') && !path.isAbsolute(fromGenerated)) {
		return `/reference/${docsEntryId(fromGenerated.split(path.sep).join('/'))}/`;
	}
	return undefined;
}

/** Lowercase, URL-safe form of one path segment. */
export function slugifySegment(segment) {
	return segment
		.toLowerCase()
		.replace(/[^a-z0-9._-]+/g, '-')
		.replace(/^-+|-+$/g, '');
}

/** Collection id (and therefore route) of a file path relative to docs/. */
export function docsEntryId(relativePath) {
	return relativePath
		.replace(/\.(md|markdown|mdx)$/i, '')
		.split('/')
		.map(slugifySegment)
		.join('/')
		.replace(/(^|\/)index$/, '');
}
