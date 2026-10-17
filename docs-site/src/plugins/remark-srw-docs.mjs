// Makes the Markdown under docs/ read the same on the site as on GitHub.
//
// The pages are written for GitHub: they open with a `# Title` H1 and link to
// each other by filename (`[Models](models.md)`) and to the rest of the
// repository by relative path (`../helm/values.yaml`). On the site this
// plugin:
//
// 1. drops the leading H1, because Starlight already renders the frontmatter
//    `title` as the page heading;
// 2. rewrites links to other pages (`page.md#anchor`) to their site routes;
// 3. rewrites links that leave the content directories, or point at a file
//    there that is not a page, to the file on GitHub, and records any that
//    name a path missing from the repository (srwDocsIntegration fails the
//    build on them);
// 4. tells starlight-links-validator which page a file is. The validator
//    derives page ids from paths under src/content/docs/, but this site's
//    content lives in ../docs, so without the hint every cross-page link
//    would fail validation.
//
// Images are left alone: Astro resolves `![…](assets/x.png)` relative to the
// Markdown file itself.

import { existsSync, statSync } from 'node:fs';
import path from 'node:path';

/** Links whose target is missing from the repository, collected per build. */
export function brokenRepoLinks() {
	globalThis.__srwDocsBrokenRepoLinks ??= new Map();
	return globalThis.__srwDocsBrokenRepoLinks;
}

/**
 * @param {object} options
 * @param {string} options.repoRoot absolute path of the repository root
 * @param {string} options.repoUrl e.g. https://github.com/org/repo
 * @param {string} options.branch branch that repository links point at
 * @param {string[]} options.contentDirs absolute directories this plugin transforms
 * @param {(absolutePath: string) => string | undefined} options.routeForFile
 *   site route of a Markdown file, or undefined when it is not a page
 */
export function remarkSrwDocs(options) {
	const { repoRoot, repoUrl, branch, contentDirs, routeForFile } = options;

	const githubUrl = (target) => {
		const relative = path.relative(repoRoot, target).split(path.sep).join('/');
		const kind = existsSync(target) && statSync(target).isDirectory() ? 'tree' : 'blob';
		const encoded = relative.split('/').map(encodeURIComponent).join('/');
		return `${repoUrl}/${kind}/${branch}/${encoded}`;
	};

	const rewrite = (url, source) => {
		if (!url || url.startsWith('#') || url.startsWith('/') || /^[a-z][a-z0-9+.-]*:/i.test(url)) {
			return undefined; // same-page anchor, site path, or absolute URL
		}
		const hashIndex = url.indexOf('#');
		const pathPart = (hashIndex === -1 ? url : url.slice(0, hashIndex)).split('?')[0];
		const hash = hashIndex === -1 ? '' : url.slice(hashIndex);
		if (!pathPart) return undefined;

		let decoded = pathPart;
		try {
			decoded = decodeURI(pathPart);
		} catch {
			// keep the raw path
		}
		const target = path.resolve(path.dirname(source), decoded);

		const route = routeForFile(target);
		if (route && existsSync(target)) return route + hash;

		if (!isInside(target, repoRoot)) return undefined; // left for the validator to report
		if (!existsSync(target)) {
			const list = brokenRepoLinks().get(source) ?? [];
			list.push(url);
			brokenRepoLinks().set(source, list);
		}
		return githubUrl(target) + hash;
	};

	return function transformer(tree, file) {
		const source = file.path ?? file.history?.[0];
		if (!source || !contentDirs.some((dir) => isInside(source, dir))) return;

		stripLeadingH1(tree);

		walk(tree, (node) => {
			if (node.type !== 'link' && node.type !== 'definition') return;
			const rewritten = rewrite(node.url, source);
			if (rewritten !== undefined) node.url = rewritten;
		});

		// The validator keys a page by `frontmatter.slug` when one is set. It reads
		// the render-time frontmatter, so setting it here changes no route:
		// Starlight builds routes from the collection entry id.
		const route = routeForFile(source);
		if (route && route !== '/') {
			file.data.astro ??= {};
			file.data.astro.frontmatter ??= {};
			file.data.astro.frontmatter.slug ??= route.replace(/^\/|\/$/g, '');
		}
	};
}

/**
 * Fails `astro build` when a page links to a repository path that does not exist.
 * @param {{ repoRoot: string }} options
 */
export function srwDocsIntegration({ repoRoot }) {
	return {
		name: 'srw-docs-repo-links',
		hooks: {
			'astro:config:setup': () => brokenRepoLinks().clear(),
			'astro:build:done': ({ logger }) => {
				const broken = brokenRepoLinks();
				if (broken.size === 0) return;
				for (const [file, urls] of broken) {
					const shown = path.relative(repoRoot, file);
					logger.error(`${shown}: link target not found in the repository: ${urls.join(', ')}`);
				}
				throw new Error('Links to missing repository files (see above).');
			},
		},
	};
}

function stripLeadingH1(tree) {
	const index = tree.children.findIndex((node) => node.type !== 'yaml' && node.type !== 'toml');
	const first = tree.children[index];
	if (first?.type === 'heading' && first.depth === 1) tree.children.splice(index, 1);
}

function walk(node, visit) {
	visit(node);
	if (Array.isArray(node.children)) for (const child of node.children) walk(child, visit);
}

function isInside(file, dir) {
	const relative = path.relative(dir, file);
	return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}
