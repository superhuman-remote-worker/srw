import { docsSchema } from '@astrojs/starlight/schema';
import { glob } from 'astro/loaders';
import { defineCollection } from 'astro:content';

import { DOCS_EXCLUDE, docsEntryId } from './site.mjs';

// docs/ at the repository root is the single source of truth: the same files
// GitHub renders are loaded here, and nothing is copied into src/content/.
export const collections = {
	docs: defineCollection({
		loader: glob({
			base: '../docs',
			pattern: ['**/*.md', ...DOCS_EXCLUDE.map((p) => `!${p}`)],
			generateId: ({ entry }) => docsEntryId(entry),
		}),
		schema: docsSchema(),
	}),
};
