// @ts-check
import { unified } from '@astrojs/markdown-remark';
import starlight from '@astrojs/starlight';
import { defineConfig, passthroughImageService } from 'astro/config';
import starlightLinksValidator from 'starlight-links-validator';

import {
	DOCS_DIR,
	EDIT_BRANCH,
	GENERATED_DIR,
	LINK_BRANCH,
	REPO_ROOT,
	REPO_URL,
	SITE_DIR,
	SITE_URL,
	routeForFile,
} from './src/site.mjs';
import { remarkSrwDocs, srwDocsIntegration } from './src/plugins/remark-srw-docs.mjs';

export default defineConfig({
	site: SITE_URL,
	trailingSlash: 'always',
	// Images are copied as they are; the docs have few, and this keeps the
	// build free of native image tooling.
	image: { service: passthroughImageService() },
	markdown: {
		processor: unified({
			// Render the same characters GitHub does (no curly quotes or dashes).
			smartypants: false,
			remarkPlugins: [
				[
					remarkSrwDocs,
					{
						repoRoot: REPO_ROOT,
						repoUrl: REPO_URL,
						branch: LINK_BRANCH,
						contentDirs: [DOCS_DIR, GENERATED_DIR],
						routeForFile,
					},
				],
			],
		}),
	},
	vite: {
		// The content lives outside this project; let the dev server read it.
		server: { fs: { allow: [SITE_DIR, DOCS_DIR, `${REPO_ROOT}/cockpit/public`] } },
	},
	integrations: [
		starlight({
			title: 'SRW docs',
			description:
				'Documentation for Superhuman Remote Worker, a self-hosted runtime for AI work on Kubernetes.',
			logo: { src: '../cockpit/public/favicon.svg', alt: 'SRW' },
			favicon: '/favicon.svg',
			// One root locale today; a German locale can be added under docs/de/.
			defaultLocale: 'root',
			locales: { root: { label: 'English', lang: 'en' } },
			social: [
				{ icon: 'github', label: 'GitHub', href: REPO_URL },
			],
			editLink: {
				// Entry paths are relative to docs-site/, e.g. ../docs/concepts.md.
				baseUrl: `${REPO_URL}/edit/${EDIT_BRANCH}/docs-site/`,
			},
			markdown: { processedDirs: ['../docs', './src/generated'] },
			customCss: ['./src/styles/custom.css'],
			components: {
				Footer: './src/components/Footer.astro',
				SocialIcons: './src/components/SocialIcons.astro',
			},
			sidebar: [
				{ label: 'Get started', slug: 'getting-started' },
				{ label: 'Concepts', slug: 'concepts' },
				{
					label: 'Guides',
					items: [
						'models',
						'local-kubernetes',
						'k3s-host-preparation',
						'connector-drivers',
						'cloud-folders',
						'voice-gateway',
						'workspace-images',
						{
							label: 'SSH access',
							link: `${REPO_URL}/blob/${LINK_BRANCH}/ssh-access.md`,
							attrs: { target: '_blank', rel: 'noopener' },
						},
						'vm-capacity',
						'vm-workspace-recovery',
						'project-loops',
					],
				},
				{
					label: 'Operations',
					items: [
						'versioning',
						{
							label: 'Release notes',
							link: `${REPO_URL}/releases`,
							attrs: { target: '_blank', rel: 'noopener' },
						},
						'security-model',
						'troubleshooting',
					],
				},
				{
					label: 'Reference',
					items: [
						'architecture',
						{ label: 'Helm values', link: '/reference/helm-values/' },
						{
							label: 'Helm chart guide',
							link: `${REPO_URL}/blob/${LINK_BRANCH}/helm/README.md`,
							attrs: { target: '_blank', rel: 'noopener' },
						},
						{
							label: 'Expert configuration',
							link: `${REPO_URL}/blob/${LINK_BRANCH}/config/README.md`,
							attrs: { target: '_blank', rel: 'noopener' },
						},
					],
				},
				{ label: 'Development', slug: 'development' },
			],
			plugins: [
				starlightLinksValidator({
					// The local-Kubernetes guide links to services on localhost.
					errorOnLocalLinks: false,
					// The landing page is a custom page; the validator cannot read its headings.
					exclude: ['/'],
				}),
			],
		}),
		srwDocsIntegration({ repoRoot: REPO_ROOT }),
	],
});
