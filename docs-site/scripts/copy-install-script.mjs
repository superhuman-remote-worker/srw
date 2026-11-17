#!/usr/bin/env node
// Copies scripts/install-cli.sh to public/install.sh, so that the installer
// of the srw command-line client is served at
// https://docs.superhuman-remote-worker.com/install.sh. The address then does
// not depend on a file path in the repository.
//
// Run by `npm run build` and `npm run dev` (see package.json). The copy is
// gitignored; the script in scripts/ is the source.

import { copyFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const SITE_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SOURCE = path.resolve(SITE_DIR, '..', 'scripts/install-cli.sh');
const TARGET = path.join(SITE_DIR, 'public/install.sh');

copyFileSync(SOURCE, TARGET);
console.log(`copied ${path.relative(SITE_DIR, SOURCE)} to ${path.relative(SITE_DIR, TARGET)}`);
