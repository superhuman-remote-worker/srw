# npm audit exceptions

The `main` and `develop` workflows reject unaccepted high/critical npm
advisories. Exceptions name individual GHSA advisories; dependent packages are
accepted only when every root advisory in their `via` chain is accepted.
An unrelated high/critical advisory against an accepted package still blocks
the gate.

## Braces: GHSA-vfj7-8cjw-p6xm

Accepted on 2026-10-04 for **development-only `braces@3.0.3`** in Cockpit's
Stylelint toolchain. This accepts a limited exposure; it does not patch Braces.

- Advisory: <https://github.com/advisories/GHSA-vfj7-8cjw-p6xm>
- Effect: deeply nested brace patterns can exhaust the Node.js call stack.
- Dependency path: `stylelint` (also through `globby` / `fast-glob`) →
  `micromatch` → `braces`. The Stylelint configuration packages inherit the same
  advisory through their dependency on Stylelint.
- Upstream status at review: Braces 3.0.3 and Micromatch 4.0.8 are the latest
  releases; Stylelint 17.16.0 still uses the affected chain. No patched Braces
  version is published.
- Exposure: `cockpit/package.json` runs `stylelint 'src/**/*.scss'`, with a
  repository-controlled pattern. Cockpit users cannot supply patterns to this
  command. Stylelint and Braces are marked `dev: true` in the lockfile.
- Shipping boundary: `docker/Dockerfile.cockpit` builds with Node and copies
  only the static browser output into nginx. These lint dependencies are not
  installed in the final image or imported by application source.
- Enforcement: the workflows activate this exception only when **every**
  installed Braces lockfile entry is exactly version 3.0.3 and `dev: true`.
  A version change or production dependency disables the exception. Other
  Braces advisories and unrelated high/critical findings remain blocking.
- Removal: review on the next Stylelint/Micromatch/Braces dependency update;
  remove the exception once a supported patched chain is available. Any change
  that exposes pattern processing to users requires a new exposure review.

## HTTP cache semantics

`http-cache-semantics` GHSA-ch52-4w7c-c8xp is **not excepted**. Its affected
locked version 4.2.0 is updated to 4.3.0, released on 2026-10-04, within the
existing `make-fetch-happen` dependency range. The npm audit no longer reports
this advisory after the update.

## Existing exceptions

The existing lodash-es and esbuild advisory IDs and their usage rationale
remain documented beside the audit steps in both workflows. This change does
not broaden those exceptions.
