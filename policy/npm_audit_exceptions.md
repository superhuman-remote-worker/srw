# npm audit exceptions

The `main` and `develop` workflows reject unaccepted high/critical npm
advisories. Exceptions name individual GHSA advisories. An unrelated
high/critical advisory against an accepted package still blocks the gate.

How a package is judged: npm rates a package by the worst advisory in any of
its `via` chains. A package that npm rates high or critical blocks the gate
unless every advisory behind it that is not accepted is rated moderate or
lower. An advisory without a rating, a package without root advisories and a
dependency missing from the report keep npm's rating of the package.

Until 2026-11-17 a package passed only when every advisory behind it was
accepted. That blocked packages on which nothing but moderate advisories was
left: `@angular/cli`, `@angular/pwa` and `@npmcli/run-script` are rated high
through picomatch and also carry two moderate advisories (`glob`
GHSA-3g3m-2fjr-6pw4, `minimatch` GHSA-g3hv-75wg-m249), which the gate has
never blocked on their own.

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

## picomatch: GHSA-fc5q-39xc-vjpf

Accepted on 2026-11-17 for **`picomatch@4.0.5` in Cockpit's build, lint and
test tools**, until a patched release exists. This accepts a limited
exposure; it does not patch picomatch.

- Advisory: <https://github.com/advisories/GHSA-fc5q-39xc-vjpf> (high,
  CVSS 7.5).
- Effect: a crafted pattern (single-branch repeated extglobs) makes matching
  take very long (regular expression denial of service).
- Upstream status at review: every release up to and including 4.0.7 is
  affected, and 4.0.7 is the latest release. No patched version is published.
- Dependency paths: one installed copy, forced by `overrides.picomatch` in
  `cockpit/package.json`. It is used by the Angular CLI and build chain
  (`@angular-devkit/core`, `@angular/build`), Vite, Vitest, `tinyglobby` and
  `fdir`, Micromatch (the Stylelint chain) and `@parcel/watcher`. npm reports
  26 packages as high through it.
- Exposure: the patterns are in the repository (build configuration, test
  and lint globs). Cockpit users cannot supply a pattern. No file under
  `cockpit/src` imports picomatch, and the built browser output holds none
  of picomatch's own strings (a build of 2026-11-13, checked on
  2026-11-17).
- Why the Braces guard does not fit: picomatch is not `dev: true` in the
  lockfile. `@angular/pwa` is listed under `dependencies` and brings in the
  Angular CLI's schematics packages, which need picomatch. `@angular/pwa` is
  a schematic collection used by `ng add`, not at run time.
- Shipping boundary: as for Braces. `docker/Dockerfile.cockpit` builds with
  Node and copies only the static browser output into nginx.
- Enforcement: the workflows activate this exception only while both hold.
  1. **No newer release.** The step asks the registry for the newest
     picomatch (`npm view picomatch version`). A version above 4.0.7 turns
     the exception off, so the gate fails until picomatch is updated. An
     answer that is missing or not a plain release number turns it off too.
  2. **No dependent that was not reviewed.** Outside the packages marked
     `dev: true`, only `@angular-devkit/core`, `tinyglobby` and `fdir` may
     depend on picomatch, and the Cockpit must not depend on it directly. Any
     other dependent turns the exception off until its use of picomatch is
     reviewed and it is added to the list in both workflows.

  A failing run names the reason. Other picomatch advisories and unrelated
  high/critical findings remain blocking.
- Removal: when a patched picomatch is published, update the lockfile (the
  override `^4.0.4` admits any 4.x release), then delete the advisory id, its
  guard in both workflows, the tests for it in
  `tests/test_npm_audit_policy.py` and this section. Condition 1 forces this.
  Any change that exposes pattern processing to users requires a new
  exposure review.

## HTTP cache semantics

`http-cache-semantics` GHSA-ch52-4w7c-c8xp is **not excepted**. Its affected
locked version 4.2.0 is updated to 4.3.0, released on 2026-10-04, within the
existing `make-fetch-happen` dependency range. The npm audit no longer reports
this advisory after the update.

## Existing exceptions

The existing lodash-es and esbuild advisory IDs and their usage rationale
remain documented beside the audit steps in both workflows. This change does
not broaden those exceptions.
