---
title: Versioning and upgrades
description: What an SRW version number tells an operator, how to pin a release, how to upgrade safely, and how maintainers cut a release.
---

# Versioning and upgrades

SRW ships as one Helm chart. The chart version, its `appVersion`, the git tag,
the GitHub Release and the `vX.Y.Z` tag on every component image all carry the
same number. This page explains what that number tells you before you
upgrade.

## What the version tells you

- **`v0.0.x`** is the alpha history: internal deploy cuts, with no release
  notes.
- **`v0.1.0`** is the first public release. From 0.1 until 1.0, SRW is in
  public beta. The release title and the README badge say so. The version
  string never does: it carries no `-beta` or `-rc` suffix, because Helm,
  Fleet and Renovate hide pre-release versions by default.

Before 1.0, the version follows two rules:

| Change | Example | What you do |
|---|---|---|
| **MINOR** (`0.1.4` → `0.2.0`) | The release contains at least one change that needs operator action: a values key renamed or removed, a new required Secret key, a database migration that cannot run during a rolling update, an API removed, or a visible default changed. | Read the release notes' **Operator action required** section and do what it says before you upgrade. |
| **PATCH** (`0.1.3` → `0.1.4`) | Everything else, **features included**. | Run `helm upgrade` with your existing values. |

Every merge to `main` is a release, so releases are frequent and most of them
are patches.

## Pinning a version

The chart is `oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker`.
`helm install` or `helm upgrade` without `--version` installs the newest
release. Always pass a version:

- **Exact pin**: `--version 0.1.3`. Recommended for production. You upgrade
  when you decide to.
- **Patch range**: `--version "~0.1.0"` means `>= 0.1.0, < 0.2.0`. You get
  every drop-in release of the 0.1 line, and never one that needs operator
  action. Helm resolves the range against the registry itself, and a Fleet
  `helm.version` accepts the same constraint.

The release chart pins every component image to the release commit's
`sha-<commit>` tag, so a given chart version always runs the same images.
The images' `latest` tags move with every release. Do not deploy them in
production.

A chart installed from a git checkout (`helm install srw ./helm`) is not a
release. It carries the placeholder version `0.0.0-local`, and its image
defaults are the moving `latest` tags. Pin `image.*.tag` and
`vmController.defaultVmImage` yourself, or install the published chart.

## Two charts: release and dev

| Chart | Published | Version | Use it for |
|---|---|---|---|
| `charts/superhuman-remote-worker` | on every merge to `main` | `X.Y.Z` under the rules on this page, with a GitHub Release and notes | Production and evaluation |
| `charts/srw-dev` | on pushes to `develop` | `0.0.<CI run number>`, with the commit SHA as `appVersion` | The maintainers' dev cluster |

`srw-dev` is **not for production**. Its `0.0.N` numbers are CI run numbers.
They are not comparable with release versions and come with no upgrade
promise. Do not move an installation between the two charts: database
migrations only run forward, and a dev chart is usually ahead of the newest
release.

## Before you upgrade

1. Read the GitHub Release of **every** version between yours and the
   target. Each one lists only its own changes, so an operator action can
   hide in a release you are skipping. Check its **Operator action required**
   section first.
2. Back up the databases before a MINOR upgrade. There are no down
   migrations, so you cannot roll back past a release that changed the
   schema. Fix forward instead.
3. Upgrade through each minor line in turn (`0.1.x` → `0.2.x` → `0.3.x`). You
   can skip patch releases within a line. Nothing tests a skip across minor
   lines yet.
4. Upgrade:

   ```bash
   helm upgrade srw oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker \
     --version <new-version> -n srw -f my-values.yaml
   ```

   The [chart guide](../helm/README.md#upgrade) covers upgrade details and
   one-time cutovers.

## What 1.0 will mean

Version 1.0 will come with a written stability promise, which does not exist
yet:

- The values schema stays compatible across all 1.x releases.
- A deprecated value or API keeps working for at least one minor release
  before it is removed.
- CI tests the upgrade from the previous release to the new one for every
  release.
- A stated support window for older minor lines.

From 1.0 on, SRW uses ordinary SemVer: operator action bumps the MAJOR
version, a new feature the MINOR, and anything else the PATCH.

## For maintainers

### How a release is cut

Merging `develop` into `main` is a release. `main.yml` runs every gate and
then:

1. computes the version with `scripts/release/next_version.py`,
2. renders the release notes with `scripts/release/release_notes.py`,
3. builds and pushes every image (`sha-<commit>`, `latest`, `vX.Y.Z`),
4. publishes the chart,
5. pushes the annotated tag `vX.Y.Z` with the message `Release vX.Y.Z`, and
6. creates the GitHub Release, marked as latest.

The script takes the last release to be the highest `vX.Y.Z` tag the
workflow wrote. It ignores pre-release tags such as `v0.2.0-rc.1`. If the
commits since that release include an operator-action commit, the release is
a MINOR. Otherwise it is a PATCH. On the `develop` → `main` pull request, the
**Release preview** job shows the version and notes that the merge would
release. A notes or version problem fails that job before the merge.

To choose the version yourself, give a plain `X.Y.Z` greater than the last
release, in one of two ways:

- **In the pull request**: add a commit whose message ends with a
  `Release-As: 0.2.0` footer line. The newest such footer in the range wins.
  This takes a single pipeline run, and the notes cover everything since the
  last release.
- **After the fact, or in an emergency**: open Actions → `main` → Run
  workflow, choose the `main` branch, and set `release_as`. This runs the
  full pipeline on the current `main` head and takes precedence over any
  footer. A dispatch on any other branch is a validation run and releases
  nothing.

Never push a `v*` tag by hand. A hand-pushed tag has no chart and no GitHub
Release, and counting from it would skip a version, so the next release run
refuses to start until you delete it with `git push --delete origin vX.Y.Z`.

If a run fails after the tag push, re-run the failed jobs. `github-release` is
a separate job, and it leaves an existing GitHub Release alone.

### Marking operator-action commits

Mark every commit that needs operator action on upgrade. Put `!` after the
type or scope:

```text
feat(helm)!: rename vmController.defaultVmImage to vm.image
```

Or add a `BREAKING CHANGE:` footer, written as the instruction to the
operator. The footer text is copied into the release notes:

```text
fix(db): make migration 0400 non-rolling

BREAKING CHANGE: stop the orchestrator before upgrading; the chart guide
lists the steps.
```

If you are not sure whether a change needs operator action, mark it. A
needless MINOR costs much less than a broken upgrade.

The notes list operator actions first. Then come **Features** (`feat`) and
**Fixes** (`fix`, `perf`, `revert`). Commits of type `test`, `style`,
`chore`, `ci`, `docs`, `refactor` and `build`, unprefixed commits and merges
are counted and linked through the full changelog, not listed.

### Highlights

Hand-written highlights for a release go in `docs/release-notes/vX.Y.Z.md`
on `develop`, before the merge. Like every page under `docs/`, the file
starts with `title` and `description` frontmatter and one `#` title. The
notes script drops both and puts the rest at the top of the GitHub Release
as written. That text is published outside the docs site, so use absolute
links.
