## What and why

<!-- The problem this solves and the behavior you chose. Link the issue: Fixes #123 -->

## How it was verified

<!-- The exact commands you ran (pytest files, npx vitest specs, helm lint)
and anything you exercised in a running installation. -->

## Operator impact

<!-- Delete this section when nothing changes for an existing installation.
Otherwise name the migration, the changed default or values key, or the new
Secret key, and what an operator must do before upgrading. Mark the commit
with `!` (for example `fix(helm)!:`) and put the instruction in a
`BREAKING CHANGE:` footer; the release notes copy it (docs/versioning.md). -->

- [ ] Tests cover the changed behavior.
- [ ] User-visible Cockpit text is in both `en.json` and `de-DE.json`.
- [ ] No keys, tokens, private hostnames or customer data in the diff.
