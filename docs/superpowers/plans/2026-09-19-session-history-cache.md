# Session history cache repair implementation plan

**Goal:** Restore full session history on main dev without changing the stored transcript.
**Architecture:** Upgrade both Dexie schemas to index the existing `created_at` field. Preserve version fences and request a full snapshot whenever the client supports versioned history, including the first legacy-cache transition.
**Tech stack:** Angular, Dexie, Vitest/fake-indexeddb, Playwright Chromium, GitHub Actions and Fleet.
**Spec:** `knowledge-base/knowledge/issues/session_history_created_at_index_mismatch.md`.

## Tasks

- [x] Reproduce cold write/read, existing legacy/v2 database migration, reopen, newest cursor, and rewind/stale-response behavior through the actual IndexedDbService. Assert REST-shaped rows remain chronological and unrelated data survives upgrades. Run `cd cockpit && npx vitest run src/app/core/services/indexed-db-history.service.spec.ts` before implementation.
- [x] Add a PersistentChatService regression with the real cache and a legacy transcript, asserting a complete versioned response renders earlier user and assistant turns. Run the targeted spec before implementation.
- [x] Add CockpitDatabase v5 and ThreadHistoryDatabase v2 schemas with `threadMessages: 'id, threadId, [threadId+created_at]'`; update readers and model comment. Fetch a full snapshot when `applyThreadHistoryPage` is supported. Run the targeted tests, frontend suite and production build.
- [x] Verify existing-cache upgrade in headless Chromium, review changes, and update incident documentation. Commit only task changes.
- [ ] Push develop, monitor CI/chart publication/Fleet rollout, verify the deployed image and affected session with a browser. Record exact evidence and any remaining verification limitation.

No server database/session mutation. Preserve unrelated working-tree changes. No direct image override of Fleet-owned deployment.

## Local verification

373 targeted tests and all 3,179 frontend tests passed (168 files, two workers). Production build and i18n checks passed. Independent review found no actionable issues. Headless Chromium read all 404 affected-session messages after cold load, both old-schema upgrades, and reopen.
