import { randomUUID } from 'node:crypto';
import type { Page, TestInfo } from '@playwright/test';
import { expect, test as base } from '@playwright/test';
import { requireJson } from './api';
import {
  BASE_ORIGIN,
  DEFER_FAILED_CLEANUP,
  RESOURCE_LEDGER_OVERRIDE,
  runtimeEnvironment,
} from './environment';
import { NetworkLedger, type JourneyPhase } from './network-ledger';
import {
  ProviderControlClient,
  requireCleanProviderState,
  type ProviderScenarioState,
} from './provider-control';
import { ResourceLedger } from './resource-ledger';

const FIXTURE_TIMEOUT_MS = 300_000;

interface ThreadList {
  threads: Array<{ id: string }>;
}

interface ThreadDetail {
  execution_lane?: unknown;
  metadata?: unknown;
}

export interface ThreadTopology {
  executionLane: string | null;
  workspaceBackend: string | null;
  workspaceStatus: string | null;
  workspaceProvisioner: string | null;
}

export interface ApplicationJourney {
  page: Page;
  runId: string;
  runToken: string;
  expectedReply: string;
  chatModel: string;
  expectedExecutionLane: 'pinned' | 'stateless';
  workspaceBackend: 'virtual' | 'sandbox';
  network: NetworkLedger;
  provider: ProviderControlClient;
  setPhase(phase: JourneyPhase): void;
  listThreadIds(): Promise<Set<string>>;
  threadTopology(threadId: string): Promise<ThreadTopology>;
  registerThread(threadId: string): void;
}

interface ApplicationFixtures {
  app: ApplicationJourney;
}

function safeTestFileName(testInfo: TestInfo): string {
  return `${testInfo.testId.replace(/[^a-zA-Z0-9_.-]+/g, '-')}.resources.json`;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function stringField(value: Record<string, unknown>, key: string): string | null {
  return typeof value[key] === 'string' ? value[key] : null;
}

async function attachJson(testInfo: TestInfo, name: string, value: unknown): Promise<void> {
  await testInfo.attach(name, {
    body: Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8'),
    contentType: 'application/json',
  });
}

async function waitForProviderIdle(
  client: ProviderControlClient,
  runId: string,
): Promise<ProviderScenarioState> {
  const deadline = Date.now() + 15_000;
  let state = await client.state(runId);
  while (state.pending_calls !== 0 && Date.now() < deadline) {
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 250));
    state = await client.state(runId);
  }
  return state;
}

export const test = base.extend<ApplicationFixtures>({
  app: [
    async ({ context, page, request }, use, testInfo) => {
      const environment = runtimeEnvironment();
      const runId = randomUUID();
      const runToken = `E2E-${runId}`;
      const expectedReply = `E2E_REPLY:${runId}`;
      const ledgerPath =
        RESOURCE_LEDGER_OVERRIDE ?? testInfo.outputPath(safeTestFileName(testInfo));
      const resources = new ResourceLedger(runId, ledgerPath);
      const network = new NetworkLedger(page, BASE_ORIGIN);
      const provider = new ProviderControlClient(environment.controlUrl, environment.controlToken);
      const createRegistrationTasks: Array<Promise<void>> = [];
      let scenarioArmed = false;

      const registerThread = (threadId: string): void => {
        resources.registerThread(threadId);
        network.registerThread(threadId);
      };

      // Automatic leak guard: track the outgoing create request, not merely
      // its eventual response event. Teardown therefore waits for and parses
      // a create that is still provisioning when a competing UI assertion
      // fails, then persists the exact id before closing browser transport.
      page.on('request', (browserRequest) => {
        let requestPath: string;
        try {
          const url = new URL(browserRequest.url());
          if (url.origin !== BASE_ORIGIN) return;
          requestPath = url.pathname;
        } catch {
          return;
        }
        if (browserRequest.method() !== 'POST' || requestPath !== '/api/persistent/threads') {
          return;
        }
        createRegistrationTasks.push(
          browserRequest.response().then(async (response) => {
            if (!response || response.status() < 200 || response.status() >= 300) return;
            const body = (await response.json()) as { thread_id?: unknown };
            if (typeof body.thread_id !== 'string') {
              throw new Error('Successful thread creation omitted its thread id.');
            }
            registerThread(body.thread_id);
          }),
        );
      });

      testInfo.annotations.push({ type: 'e2e-run-id', description: runId });
      const application: ApplicationJourney = {
        page,
        runId,
        runToken,
        expectedReply,
        chatModel: environment.chatModel,
        expectedExecutionLane: environment.expectedExecutionLane,
        workspaceBackend: environment.workspaceBackend,
        network,
        provider,
        setPhase: (phase) => network.setPhase(phase),
        listThreadIds: async () => {
          const body = await requireJson<ThreadList>(
            await request.get('/api/persistent/threads'),
            'journey thread inventory',
          );
          return new Set(body.threads.map(({ id }) => id));
        },
        threadTopology: async (threadId) => {
          const body = await requireJson<ThreadDetail>(
            await request.get(`/api/persistent/threads/${encodeURIComponent(threadId)}`),
            'journey thread topology',
          );
          const metadata = record(body.metadata);
          const configOverride = record(metadata['config_override']);
          const workspace = record(configOverride['workspace']);
          const workspaceContainer = record(metadata['workspace_container']);
          return {
            executionLane: typeof body.execution_lane === 'string' ? body.execution_lane : null,
            workspaceBackend: stringField(workspace, 'backend'),
            workspaceStatus: stringField(workspaceContainer, 'status'),
            workspaceProvisioner: stringField(workspaceContainer, 'provisioner'),
          };
        },
        registerThread,
      };

      await provider.health();
      await provider.arm(runId);
      scenarioArmed = true;

      await use(application);

      const teardownErrors: Error[] = [];
      const bodyFailed =
        testInfo.status !== undefined && testInfo.status !== testInfo.expectedStatus;
      const registrationResults = await Promise.allSettled(createRegistrationTasks);
      for (const result of registrationResults) {
        if (result.status === 'rejected') {
          teardownErrors.push(
            new Error(`Automatic thread registration failed: ${errorMessage(result.reason)}`),
          );
        }
      }
      resources.finalize();

      if (bodyFailed && resources.threadIds().length > 0) {
        try {
          await attachJson(
            testInfo,
            'sanitized-thread-evidence',
            await resources.captureFailureEvidence(request),
          );
        } catch (error) {
          teardownErrors.push(
            new Error(
              'Could not capture sanitized pre-cleanup thread evidence: ' + errorMessage(error),
            ),
          );
        }
      }

      network.setPhase('closing');
      try {
        if (!page.isClosed()) await page.close({ runBeforeUnload: false });
        await context.close();
      } catch (error) {
        teardownErrors.push(new Error(`Browser transport close failed: ${errorMessage(error)}`));
      }

      try {
        network.assertClean();
      } catch (error) {
        teardownErrors.push(error instanceof Error ? error : new Error(String(error)));
      }

      const cleanupDeferred =
        DEFER_FAILED_CLEANUP &&
        resources.threadIds().length > 0 &&
        (bodyFailed || teardownErrors.length > 0);
      if (cleanupDeferred && !bodyFailed) {
        try {
          await attachJson(
            testInfo,
            'sanitized-thread-evidence',
            await resources.captureFailureEvidence(request),
          );
        } catch (error) {
          teardownErrors.push(
            new Error(
              'Could not capture sanitized pre-cleanup thread evidence: ' + errorMessage(error),
            ),
          );
        }
      }

      let cleanupSucceeded = true;
      if (cleanupDeferred) {
        cleanupSucceeded = false;
        testInfo.annotations.push({
          type: 'cleanup-deferred',
          description:
            'Outer owned-cluster runner must diagnose, exact-clean, then retain closed provider accounting.',
        });
      } else {
        try {
          await resources.cleanup(request);
        } catch (error) {
          cleanupSucceeded = false;
          teardownErrors.push(new Error(`Resource cleanup failed: ${errorMessage(error)}`));
        }
      }

      let finalProviderState: ProviderScenarioState | null = null;
      let finalProviderOverview: Awaited<ReturnType<ProviderControlClient['overview']>> | null =
        null;
      let providerCleanupSucceeded = !scenarioArmed;
      if (scenarioArmed) {
        try {
          finalProviderState = cleanupSucceeded
            ? await waitForProviderIdle(provider, runId)
            : await provider.state(runId);
          finalProviderOverview = await provider.overview();
          if (cleanupSucceeded) {
            requireCleanProviderState(
              finalProviderState,
              runId,
              environment.chatModel,
              { requireIdle: true, requireReply: !bodyFailed },
              finalProviderOverview,
            );
          }
        } catch (error) {
          teardownErrors.push(new Error(`Provider accounting failed: ${errorMessage(error)}`));
        } finally {
          // A failed exact cleanup leaves the lifecycle responders armed for
          // the outer runner's crash-recovery cleanup. Close is legal only
          // after every ledger-owned resource is absent.
          if (cleanupSucceeded) {
            try {
              finalProviderState = await provider.close(runId);
              finalProviderOverview = await provider.overview();
              providerCleanupSucceeded = true;
            } catch (error) {
              teardownErrors.push(
                new Error(`Provider retaining close failed: ${errorMessage(error)}`),
              );
            }
          }
        }
      }

      if (cleanupSucceeded && providerCleanupSucceeded) {
        try {
          resources.markCleanupComplete();
        } catch (error) {
          teardownErrors.push(
            new Error(`Resource-ledger completion failed: ${errorMessage(error)}`),
          );
        }
      }

      await attachJson(testInfo, 'sanitized-network-ledger', network.entries());
      if (finalProviderState) {
        await attachJson(testInfo, 'sanitized-provider-accounting', {
          scenario: finalProviderState,
          overview: finalProviderOverview,
        });
      }
      await testInfo.attach('exact-resource-ledger', {
        path: resources.path,
        contentType: 'application/json',
      });

      if (teardownErrors.length === 1) throw teardownErrors[0];
      if (teardownErrors.length > 1) {
        throw new AggregateError(
          teardownErrors,
          'Application E2E teardown found multiple failures.',
        );
      }
    },
    { timeout: FIXTURE_TIMEOUT_MS },
  ],
});

export { expect };
