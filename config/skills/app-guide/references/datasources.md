# Connectors — giving agents access to external systems and data

A **connector** is a stored configuration for something the agent should work
with: a database, MCP server, mailbox, cloud folder, git repository, knowledge
base, or credential file. You create connectors once, then attach them to
projects or individual jobs and sessions. Attaching is what grants access —
agents never see connectors you didn't attach.

## Supported types

- **MCP Server** — tools discovered from a remote HTTP/SSE server, when MCP
  connectors are enabled on the deployment. A local stdio command no longer
  runs inside the agent; a stdio server's image runs as a managed MCP server.
- **PostgreSQL, MongoDB, and Neo4j** — relational, document, and graph
  database access.
- **WebDAV** — cloud file storage (this is how the built-in cloud storage and
  services like Nextcloud/OpenCloud are attached); the agent can list, read,
  and — if allowed — write and delete files there.
- **Email** — mailbox search, reading, drafts, and optionally sending, within
  the configured folders and access tier.
- **Repository** — a git repository the agent can work with.
- **OKF Knowledge Base** — centrally indexed Markdown knowledge the agent can
  search and read.
- **Credential files** — kubeconfig, SSH key, or a generic file the agent
  needs to reach some other system, delivered to a sandbox or VM workspace.
  A kubeconfig or generic file lands at its path under the workspace home
  (kubeconfigs are merged into `~/.kube/config`, so `kubectl` works as is);
  an SSH key is held by an ssh-agent and never written to disk. A file's
  path must be in a credential location (`~/.kube/`, `~/.aws/`, `~/.azure/`,
  `~/.docker/`, `~/.srw-files/`, a few `~/.config/<app>/` directories,
  `~/.netrc`, `~/.pgpass`), and the files go when the session ends or the job
  finishes. A job that ends while paused or waiting for review keeps them in
  its workspace until the job is deleted.
- **Credentials** — named environment variables for API keys or website
  logins, delivered to a sandbox or VM workspace. Scripts read them from the
  environment; browser forms use `browser_type(ref=..., env_var="NAME")`.
  Once attached, credentials stay for the session and cannot be detached.
  Output is not filtered, and expiry is controlled by the credential provider.
- **Generic** — a free-form connection definition for anything else.

Email and OKF Knowledge Base setup have focused guide topics because their
access and indexing models are different from ordinary read-only/read-write
connectors.

## What attaching one does

When a job or session starts with a connector attached, the agent
automatically gets the matching access:

- An MCP server contributes its discovered, namespaced tools.
- A PostgreSQL connector gives it SQL query and schema-inspection tools.
- A MongoDB connector gives it query, aggregation, and schema tools.
- A Neo4j connector gives it graph (Cypher) query and schema tools.
- A WebDAV connector gives it cloud file tools.
- Repository and credential-file connectors prepare the workspace and
  credentials the agent needs.

## Read-only vs. read-write

Managed database and WebDAV attachments can be marked **read-only**. Read-only
connectors expose only querying and reading; read-write connectors additionally
allow writes (SQL execute, document inserts/updates, graph writes, cloud file
writes and deletes). Other connector types use their own boundary: email has
an access tier, MCP follows the server and credentials, and credential files
carry whatever access their underlying credential grants.

## Where to attach

- **On a project** — every job in the project can use it, and it becomes part
  of the project's shared context. Best for the data a team works with
  continuously.
- **On a single job or session** — one-off access for one piece of work.

Jobs an agent creates on your behalf (for example, a session delegating work)
inherit the connector selection of their parent unless overridden.

## Add access to the current session

If a task needs an external account, first check the supported type above and
explain what access is needed and why. A saved connector and a session
attachment are separate steps:

1. Open **Connectors** to create or configure the appropriate connection. Use
   the focused `datasources-email` or `datasources-okf` guide for those types.
   Keep passwords, tokens, and keys in the connector's credential controls.
2. Return to the connected session and open header **Settings → Connectors**.
   On a narrow screen, **Settings** is in the header's three-dot menu. Select
   the eligible connector for this conversation. If the list fails to load,
   use **Retry**; an empty or failed list is not proof that a type is unsupported.
3. Changes apply automatically from the next response. Read any error, then
   send the agent a follow-up so it can check its newly bound tools and try
   the operation. An attachment alone is not proof that the service is ready.

Repository and **Credentials** attachments need a Container or VM workspace;
upgrade a Virtual session under **Settings → Workspace** first. Knowledge-base
attachments are fixed during a live session; choose those when starting a
session. **Credentials** cannot be detached once attached. Other removals
close connections after the current response finishes; information the agent
already read remains in the conversation. Removing a connector does not
revoke its underlying provider credential or undo prior actions.

## When a service has no named connector

Check whether its supported interface matches an existing connector, such as
WebDAV, a repository, or an MCP server. An MCP server must actually exist and
be compatible; a **Generic** connection definition does not create tools or
implement an integration by itself.

An external provider may also offer an API or CLI usable from a shell-capable
workspace, a website usable with Browser tools, or an export/import workflow
the user can carry out with the agent's help. Verify the provider's current
documentation and required access before choosing that route. Read
`canvas-and-browser` for browser enablement and a user-controlled login;
`files-and-integrations` covers external hosting prerequisites. Explain which
parts the agent can do now, the smallest setup step the user needs to take,
and useful work that can begin while access is pending. No named connector
does not by itself make the user's goal impossible.
