// Package routes lists every server route srw calls, with the token scope
// the server asks for it. A test compares this table with the server's own
// route inventory, so a route that is renamed or re-scoped on the server
// fails in the CLI's tests instead of at a user's terminal.
package routes

// Route is one route srw calls.
type Route struct {
	Method string
	// Path is the route template, as the server's inventory spells it.
	Path string
	// Scope is the token scope the route needs: a scope name, or "any" for
	// a route every scoped token may call.
	Scope string
}

// The routes of the commands built so far.
var (
	AuthMe  = Route{"GET", "/api/auth/me", "any"}
	Version = Route{"GET", "/api/version", "any"}

	ManifestValidate = Route{"POST", "/api/manifests/validate", "jobs:read"}
	ManifestPreview  = Route{"POST", "/api/manifests/preview", "jobs:read"}
	ManifestExport   = Route{"POST", "/api/manifests/export", "jobs:read"}
	ManifestApply    = Route{"POST", "/api/manifests/apply", "resources:write"}

	ResourceList   = Route{"GET", "/api/resources", "jobs:read"}
	ResourceGet    = Route{"GET", "/api/resources/{resource_id}", "jobs:read"}
	ResourceDelete = Route{"DELETE", "/api/resources/{resource_id}", "resources:write"}

	CacheList   = Route{"GET", "/api/workspace-cache", "jobs:read"}
	CacheDelete = Route{"DELETE", "/api/workspace-cache/{artifact_id}", "admin"}

	ExpertList       = Route{"GET", "/api/experts", "jobs:read"}
	ProjectList      = Route{"GET", "/api/projects", "jobs:read"}
	ConnectorCatalog = Route{"GET", "/api/datasources/catalog", "jobs:read"}
)

// All is every route above.
var All = []Route{
	AuthMe, Version,
	ManifestValidate, ManifestPreview, ManifestExport, ManifestApply,
	ResourceList, ResourceGet, ResourceDelete,
	CacheList, CacheDelete,
	ExpertList, ProjectList, ConnectorCatalog,
}
