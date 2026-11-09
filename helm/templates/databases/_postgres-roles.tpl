{{/*
Role split on the bundled Postgres StatefulSets of SRW's own three databases
(app, pgvector, audit). `postgres` is the bootstrap superuser, with a password
in a Secret of its own that no application pod reads; SRW's role is an
ordinary owner of its database, the way CloudNativePG already runs it.
files/postgres-roles.sh does the work, at initdb on a fresh volume and from an
init container on every start, which is what converts an existing volume.
Operator view: helm/README.md, "Bundled Postgres roles".
*/}}

{{/* The bootstrap superuser's name on every bundled SRW database server. */}}
{{- define "srw.postgresSuperuser" -}}
postgres
{{- end }}

{{/*
The superuser passwords' Secret. Deliberately NOT the application Secret:
agent pods take that one whole through envFrom.
*/}}
{{- define "srw.postgresSuperuserSecretName" -}}
{{- printf "%s-db-superuser" (include "srw.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Per-database wiring: values key -> application Secret keys, ConfigMap key for
the database name, superuser Secret key, and the extensions the superuser
creates before the migrations run. The lists are the CREATE EXTENSION
statements of src/orchestrator/database/migrations/<chain>: `vector` is not a
trusted extension, so the application role cannot create it; the trusted ones
are listed too so a fresh database matches a migrated one before 0001 runs.
*/}}
{{- define "srw.postgresRolesDatabases" -}}
postgres:
  userKey: POSTGRES_USER
  passwordKey: POSTGRES_PASSWORD
  dbKey: POSTGRES_DB
  superuserPasswordKey: POSTGRES_SUPERUSER_PASSWORD
  extensions: uuid-ossp btree_gist
vector:
  userKey: VECTOR_POSTGRES_USER
  passwordKey: VECTOR_POSTGRES_PASSWORD
  dbKey: VECTOR_POSTGRES_DB
  superuserPasswordKey: VECTOR_POSTGRES_SUPERUSER_PASSWORD
  extensions: uuid-ossp vector pg_trgm
audit:
  userKey: AUDIT_POSTGRES_USER
  passwordKey: AUDIT_POSTGRES_PASSWORD
  dbKey: AUDIT_POSTGRES_DB
  superuserPasswordKey: AUDIT_POSTGRES_SUPERUSER_PASSWORD
  extensions: ""
{{- end }}

{{/* "true" when at least one of the three renders its StatefulSet. */}}
{{- define "srw.postgresRolesEnabled" -}}
{{- range $key := list "postgres" "vector" "audit" -}}
{{- if include "srw.dbRendersStatefulset" (dict "context" $ "key" $key) -}}true{{- end -}}
{{- end -}}
{{- end }}

{{/*
Environment for both the init container and the database container: the
image's initdb reads POSTGRES_USER/POSTGRES_PASSWORD as the bootstrap
superuser, the script reads the rest.
Usage: {{ include "srw.postgresRolesEnv" (dict "context" . "key" "vector") }}
*/}}
{{- define "srw.postgresRolesEnv" -}}
{{- $ctx := .context -}}
{{- $db := index (include "srw.postgresRolesDatabases" $ctx | fromYaml) .key -}}
{{- $appUser := index ($ctx.Values.secrets.values | default dict) $db.userKey | default "" -}}
{{- if eq $appUser (include "srw.postgresSuperuser" $ctx) -}}
{{- fail (printf "secrets.values.%s must not be %q: that name is the bundled database's bootstrap superuser" $db.userKey $appUser) -}}
{{- end -}}
- name: POSTGRES_USER
  value: {{ include "srw.postgresSuperuser" $ctx | quote }}
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "srw.postgresSuperuserSecretName" $ctx }}
      key: {{ $db.superuserPasswordKey }}
- name: SRW_APP_USER
  valueFrom:
    secretKeyRef:
      name: {{ include "srw.secretName" $ctx }}
      key: {{ $db.userKey }}
- name: SRW_APP_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "srw.secretName" $ctx }}
      key: {{ $db.passwordKey }}
- name: SRW_APP_DB
  valueFrom:
    configMapKeyRef:
      name: {{ include "srw.configMapName" $ctx }}
      key: {{ $db.dbKey }}
- name: SRW_DB_EXTENSIONS
  value: {{ $db.extensions | quote }}
{{- end }}

{{/*
The init container. A fresh volume is left to the image's initdb (which runs
the same script from /docker-entrypoint-initdb.d); an existing one is
converted if needed and brought to the end state, on a temporary server that
listens on the pod's Unix socket only.
Usage: {{ include "srw.postgresRolesInitContainer" (dict "context" . "key" "vector" "db" .Values.databases.vector) }}
*/}}
{{- define "srw.postgresRolesInitContainer" -}}
- name: postgres-roles
  image: {{ .db.image }}
  command: ["bash", "/srw-postgres-roles/postgres-roles.sh", "upgrade"]
  env:
    {{- include "srw.postgresRolesEnv" (dict "context" .context "key" .key) | nindent 4 }}
  volumeMounts:
    - name: data
      mountPath: /var/lib/postgresql/data
    - name: postgres-roles
      mountPath: /srw-postgres-roles
      readOnly: true
  resources:
    {{- toYaml .db.resources | nindent 4 }}
{{- end }}

{{/* Pod annotation: a change to the script restarts the pod, which applies it. */}}
{{- define "srw.postgresRolesChecksum" -}}
checksum/postgres-roles: {{ .Files.Get "files/postgres-roles.sh" | sha256sum | quote }}
{{- end }}

{{/* The database container's mount: the image's initdb runs *.sh from here. */}}
{{- define "srw.postgresRolesInitdbMount" -}}
- name: postgres-roles
  mountPath: /docker-entrypoint-initdb.d
  readOnly: true
{{- end }}

{{- define "srw.postgresRolesVolume" -}}
- name: postgres-roles
  configMap:
    name: {{ include "srw.fullname" . }}-postgres-roles
    # Executable: docker-entrypoint.sh runs an executable *.sh as a program.
    defaultMode: 0555
{{- end }}
