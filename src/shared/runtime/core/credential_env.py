"""Workspace-side credential installation programs, delivered over SSH stdin.

Only the program and destination paths appear in the command. Values travel
in stdin. Environment values are retained in the session workspace as
explicitly agreed for v1; credential files are synced (removed once no
longer delivered) and retired with the work item.

Everything both programs write lives under ``~/.srw-credentials/`` (0700),
which a workspace snapshot never captures (``CREDENTIAL_EXCLUDE_PATTERNS`` in
``orchestrator.services.snapshot_service``). Both run as
``/usr/bin/python3 -I`` (:data:`WORKSPACE_PYTHON`), never a ``python3`` found
on the workspace's ``PATH`` or a site directory the workspace can write: the
secrets on their stdin must not reach a planted interpreter or ``.pth`` file.
"""

#: The interpreter the programs run with: absolute, in isolated mode.
WORKSPACE_PYTHON = "/usr/bin/python3 -I"

INSTALL_CREDENTIAL_ENV = r"""
import json, os, pathlib, shlex, sys, tempfile

target = pathlib.Path(sys.argv[1])
target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
os.chmod(target.parent, 0o700)
state = target.with_suffix('.json')
values = json.loads(state.read_text()) if state.exists() else {}
values.update(json.load(sys.stdin))
for path, contents in (
    (state, json.dumps(values)),
    (target, ''.join('unset ' + key + '\n' if value is None
                     else 'export ' + key + '=' + shlex.quote(value) + '\n'
                     for key, value in values.items())),
):
    fd, temporary = tempfile.mkstemp(dir=target.parent, prefix='.credentials-')
    try:
        with os.fdopen(fd, 'w') as output:
            output.write(contents)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
"""

#: Sync, or retire, one work item's credential files in the workspace home.
#:
#: ``argv``: the home, the store's name (``files-<identity>``) and ``sync``
#: or ``retire``. ``sync`` reads ``{"files": [...], "env": [...]}`` on stdin.
#: Each file (``name``, ``content``, ``mode``, ``link``) is written into
#: ``~/.srw-credentials/<store>/`` with its mode, never an execute bit;
#: ``link``, if given, is a home-relative path that becomes a symlink to it.
#: A link replaces nothing but a link into this store or a stale link into
#: another (a restored snapshot keeps links, never the store), never lands
#: outside the home or inside ``~/.srw-credentials``, and never passes
#: through a symlinked directory. A link that cannot be placed is skipped
#: with its reason; the others still are.
#:
#: Each ``env`` item (``name``, ``files``, ``prepend``) sets a variable in the
#: work item's environment file (``~/.srw-credentials/<identity>.sh``) to its
#: stored files, colon-separated, after ``prepend`` (a home-relative path) if
#: that path exists and is not this store's link. A variable belongs to the
#: sync only while that file holds what the sync last wrote: one another
#: connector set is skipped, never overwritten. A variable an earlier sync
#: set and this one does not is unset (``unset NAME``, never ``NAME=``).
#:
#: What an earlier sync placed and this one does not is removed, with the
#: directories it created once they are empty. The state (links, directories,
#: variables) is ``<store>/.links.json``, always written. ``retire`` removes
#: the links, the store and the work item's environment file. Both print one
#: JSON line naming paths and variables only, never contents.
INSTALL_CREDENTIAL_FILES = r"""
import json, os, shlex, shutil, sys, tempfile

home = os.path.normpath(sys.argv[1])
store_name = sys.argv[2]
action = sys.argv[3]
if not store_name.startswith('files-') or '/' in store_name or action not in ('sync', 'retire'):
    sys.exit(3)
root = os.path.join(home, '.srw-credentials')
store = os.path.join(root, store_name)
prefix = store + '/'
real_prefix = os.path.join(os.path.realpath(root), store_name) + '/'
env_sh = os.path.join(root, store_name[len('files-'):] + '.sh')
env_json = env_sh[:-3] + '.json'


def inside(path, base):
    return path == base or path.startswith(base + '/')


def write(directory, path, contents, mode):
    fd, temporary = tempfile.mkstemp(dir=directory, prefix='.write-')
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, 'w', encoding='utf-8') as output:
            output.write(contents)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def ours(path):
    if not os.path.islink(path):
        return False
    target = os.readlink(path)
    return target.startswith(prefix) or target.startswith(real_prefix)


def stale(path):
    return (
        os.path.islink(path)
        and os.readlink(path).startswith(root + '/')
        and not os.path.exists(path)
    )


def load(path, default):
    try:
        with open(path, encoding='utf-8') as handle:
            value = json.load(handle)
        return value if isinstance(value, type(default)) else default
    except (OSError, ValueError):
        return default


def unlink_ours(links):
    for link in links:
        path = os.path.join(home, link)
        try:
            if ours(path):
                os.unlink(path)
        except OSError:
            pass


def prune(directories):
    left = []
    for directory in sorted(set(directories), key=len, reverse=True):
        path = os.path.join(home, directory)
        try:
            os.rmdir(path)
        except OSError:
            if os.path.isdir(path):
                left.append(directory)
    return left


placed = load(os.path.join(store, '.links.json'), {})
previous_links = [link for link in placed.get('links', []) if isinstance(link, str)]
previous_env = {
    name: value
    for name, value in (placed.get('env') or {}).items()
    if isinstance(name, str) and isinstance(value, str)
}

if action == 'retire':
    unlink_ours(previous_links)
    prune(placed.get('dirs', []))
    shutil.rmtree(store, ignore_errors=True)
    for path in (env_sh, env_json):
        try:
            os.unlink(path)
        except OSError:
            pass
    print(json.dumps({'retired': True, 'links': sorted(previous_links)}))
    sys.exit(0)

request = json.load(sys.stdin)
files = request.get('files') or []
wanted_env = request.get('env') or []
if not files and not wanted_env and not os.path.isdir(store):
    print(json.dumps({'store': store, 'linked': [], 'skipped': {}, 'env': [],
                      'env_skipped': {}, 'env_retired': [],
                      'env_file': os.path.exists(env_sh)}))
    sys.exit(0)

os.makedirs(root, mode=0o700, exist_ok=True)
os.chmod(root, 0o700)
os.makedirs(store, mode=0o700, exist_ok=True)
os.chmod(store, 0o700)

names, links = set(), {}
for item in files:
    name = item['name']
    if not name or '/' in name or name.startswith('.'):
        sys.exit(3)
    names.add(name)
    write(store, os.path.join(store, name), item['content'], item['mode'] & 0o666)
    if item.get('link'):
        links[item['link']] = os.path.join(store, name)

unlink_ours(link for link in previous_links if link not in links)
for entry in os.listdir(store):
    if entry in names or (entry.startswith('.') and not entry.startswith('.write-')):
        continue
    try:
        os.unlink(os.path.join(store, entry))
    except OSError:
        pass


def place(link, target, made):
    if os.path.isabs(link):
        return 'outside the home'
    path = os.path.normpath(os.path.join(home, link))
    if path == home or not inside(path, home):
        return 'outside the home'
    if inside(path, root):
        return 'inside the credential store'
    parent = os.path.dirname(path)
    # Every existing component between the home and the link must be a real
    # directory: a symlinked one could lead anywhere.
    probe, missing = home, []
    for part in os.path.relpath(parent, home).split('/'):
        if part in ('', '.'):
            continue
        probe = os.path.join(probe, part)
        if os.path.islink(probe):
            return 'a directory on the way is a symlink'
        if not os.path.lexists(probe):
            missing.append(probe)
        elif not os.path.isdir(probe):
            return 'a file is in the way'
    for directory in missing:
        os.mkdir(directory, 0o700)
        made.append(os.path.relpath(directory, home))
    if os.path.lexists(path) and not ours(path) and not stale(path):
        if os.path.islink(path) and os.readlink(path).startswith(root + '/'):
            return 'another work item\'s file is there'
        return 'a file of the user is there'
    temporary = os.path.join(parent, '.srw-link-' + os.urandom(8).hex())
    os.symlink(target, temporary)
    try:
        os.replace(temporary, path)
    except OSError:
        os.unlink(temporary)
        raise
    return None


kept, skipped, made = [], {}, list(placed.get('dirs', []))
for link, target in links.items():
    try:
        reason = place(link, target, made)
    except OSError as exc:
        reason = exc.strerror or type(exc).__name__
    if reason is None:
        kept.append(link)
    else:
        skipped[link] = reason
left = prune(made)

values = load(env_json, {})
env, env_skipped, env_set = {}, {}, []
for item in wanted_env:
    name = item['name']
    current = values.get(name)
    # An unset or empty variable is nobody's (a sync unsets what it retires).
    if current and previous_env.get(name) != current:
        env_skipped[name] = 'set by another connector'
        continue
    parts = [os.path.join(store, stored) for stored in item.get('files') or []]
    first = item.get('prepend')
    if first:
        # The user's own file, if ours could not take its place, comes first:
        # a connector never replaces the user's default (kubectl's
        # current-context is the first file's).
        path = os.path.join(home, first)
        if os.path.lexists(path) and not ours(path):
            parts.insert(0, path)
    env[name] = ':'.join(parts)
    env_set.append(name)
retired = {}
for name, value in previous_env.items():
    if name in env:
        continue
    if values.get(name) == value:
        # Unset, never exported empty: an empty AWS_SHARED_CREDENTIALS_FILE
        # would hide ~/.aws/credentials for the rest of the work item.
        retired[name] = None
values.update(env)
values.update(retired)
if env or retired or os.path.exists(env_json):
    write(root, env_json, json.dumps(values), 0o600)
    write(root, env_sh, ''.join('unset ' + key + '\n' if value is None
                                else 'export ' + key + '=' + shlex.quote(value) + '\n'
                                for key, value in values.items()), 0o600)

state = {'links': kept, 'dirs': left, 'env': env}
if names or kept or env:
    write(store, os.path.join(store, '.links.json'), json.dumps(state), 0o600)
else:
    try:
        os.unlink(os.path.join(store, '.links.json'))
    except OSError:
        pass
    try:
        os.rmdir(store)
    except OSError:
        write(store, os.path.join(store, '.links.json'), json.dumps(state), 0o600)
print(json.dumps({'store': store, 'linked': sorted(kept), 'skipped': skipped,
                  'env': sorted(env_set), 'env_skipped': env_skipped,
                  'env_retired': sorted(retired),
                  'env_file': os.path.exists(env_sh)}))
"""
