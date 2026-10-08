"""Workspace-side credential installation programs, delivered over SSH stdin.

Only the program and destination paths appear in the command. Values travel
in stdin. Environment values are retained in the session workspace as
explicitly agreed for v1; credential files are synced (removed once no
longer delivered).

Everything both programs write lives under ``~/.srw-credentials/``, which a
workspace snapshot never captures (``CREDENTIAL_EXCLUDE_PATTERNS`` in
``orchestrator.services.snapshot_service``).
"""

INSTALL_CREDENTIAL_ENV = r"""
import json, os, pathlib, shlex, sys, tempfile

target = pathlib.Path(sys.argv[1])
target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
state = target.with_suffix('.json')
values = json.loads(state.read_text()) if state.exists() else {}
values.update(json.load(sys.stdin))
for path, contents in (
    (state, json.dumps(values)),
    (target, ''.join('export ' + key + '=' + shlex.quote(value) + '\n'
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

#: Sync one work item's credential files into the workspace home.
#:
#: ``argv``: the home directory and the store's name. ``stdin``: a JSON list
#: of ``{"name", "content", "mode", "link"}``. Each file is written, with its
#: mode, into ``~/.srw-credentials/<store>/`` (0700). ``link``, if given, is
#: a home-relative path that becomes a symlink to the file. A link replaces
#: nothing but a link into this store or a stale link into another one (a
#: restored snapshot keeps links, never the store), and never lands outside
#: the home or inside ``~/.srw-credentials``. What an earlier sync of the
#: same store placed and this one does not is removed, with the directories
#: it created once they are empty; an empty list removes the store.
INSTALL_CREDENTIAL_FILES = r"""
import json, os, sys, tempfile

home = os.path.normpath(sys.argv[1])
root = os.path.join(home, '.srw-credentials')
store = os.path.join(root, sys.argv[2])
prefix = store + '/'
files = json.load(sys.stdin)


def inside(path, base):
    return path == base or path.startswith(base + '/')


def write(path, contents, mode):
    fd, temporary = tempfile.mkstemp(dir=store, prefix='.write-')
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, 'w', encoding='utf-8') as output:
            output.write(contents)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def ours(path):
    return os.path.islink(path) and os.readlink(path).startswith(prefix)


def stale(path):
    return (
        os.path.islink(path)
        and os.readlink(path).startswith(root + '/')
        and not os.path.exists(path)
    )


os.makedirs(root, mode=0o700, exist_ok=True)
os.makedirs(store, mode=0o700, exist_ok=True)
os.chmod(store, 0o700)
real_home = os.path.realpath(home)
real_root = os.path.realpath(root)
state = os.path.join(store, '.links.json')
try:
    with open(state, encoding='utf-8') as handle:
        placed = json.load(handle)
except (OSError, ValueError):
    placed = {}

names, links = set(), {}
for item in files:
    name = item['name']
    if not name or '/' in name or name.startswith('.'):
        sys.exit(3)
    names.add(name)
    write(os.path.join(store, name), item['content'], item['mode'] & 0o777)
    if item.get('link'):
        links[item['link']] = os.path.join(store, name)

for link in placed.get('links', []):
    path = os.path.join(home, link)
    if link not in links and ours(path):
        os.unlink(path)
for entry in os.listdir(store):
    if entry in names or (entry.startswith('.') and not entry.startswith('.write-')):
        continue
    os.unlink(os.path.join(store, entry))

kept, made = [], list(placed.get('dirs', []))
for link, target in links.items():
    if os.path.isabs(link):
        continue
    path = os.path.normpath(os.path.join(home, link))
    if path == home or not inside(path, home):
        continue
    parent = os.path.dirname(path)
    missing, probe = [], parent
    while not os.path.lexists(probe):
        missing.append(probe)
        probe = os.path.dirname(probe)
    real = os.path.realpath(probe)
    if not inside(real, real_home) or inside(real, real_root):
        continue
    for directory in reversed(missing):
        os.mkdir(directory, 0o700)
        made.append(os.path.relpath(directory, home))
    real = os.path.realpath(parent)
    if not inside(real, real_home) or inside(real, real_root):
        continue
    if os.path.lexists(path) and not ours(path) and not stale(path):
        continue
    temporary = os.path.join(parent, '.srw-link-' + os.urandom(8).hex())
    os.symlink(target, temporary)
    os.replace(temporary, path)
    kept.append(link)

left = []
for directory in sorted(set(made), key=len, reverse=True):
    try:
        os.rmdir(os.path.join(home, directory))
    except OSError:
        if os.path.isdir(os.path.join(home, directory)):
            left.append(directory)

if names:
    write(state, json.dumps({'links': kept, 'dirs': left}), 0o600)
else:
    if os.path.exists(state):
        os.unlink(state)
    try:
        os.rmdir(store)
    except OSError:
        pass
"""
