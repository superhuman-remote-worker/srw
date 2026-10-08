package main

import (
	"fmt"
	"strconv"
	"strings"
)

// Linux mount(2) flags and the file-type mask, spelled out so the policy
// builds and is tested on every platform.
const (
	msReadOnly = 0x1
	msNoSuid   = 0x2
	msNoDev    = 0x4
	msNoExec   = 0x8
	sIFMT      = 0o170000
)

// policy is what the opener grants, whatever the client asks.
type policy struct {
	ReadOnly   bool   // force ro: the mount refuses writes in the kernel
	AllowOther bool   // the workspace's user is not the rclone sidecar's
	Source     string // the mount's source column ("srw-cloud")
	Subtype    string // fuse.<subtype>; rclone's mount checks look for "rclone"
}

// mountSpec is one mount(2) call, without the fd= option.
type mountSpec struct {
	Flags   uintptr
	FSType  string
	Source  string
	MaxRead int
	Default bool // default_permissions
}

// planMount turns the client's fusermount3 options into a mount the policy
// allows. Options that only take privilege away are honoured; options that
// would add privilege, or that the opener does not know, are refused, as
// fusermount3 refuses unknown options. The client never chooses the source,
// the type, suid or dev, or whether others may enter.
func planMount(p policy, options string) (mountSpec, error) {
	spec := mountSpec{
		Flags:  msNoSuid | msNoDev,
		FSType: "fuse." + p.Subtype,
		Source: p.Source,
	}
	if p.ReadOnly {
		spec.Flags |= msReadOnly
	}
	for _, option := range strings.Split(options, ",") {
		key, value, hasValue := strings.Cut(strings.TrimSpace(option), "=")
		switch {
		case key == "":
		case key == "ro":
			spec.Flags |= msReadOnly
		case key == "noexec":
			spec.Flags |= msNoExec
		case key == "rw", key == "nosuid", key == "nodev", key == "exec":
			// rw cannot lift a forced ro; the others are already set or harmless.
		case key == "allow_other", key == "allow_root":
			// The policy decides who may enter, not the client.
		case key == "default_permissions":
			spec.Default = true
		case key == "fsname", key == "subtype":
			// Fixed by the policy: a client-chosen type could pose as another
			// filesystem in mountinfo.
		case key == "max_read" && hasValue:
			n, err := strconv.Atoi(value)
			if err != nil || n <= 0 {
				return mountSpec{}, fmt.Errorf("bad max_read %q", value)
			}
			spec.MaxRead = n
		default:
			return mountSpec{}, fmt.Errorf("option %q is not allowed", key)
		}
	}
	return spec, nil
}

// data is mount(2)'s data argument for a FUSE mount, as fusermount3 builds
// it: the descriptor, the root's file type, the daemon's ids.
func (s mountSpec) data(p policy, fd int, rootMode uint32, uid, gid int) string {
	parts := []string{
		"fd=" + strconv.Itoa(fd),
		"rootmode=" + strconv.FormatUint(uint64(rootMode&sIFMT), 8),
		"user_id=" + strconv.Itoa(uid),
		"group_id=" + strconv.Itoa(gid),
	}
	if p.AllowOther {
		parts = append(parts, "allow_other")
	}
	if s.Default {
		parts = append(parts, "default_permissions")
	}
	if s.MaxRead > 0 {
		parts = append(parts, "max_read="+strconv.Itoa(s.MaxRead))
	}
	return strings.Join(parts, ",")
}
