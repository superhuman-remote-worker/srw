---
name: word-count
description: Use when you need exact word, line, and character counts for a text file — runs a bundled script so the numbers are exact instead of estimated by reading.
display_name: Word Count
icon: calculate
color: "#94e2d5"
tags:
  - text
  - scripts
---

# Word Count

Counts words, lines, and characters in a text file **exactly**, using a bundled
script — don't estimate the counts by reading the file yourself.

## How to use

This skill bundles a script at `skills/word-count/scripts/wordcount.py`. Run it on
the target file and read the printed result:

    python skills/word-count/scripts/wordcount.py <path-to-file>

It prints one line: `lines=<N> words=<N> chars=<N>`. Use those numbers in your
answer. The script reads only the file you pass and writes nothing — its output
is the only thing that matters.

If you are on a lite (virtual) workspace with no shell, `use_skill` will tell you
the script can't run here; call `request_workspace_upgrade` to ask for a
workspace with a shell, then run the command above.
