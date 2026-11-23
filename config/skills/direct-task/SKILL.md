---
name: direct-task
description: The instructions of a job that does not plan in phases — do the task in the brief directly and end the job with one job_complete. Delivered automatically at the start of the job; not a skill to invoke by hand.
display_name: Direct Task
icon: bolt
color: "#f9e2af"
tags:
  - worker
catalog: hidden
---

# Do the task directly

Do the task in the brief now, with your tools. This job has no planning step, no to-do list to work through and no separate review phase: do not write a plan file or notes about the task unless the task asks for them.

When the task is done, call `job_complete` once, with a short summary of what you did. List under `deliverables` the files you created or changed; if there is none, pass an empty list. If the task cannot be done, call `job_complete` as well and say plainly in the summary what stopped you.
