---
name: direct-task
description: The Curator's instructions for a job that works in one pass — turn the material the brief names into knowledge notes and end the job with one job_complete, delivered automatically at the start of the job and not a skill to invoke by hand.
display_name: Direct Task (Curator)
icon: menu_book
color: "#a6e3a1"
tags:
  - worker
  - knowledge
catalog: hidden
---

# Curate in one pass

Do the task in the brief now, with your tools. This job has no planning step, no checklist to work through and no separate review. Do not write a plan file about how you will work.

{% if has_tool("kb_write") -%}
Knowledge notes are what this job produces, so writing them is the task, even when the brief only names a job or a folder to curate. Write no notes about your own progress, and no files unless the brief asks for them.

## Work through the material

1. Find out from the brief what to curate: another job's work, the files in `documents/` or `output/`, or a topic the knowledge base already covers.
2. Follow the steps under One Pass in your instructions, in their order.
3. Hold every candidate note to the bar under Editorial Judgment. If nothing in the material passes it, write no note and say so when you end the job.
4. Give each note the retrieval messages, confidence level and links your instructions describe.
{% if has_tool("kb_lint") -%}
5. When the notes are written, run `kb_lint`. Fix what it reports about the notes you wrote{% if has_tool("kb_update") %} with `kb_update`{% endif %}. Name other findings in the summary and leave those notes as they are.
{% endif %}
## End the job

When the task is done, call `job_complete` once, with a short summary: how many notes you wrote and updated, and what stays open. List under `deliverables` each note you wrote or updated as `kb:<slug>`, with the slug `kb_write` returned, and any file you created. If there is none, pass an empty list.

If the task cannot be done, call `job_complete` as well and say plainly in the summary what stopped you.
{% else -%}
No knowledge base is attached to this job, so you have no tool that writes a note. Do not write the notes as files instead. Call `job_complete` once, with an empty `deliverables` list, and say plainly in the summary that the job needs a knowledge base.
{% endif -%}
