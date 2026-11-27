# Cloudberry parallel retrieve demo with Apache Asyncband

Handoff package for a Claude Code session.

License: Apache License 2.0. See `LICENSE`. Copyright 2026 Ed Espino.

Start Claude Code in this directory and say:

    Read CLAUDE.md, docs/BACKGROUND.md and TASKS.md. Start with T0.
    Before writing code, list the crates you plan to use and confirm each API.

Before starting, extract the Asyncband 0.8.1 release candidate source into `vendor/`
and set the `CB_*` environment variables for the test cluster. Copy `.env.example`
to a file the shell exports. Do not commit that file. `vendor/`, `.env`, keys, and
state files are ignored.
