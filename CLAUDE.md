# Lightspeed OTel Collector

Custom OpenTelemetry Collector distribution for OpenShift Lightspeed. Go.

## Specs

All specifications live in `.ai/spec/`. Start with `.ai/spec/README.md` for project overview, reading order, and structure guide.

## Conventions

- Commit messages and PR titles start with `OLS-XXXX`
- Fork-based workflow: push to your fork, PR against `origin/main`
- Squash commits before pushing
- Never create branches, commit, or push unless the user explicitly asks

## Code Citations

When referencing existing code, ALWAYS use clickable Cursor code references.
Format: triple-backtick line with `startLine:endLine:relative/path` (no language tag).
Path is relative to repo root.
CRITICAL: Content inside the block MUST be copied verbatim from the file (read it first).
Tabs, spaces, and indentation must match exactly or the link breaks.
Never type content from memory — always read the file to get exact content.
Only use language-tagged blocks for NEW code not yet in the repo.
