# Lightspeed OTel Collector

Custom OpenTelemetry Collector distribution for OpenShift Lightspeed. Go.

## Specs

All specifications live in `.ai/spec/`. Start with `.ai/spec/README.md` for project overview, reading order, and structure guide.

## Commands

```bash
make build           # Build the collector binary via ocb
make test            # Run unit tests
make lint            # golangci-lint
make fmt             # go fmt
make vet             # go vet
make generate        # Generate collector source code from builder config
make verify-generate # Verify generated source is up to date
make run             # Build and run the collector locally
make docker-build    # Build container image (runs tests first)
```

## Conventions

- Built as a custom OTel Collector distribution using the OpenTelemetry Collector Builder (ocb)
- Include only the receivers, processors, and exporters needed by the OLS fleet
- Never create branches, commit, or push unless the user explicitly asks

### Code Citations

When referencing existing code, ALWAYS use clickable Cursor code references.
Format: triple-backtick line with `startLine:endLine:relative/path` (no language tag).
Path is relative to repo root.
CRITICAL: Content inside the block MUST be copied verbatim from the file (read it first).
Tabs, spaces, and indentation must match exactly or the link breaks.
Never type content from memory — always read the file to get exact content.
Only use language-tagged blocks for NEW code not yet in the repo.

## Git and PR Workflow

### Commit Messages
- Start with the Jira ticket reference: `OLS-XXXX description`
- Keep the first line under 72 characters
- Use imperative mood

### Pull Requests
This repo uses a **fork-based workflow**:

1. **Push to your fork**, not to `origin` (openshift/lightspeed-otel-collector)
2. **Create the PR** against `origin/main` using your fork's branch:
   ```bash
   git push <your-fork-remote> <branch>
   gh pr create --repo openshift/lightspeed-otel-collector --head <your-github-user>:<branch> --base main
   ```
3. **PR title** must start with the Jira reference: `OLS-XXXX description`
4. **Squash commits** before pushing
