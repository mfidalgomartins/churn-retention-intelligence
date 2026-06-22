# Security Policy

## Supported versions

Security support applies to the `main` branch and the latest published release
artifacts generated from it. Historical outputs are not patched in place; fix
forward from the current branch and republish.

## Reporting a vulnerability

Do not open a public issue with exploit details, secrets, or private data.
Report security issues through GitHub private vulnerability reporting if it is
enabled for the repository. If private reporting is unavailable, contact the
repository owner with a minimal description and request a private disclosure
channel before sharing technical detail.

Include:

- Affected file, command, artifact, or dependency.
- Reproduction steps or proof of impact.
- Whether any secret, credential, or non-synthetic data may be exposed.
- Suggested fix, if known.

## Response policy

This is an analytics project with best-effort maintainer support and no formal
SLA. Security issues that can expose secrets, execute untrusted code, corrupt
release artifacts, or publish non-synthetic data should block release until
triaged and fixed or explicitly accepted by the project owner.

## Data handling

The repository is designed for synthetic data. Do not commit real customer
records, production credentials, database dumps, private notebooks, or raw
exports. Keep production data outside version control and outside generated
artifact directories.

## Dependency policy

Dependency risk is checked with:

```bash
make security
```

This runs Bandit against `src/` and audits installed dependencies with
`pip-audit --skip-editable`. Rerun it after dependency upgrades and before any
release.
