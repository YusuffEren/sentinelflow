# ADR 0001: Record architecture decisions

- Status: accepted
- Date: 2026-09-19

## Context

The team is growing beyond a single developer. Architectural decisions so far
(Kafka-based ingestion, Neo4j ring detection, Redis geo, ensemble ML, MASAK
compliance) live only in code and commit history, which is not discoverable
for newcomers and leads to relitigated debates.

## Decision

We adopt Architecture Decision Records (ADRs), stored in `docs/adr/` as
numbered Markdown files (`NNNN-title.md`). Each significant, hard-to-reverse
decision gets an ADR with: Context, Decision, Consequences, Alternatives.
Superseded ADRs are marked `superseded by NNNN` rather than deleted.

## Consequences

- New contributors can understand *why*, not just *what*.
- PRs that change an architectural choice must add/update an ADR
  (checked in PR template).
