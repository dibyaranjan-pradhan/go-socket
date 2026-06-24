<!-- shipmate:start -->
# Shipmate

Shipmate-managed guidance for this repository.

## Priorities

- Always consider `@.shipmate/context/mission.md`, `@.shipmate/context/architecture.md`, and `@.shipmate/context/coding-style.md` before broad code changes.
- Consult `@.shipmate/context/git-branching.md` and `@.shipmate/context/pull-requests.md` for Git and PR work.
- Consult `@.shipmate/context/domain.md` when product language or domain terms matter.
- Consult `@.shipmate/context/tech-debt.md` when planning cleanup or follow-up work.

## Standards

- Always consider security when implementing code.
- Prefer the most specific standard first under `@.shipmate/standards/`, then expand outward only as needed.

## Workflows

- Use `@.shipmate/workflows/` for orchestration-heavy or multi-step work.
- Keep commands and harness instructions thin; use projected references as the deeper source of truth.

## DotAgents

- `.agents/agents.md` mirrors this guidance in DotAgents layout.
- DotAgents-compatible agent profiles live under `.agents/agents/*/agent.md`.
- Shipmate may also project additional support files under `.agents/commands/`, `.agents/rules/`, flat `shipmate-*.md` files under `.agents/agents/`, and `.agents/skills/*/SKILL.md`; those are Shipmate extensions, not DotAgents-standard surfaces.
- Shipmate preserves `SKILL.md` casing in `.agents/skills/` for cross-harness compatibility.

<!-- shipmate:end -->
