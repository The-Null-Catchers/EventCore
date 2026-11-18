# EventCore portfolio roadmap

Baseline reviewed on November 17, 2026. Repository roadmap published on November 18, 2026.

This roadmap applies to this repository and follows the shared Null Catchers portfolio plan. Verify the documented state against the current code and runs before starting work.

**Estimated initial demo effort** 45–75 hours for a developer familiar with the project. The estimate excludes external reviews, store accounts, target environment setup, and later milestones.

## Project priorities

**Documented state** The disk event log uses CRC and fsync. The broker supports consumer fencing, partial commits, dead-letter queues, and replay. SDK acceptance includes restart. The broker is single-node, and the dashboard, OpenAPI, and administration remain roadmap items.

**P0 — Demo** Demonstrate publishing and partial commits in a small client or console built on the existing API. Document the paths used for topics, partitions, reads, lag, and failed webhook delivery. Make expired offsets and stale leases understandable.

**P1 — Validation and depth** Run two consumers, commit part of a batch, restart a consumer and the broker, and verify redelivery. Measure throughput and latency with a slow consumer and a full test disk, and document at-least-once limits.

**P2 — Later** Add topic and member administration, password reset, and async SDK transports as demand warrants. Clustering is outside v0.1.

**Acceptance criteria** Displayed offsets and lag match the broker. Acknowledged events survive restart according to the storage guarantees, and uncommitted events can be redelivered.

**Demo scenario** Publish → consume → stop → redeliver → dead-letter queue → retry → replay to another topic. [STATUS](https://github.com/The-Null-Catchers/EventCore/blob/main/docs/STATUS.md), [PR #9](https://github.com/The-Null-Catchers/EventCore/pull/9).

## Next milestones

The [status record](docs/STATUS.md) covers partial commits and stale generation rejection. The [API documentation](docs/API.md) lists the available contracts.

1. [ ] Create a topic, one consumer group, and a fixture of five events using the current API.
2. [ ] Commit the first two events and restart the consumer and broker. Verify redelivery of the remaining three, rejection of an old-generation commit, and matching offsets.
3. [ ] Attach the output and commit SHA to a release. State the single-node and at-least-once delivery limits.

## Portfolio acceptance checklist

- [ ] Another person can reproduce the primary workflow from a clean checkout using the README and test credentials or fixtures.
- [ ] The demo tells one complete story with a defined user, problem, and saved result.
- [ ] Tests cover the main flow, an important failure, and access boundaries when multiple accounts are involved.
- [ ] Demo data is labeled synthetic and reproducible. Review screenshots and recordings for secrets and customer data.
- [ ] Include four to six real screenshots where appropriate, a 60–120 second recording, and working links. Identify the revision used for capture.
- [ ] The README describes implemented capabilities, setup, architecture, limits, tests, and the demo.
- [ ] A short case study explains two engineering decisions, a considered alternative, and how the system handles an actual failure.
- [ ] Release evidence records the commit SHA, CI, prerequisites, and limits. Performance data includes workload, hardware, and method.
- [ ] Licensing and contributor attribution are settled before distribution. Choose the license for each repository individually.

A hosted demo is optional. Code execution, server administration, and secret management can be demonstrated through a recording and a reproducible isolated environment. Keep public demo access scoped to the activities it needs.

## Implementation and follow-up

- Break P0 into tasks with expected behavior, acceptance criteria, and a suitable test.
- Verify existing functionality first. Update this plan when evidence tied to a commit or PR closes a milestone.
- Record the fixture source, the revision used for evidence, and the tested limits.
- Review the final diff and verify required checks for its exact SHA before merging. Follow branch protection.
- For documentation-only changes with no applicable workflow, verify content and links and record the checks actually performed.
- Tie performance, security, and readiness claims to supporting evidence.
