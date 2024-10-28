# **Contributing to pg_analytics**

Welcome! We're excited that you're interested in contributing to `pg_analytics` and want to make the process as smooth as possible.

## Technical Info

Before submitting a pull request, please review this document, which outlines what conventions to follow when submitting changes. If you have any questions not covered in this document, please reach out to us in the [ParadeDB Community Slack](https://join.slack.com/t/paradedbcommunity/shared_invite/zt-2lkzdsetw-OiIgbyFeiibd1DG~6wFgTQ) or via [email](mailto:support@paradedb.com).

### Claiming GitHub Issues

This repository has a workflow to automatically assign issues to new contributors. This ensures that you don't need approval
from a maintainer to pick an issue.

1. Before claiming an issue, ensure that:

- It's not already assigned to someone else
- There are no comments indicating ongoing work

2. To claim an unassigned issue, simply comment `/take` on the issue. This will automatically assign the issue to you.

If you find yourself unable to make progress, don't hesitate to seek help in the issue comments or in the [ParadeDB Community Slack](https://join.slack.com/t/paradedbcommunity/shared_invite/zt-2lkzdsetw-OiIgbyFeiibd1DG~6wFgTQ). If you no longer wish to
work on the issue(s) you self-assigned, please use the `unassign me` link at the top of the issue(s) page to release it.

### Development Workflow

The development of the `pg_analytics` Postgres extension is done via `pgrx`. For detailed development instructions, please refer to the Development section of the README in the extension's subfolder.

### Pull Request Workflow

All changes to `pg_analytics` happen through GitHub Pull Requests. Here is the recommended
flow for making a change:

1. Before working on a change, please check to see if there is already a GitHub issue open for that change.
2. If there is not, please open an issue first. This gives the community visibility into what you're working on and allows others to make suggestions and leave comments.
3. Fork the `pg_analytics` repo and branch out from the `dev` branch.
4. Install [pre-commit](https://pre-commit.com/) hooks within your fork with `pre-commit install` to ensure code quality and consistency with upstream.
5. Make your changes. If you've added new functionality, please add tests. We will not merge a feature without appropriate tests.
6. Open a pull request towards the `dev` branch. Ensure that all tests and checks pass. Note that the `pg_analytics` repository has pull request title linting in place and follows the [Conventional Commits spec](https://github.com/amannn/action-semantic-pull-request).
7. Congratulations! Our team will review your pull request.

### Documentation

The public-facing documentation for `pg_analytics` is written directly in the README. If you are adding a new feature that requires new documentation, please add the documentation as part of your pull request. We will not merge a feature without appropriate documentation.

## Legal Info

### License

By contributing to `pg_analytics`, you agree that your contributions will be licensed under the [PostgreSQL License](LICENSE).
