# Agent Configuration for Nitro Sender

## Project Overview

The Nitro Sender is a library crate which has a core sender struct. This struct is the main interface for library
consumers and is meant to be a reusable module for fast transaction lending on Solana L1.

## Key Directories & Files

- `src/tasks` - The separate tasks which are spun up as part of the sender
- `.github` - Github related workflow files and remplates
- `justfile` - Utility commands for lintig and testing

## Coding Standards

- Rust formatting with `cargo +nightly fmt`
- Test before declaring that a solution is correct
- When adding new dependencies keep them in separate logical groups
- Always add a test for any non trivial feature or function added
- Prefer coding solutions that don't generate deep nesting of `if` or `let`, use `let ... else` and guard clauses
- Prefer iterators where possible
- Prefer graceful shutdown where possible
- Keep all public items documented and internal ones documented if not clear enough from the name or if too complex
- Avoid clones when unnecessary
- Avoid blocking if not needed
- Avoid all magic numbers, instead use documented contants
- For any new errors or failable commands prefer to have error invariants in a top level error enum for better
  usability
- If new features are added, add documentation to the top level `README.md` file, and if key documentation elements
  are missing from the `README.md`, leave it in a more complete state than was previously seen

## Development Workflow

- We are using Graphite (`gt`) for branch management. Get the most recent information on the CLI commands, because they
  change
- `just test` runs all the tests
- `just lint-fix` tries to auto-fix all possible lint and formatting issues and warnings, but if any warnings remain,
  try to address those according to the lint messages
