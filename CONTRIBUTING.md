# Contributing to Postgres Search Replica

Thank you for your interest in contributing to Postgres Search Replica! We welcome contributions that help improve the robustness, simplicity, and performance of this library.

## Project Philosophy

- **Simplicity is Key**: We value simple and elegant solutions over complex ones. "Subtraction is addition."
- **Robustness**: The library handles critical infrastructure (replication slots, data consistency). Every change must prioritize data safety and source database protection.
- **Declarative Design**: We prefer declarative configurations over manual orchestration where possible.

## Getting Started

### Prerequisites

- **Python 3.11+**
- **[uv](https://github.com/astral-sh/uv)** for dependency management.
- **Docker & Docker Compose** for local development and testing.

### Setup

1.  Clone the repository:
    ```bash
    git clone https://github.com/h4gen/postgres-search-replica.git
    cd postgres-search-replica
    ```

2.  Sync dependencies:
    ```bash
    uv sync --extra test
    ```

3.  Set up the development environment:
    ```bash
    make dev
    ```
    This spins up a mock source database and the necessary infrastructure.

## Development Workflow

We use a `Makefile` to simplify common tasks. Please use these commands to ensure consistency:

- **Linting & Formatting**: We use `ruff`.
  ```bash
  make lint
  ```
- **Type Checking**: We use `ty`.
  ```bash
  make type-check
  ```
- **Testing**:
  ```bash
  make test
  ```
  Note: Most tests are integration tests that require the development containers to be running (`make dev`).

## Coding Standards

- **Python**: Follow idiomatic Python patterns. Avoid over-complicating data handling—use existing libraries (like `pandas` for tabular data) when appropriate.
- **Error Handling**: Be explicit. Ensure that database connections and replication slots are always safely managed, even in error states.
- **Logging**: Use the structured JSON logger provided in `src/pg_replica/observability.py`.
- **Documentation**: Update the `README.md` if you add new features or change existing configurations.

## Pull Request Process

1.  **Open an Issue**: For significant changes, please open an issue first to discuss your proposal.
2.  **Create a Branch**: Use descriptive branch names (e.g., `feature/hybrid-recovery-optimization` or `fix/connection-leak`).
3.  **Run Checks**: Ensure `make lint`, `make type-check`, and `make test` pass before submitting.
4.  **Write Tests**: Any new functionality should be covered by integration tests in the `tests/` directory.
5.  **License**: By contributing, you agree that your contributions will be licensed under the project's **AGPL v3** license.

## License

By contributing to this project, you agree that your contributions will be licensed under its **AGPL v3** license.

