# Contributing to pyspark-example

Thank you for your interest in contributing to this project!

## How to Contribute

1. **Fork the Repository**: Create a personal fork on GitHub.
2. **Create a Feature Branch**:
   ```bash
   git checkout -b feature/my-new-feature
   ```
3. **Set Up Development Environment**:
   - Install [uv](https://docs.astral.sh/uv/) (recommended) or standard Python 3.10+.
   - Ensure Java 21 (or 17) is installed and `JAVA_HOME` is set.
4. **Run Tests Locally**:
   ```bash
   uv run --group test pytest -v
   ```
   Or via `make`:
   ```bash
   make test
   ```
   Verify all unit tests pass before opening a pull request.
5. **Commit with Conventional Commits**:
   - Use clear commit messages (e.g., `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`).
6. **Submit a Pull Request**:
   - Push your branch to GitHub and open a PR against the `main` branch.
   - Describe what changed and why.

## Reporting Issues

If you find a bug or have an idea for an enhancement, please open an issue on GitHub with:
- Operating system, Python version, Java version, and Spark/Delta versions
- Full error message or stack trace
- Steps to reproduce
