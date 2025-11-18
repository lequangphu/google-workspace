# AGENTS.md (/python)

## Python Tooling Guideline

When working with Python projects or tasks:

- Use `uv` exclusively for package management, virtual environments, and dependency resolution.
- Prefer `uv pip install` over `pip install`.
- Create environments with `uv venv`.
- Add dependencies with `uv add <package>`.
- Sync with `uv sync`.
- Run scripts via `uv run <script>`.

This ensures speed and consistency. Install uv via `curl -LsSf https://astral.sh/uv/install.sh | sh`.