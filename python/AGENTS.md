# AGENTS.md (/python)

## Overview
Python scripts for data processing and analysis in Google Workspace workflows.

## Requirements
- Python 3.10+
- uv (install: `curl -LsSf https://astral.sh/uv/install.sh | sh`)

## Python Tooling

Always use `uv` for package management:
- `uv add <package>` - Add dependencies to pyproject.toml
- `uv sync` - Install from pyproject.toml
- `uv run <script.py>` - Run scripts in the project environment
- `uv venv` - Create virtual environments

Do NOT use `pip install` or `poetry`.

## Setup
```bash
cd python
uv sync  # Install project dependencies
source .venv/bin/activate  # Activate environment (optional)
```

## Running Scripts
```bash
uv run script_name.py
```

## Scripts
[Add descriptions of available scripts]
