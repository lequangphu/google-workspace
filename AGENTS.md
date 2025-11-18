# AGENTS.md

## Overview
This repo agents automate Google Workspace workflows in Drive:
- Google Apps Script (Node.js via clasp): Workflow automation.
- Python (uv): Data analysis.

## Structure
- `/apps-script`: Apps Script files, `.clasp.json`.
- `/python`: Python scripts, `pyproject.toml` (uv).
- Root: `package.json` (clasp), shared configs.

## Setup
```bash
# Root
npm install -g @google/clasp
npm init -y  # if needed
clasp login
clasp clone <scriptId>  # in /apps-script
```