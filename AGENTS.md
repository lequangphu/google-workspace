# AGENTS.md

## Overview
This repository contains automation agents for Google Workspace Drive workflows:
- **Google Apps Script** (clasp/Node.js): Server-side automation and Drive integration
- **Python** (uv): Data processing and analysis

## Prerequisites
- Node.js 16+
- Python 3.10+
- Google Cloud project with Drive API enabled
- Google Workspace account

## Directory Structure
- `/apps-script/` - Apps Script source files (.gs, .html) and `.clasp.json` config
  - See [`/apps-script/AGENTS.md`](/apps-script/AGENTS.md) for detailed setup and guidelines
- `/python/` - Python scripts with `pyproject.toml` (managed by uv)
  - See [`/python/AGENTS.md`](/python/AGENTS.md) for detailed setup and guidelines
- Root - `package.json` for clasp/npm dependencies, shared configs

## Quick Start
```bash
# Root setup
npm install -g @google/clasp
npm init -y  # if needed
clasp login

# Then follow subdirectory-specific guides in their AGENTS.md files
```

## Deployment
[Add deployment instructions]

## Environment Variables
[Add required env vars]

## Troubleshooting
[Add common issues]
