# OCELint for VS Code

VS Code integration for [ocelint](https://github.com/azizketata/ocelint), the semantic linter for OCEL 2.0 (Object-Centric Event Log) files.

When you open or save an `.json` / `.jsonocel` / `.xml` / `.xmlocel` / `.sqlite` / `.sqlite3` / `.db` / `.sqliteocel` file, the extension runs `ocelint lint --format sarif` on it and surfaces the results in the **Problems** panel.

## Requirements

The `ocelint` Python CLI must be on your PATH:

```bash
pip install ocelint
ocelint version
```

If `ocelint` lives elsewhere, set `ocelint.executablePath` in VS Code settings.

## Features

- **Diagnostics** in the Problems panel for all 53 ocelint rules.
- **Status bar** item with the violation count for the active OCEL file.
- **Manual command**: `OCELint: Lint current file` (Command Palette).
- **Lint on open / save** (toggleable).

### Known limitations

- All diagnostics land on **line 1** of the file — ocelint's SARIF output is currently file-scoped (no `region.startLine`). Locations are surfaced inside the diagnostic message as a `[bracketed semantic anchor]` like `[events[eid=e1]]`.
- The extension shells out to the CLI on every save. Files larger than `ocelint.maxFileSizeMB` (default 50 MB) are skipped.

## Settings

| Setting | Default | Purpose |
| --- | --- | --- |
| `ocelint.executablePath` | `ocelint` | Path to the ocelint binary. Use absolute path if not on PATH. |
| `ocelint.lintOnSave` | `true` | Re-lint when an OCEL file is saved. |
| `ocelint.lintOnOpen` | `true` | Lint when an OCEL file is opened. |
| `ocelint.maxFileSizeMB` | `50` | Skip files larger than this. |
| `ocelint.configPath` | `""` | Optional `--config` path forwarded to ocelint (e.g. a `pyproject.toml`). |

## Build from source

```bash
cd ocelint-vscode
npm install
npm run build         # esbuild → dist/extension.js
npm run package       # vsce → ocelint-0.1.0.vsix
code --install-extension ocelint-0.1.0.vsix
```

For development, press **F5** in this folder to launch an Extension Development Host.

## License

MIT.
