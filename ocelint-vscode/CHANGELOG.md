# Changelog

All notable changes to the OCELint VS Code extension will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-04-28

### Added
- Marketplace icon (128x128 teal "OC" with a green checkmark badge).

## [0.1.1] - 2026-04-28

### Changed
- Diagnostics now point at the actual line of the offending event/object
  by searching the document text for the id named in the SARIF logical
  location (e.g. `events[eid=e1]` resolves to the line containing
  `"id": "e1"`). Aggregate-style violations without an id still land on
  line 1.
- Diagnostics are now sorted by severity, then by line, in the Problems
  panel.

## [0.1.0] - 2026-04-28

### Added
- Initial release.
- Diagnostics integration: runs `ocelint lint --format sarif` on file open and save, parses the SARIF output, and surfaces results in the Problems panel.
- Manual `OCELint: Lint current file` command.
- Status bar item with the active editor's violation count.
- Settings: `executablePath`, `lintOnSave`, `lintOnOpen`, `maxFileSizeMB`, `configPath`.
- Recognises `.json`, `.jsonocel`, `.xml`, `.xmlocel`, `.sqlite`, `.sqlite3`, `.db`, `.sqliteocel`.

### Known issues
- All diagnostics appear on line 1 because ocelint's SARIF output is currently file-scoped; semantic location is surfaced inside the diagnostic message.
- Configuration changes require a window reload to take effect.
