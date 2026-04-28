import * as vscode from "vscode";
import type { SarifLog } from "./types";

const SARIF_LEVEL_TO_SEVERITY: Record<string, vscode.DiagnosticSeverity> = {
  error: vscode.DiagnosticSeverity.Error,
  warning: vscode.DiagnosticSeverity.Warning,
  note: vscode.DiagnosticSeverity.Information,
  none: vscode.DiagnosticSeverity.Hint,
};

const README_BASE =
  "https://github.com/azizketata/ocelint/blob/main/README.md";

const FALLBACK_RANGE = new vscode.Range(0, 0, 0, 1);

export function sarifToDiagnostics(
  sarif: SarifLog,
  doc: vscode.TextDocument,
): vscode.Diagnostic[] {
  const out: vscode.Diagnostic[] = [];
  const run = sarif.runs?.[0];
  if (!run?.results) {
    return out;
  }

  // Cache document text once; multiple results may want to look up ids.
  const text = doc.getText();
  const fsPath = doc.uri.fsPath.toLowerCase();
  const isXml = /\.xml(?:ocel)?$/i.test(fsPath);
  const isSqlite = /\.(sqlite3?|db|sqliteocel)$/i.test(fsPath);

  for (const result of run.results) {
    const sev =
      SARIF_LEVEL_TO_SEVERITY[result.level ?? "warning"] ??
      vscode.DiagnosticSeverity.Warning;

    const messageText = result.message?.text ?? "(no message)";
    const logical = result.locations?.[0]?.logicalLocations?.[0]?.name;
    const message = logical ? `${messageText} [${logical}]` : messageText;

    const range = isSqlite
      ? FALLBACK_RANGE
      : rangeForLocation(doc, text, isXml, logical);

    const diag = new vscode.Diagnostic(range, message, sev);
    diag.source = "ocelint";
    if (result.ruleId) {
      diag.code = {
        value: result.ruleId,
        target: vscode.Uri.parse(
          `${README_BASE}#${result.ruleId.toLowerCase()}`,
        ),
      };
    }
    out.push(diag);
  }

  out.sort((a, b) => {
    if (a.severity !== b.severity) return a.severity - b.severity;
    return a.range.start.line - b.range.start.line;
  });
  return out;
}

/**
 * Resolve a semantic location like `events[eid=e1]` or
 * `event_object[oid=o1,eid=e1]` to a physical Range by searching the document
 * for the id. Falls back to line 1 when the id can't be located, when the
 * location string carries no id, or when the file is binary (SQLite).
 */
function rangeForLocation(
  doc: vscode.TextDocument,
  text: string,
  isXml: boolean,
  location: string | undefined,
): vscode.Range {
  if (!location) return FALLBACK_RANGE;

  // Pull out the first identifier-like value: eid, oid, source_oid, target_oid.
  const m = location.match(/\[(?:eid|oid|source_oid|target_oid)=([^,\]]+)/);
  if (!m) return FALLBACK_RANGE;
  const id = m[1].trim();
  if (!id) return FALLBACK_RANGE;

  const escaped = escapeRegex(id);
  // JSON: `"id": "e1"` (allow whitespace around colon).
  // XML:  `id="e1"` or `object-id="e1"` (allow single or double quotes).
  const pattern = isXml
    ? new RegExp(`\\b(?:object-)?id\\s*=\\s*["']${escaped}["']`)
    : new RegExp(`"id"\\s*:\\s*"${escaped}"`);

  const match = pattern.exec(text);
  if (!match) return FALLBACK_RANGE;

  // Highlight just the id value, not the surrounding key/quotes.
  const idOffset = match.index + match[0].indexOf(id);
  const start = doc.positionAt(idOffset);
  const end = doc.positionAt(idOffset + id.length);
  return new vscode.Range(start, end);
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
