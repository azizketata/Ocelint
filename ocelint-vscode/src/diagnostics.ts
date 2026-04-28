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

export function sarifToDiagnostics(sarif: SarifLog): vscode.Diagnostic[] {
  const out: vscode.Diagnostic[] = [];
  const run = sarif.runs?.[0];
  if (!run?.results) {
    return out;
  }

  // Hardcoded range: every diagnostic lands on line 1 (col 0..1) because
  // ocelint's SARIF carries no region. Will be revisited when the CLI emits
  // physical offsets.
  const range = new vscode.Range(0, 0, 0, 1);

  for (const result of run.results) {
    const sev =
      SARIF_LEVEL_TO_SEVERITY[result.level ?? "warning"] ??
      vscode.DiagnosticSeverity.Warning;

    const messageText = result.message?.text ?? "(no message)";
    const logical = result.locations?.[0]?.logicalLocations?.[0]?.name;
    const message = logical ? `${messageText} [${logical}]` : messageText;

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

  out.sort((a, b) => a.severity - b.severity);
  return out;
}
