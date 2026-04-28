import * as vscode from "vscode";

export class StatusBar implements vscode.Disposable {
  private item: vscode.StatusBarItem;

  constructor() {
    this.item = vscode.window.createStatusBarItem(
      vscode.StatusBarAlignment.Left,
      100,
    );
    this.item.command = "ocelint.lintCurrentFile";
    this.item.hide();
  }

  setBusy(doc: vscode.TextDocument): void {
    this.item.text = "$(loading~spin) OCELint";
    this.item.tooltip = `Linting ${doc.uri.fsPath}`;
    this.item.show();
  }

  setError(): void {
    this.item.text = "$(error) OCELint";
    this.item.tooltip = "OCELint failed - click to retry";
    this.item.show();
  }

  refresh(
    doc: vscode.TextDocument | undefined,
    diag: vscode.DiagnosticCollection,
  ): void {
    if (!doc) {
      this.item.hide();
      return;
    }
    const ds = diag.get(doc.uri) ?? [];
    if (ds.length === 0) {
      this.item.text = "$(check) OCELint";
      this.item.tooltip = "No violations";
    } else {
      const errs = ds.filter(
        (d) => d.severity === vscode.DiagnosticSeverity.Error,
      ).length;
      const warns = ds.filter(
        (d) => d.severity === vscode.DiagnosticSeverity.Warning,
      ).length;
      this.item.text = `$(alert) ${ds.length} OCELint`;
      this.item.tooltip = `${errs} error(s), ${warns} warning(s)`;
    }
    this.item.show();
  }

  dispose(): void {
    this.item.dispose();
  }
}
