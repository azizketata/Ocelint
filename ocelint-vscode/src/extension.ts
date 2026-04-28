import * as vscode from "vscode";
import { getConfig } from "./config";
import { sarifToDiagnostics } from "./diagnostics";
import { OcelintNotFoundError, runOcelint } from "./runner";
import { StatusBar } from "./statusBar";

const OCEL_EXTENSIONS = new Set([
  ".json",
  ".jsonocel",
  ".xml",
  ".xmlocel",
  ".sqlite",
  ".sqlite3",
  ".db",
  ".sqliteocel",
]);

let diag: vscode.DiagnosticCollection;
let output: vscode.OutputChannel;
let statusBar: StatusBar;
const inFlight = new Map<string, AbortController>();

export function activate(ctx: vscode.ExtensionContext): void {
  diag = vscode.languages.createDiagnosticCollection("ocelint");
  output = vscode.window.createOutputChannel("OCELint");
  statusBar = new StatusBar();

  ctx.subscriptions.push(
    diag,
    output,
    statusBar,
    vscode.commands.registerCommand("ocelint.lintCurrentFile", lintActive),
    vscode.commands.registerCommand("ocelint.showOutput", () => output.show()),
    vscode.workspace.onDidOpenTextDocument((d) => {
      if (getConfig().lintOnOpen) {
        void lintDocument(d);
      }
    }),
    vscode.workspace.onDidSaveTextDocument((d) => {
      if (getConfig().lintOnSave) {
        void lintDocument(d);
      }
    }),
    vscode.workspace.onDidCloseTextDocument((d) => {
      diag.delete(d.uri);
      inFlight.get(d.uri.toString())?.abort();
    }),
    vscode.window.onDidChangeActiveTextEditor((ed) => {
      statusBar.refresh(ed?.document, diag);
    }),
  );

  // Lint already-open files at activation time
  for (const d of vscode.workspace.textDocuments) {
    void lintDocument(d);
  }
}

export function deactivate(): void {
  // subscriptions handle their own cleanup
}

function isOcelFile(doc: vscode.TextDocument): boolean {
  if (doc.uri.scheme !== "file") {
    return false;
  }
  const path = doc.uri.fsPath.toLowerCase();
  for (const ext of OCEL_EXTENSIONS) {
    if (path.endsWith(ext)) {
      return true;
    }
  }
  return false;
}

async function lintActive(): Promise<void> {
  const doc = vscode.window.activeTextEditor?.document;
  if (!doc) {
    void vscode.window.showInformationMessage("OCELint: no active editor.");
    return;
  }
  await lintDocument(doc, true);
}

async function lintDocument(
  doc: vscode.TextDocument,
  forced = false,
): Promise<void> {
  if (!isOcelFile(doc)) {
    if (forced) {
      void vscode.window.showWarningMessage(
        `OCELint: not an OCEL file: ${doc.uri.fsPath}`,
      );
    }
    return;
  }

  const cfg = getConfig();

  // Size guard: stat the file on disk because the in-memory text doc may
  // be partial for SQLite (binary) files.
  let sizeMB = 0;
  try {
    const stat = await vscode.workspace.fs.stat(doc.uri);
    sizeMB = stat.size / (1024 * 1024);
  } catch {
    // If stat fails the runner will surface the error.
  }
  if (sizeMB > cfg.maxFileSizeMB) {
    output.appendLine(
      `[skip] ${doc.uri.fsPath} is ${sizeMB.toFixed(1)} MB > ${cfg.maxFileSizeMB} MB`,
    );
    return;
  }

  const key = doc.uri.toString();
  inFlight.get(key)?.abort();
  const ac = new AbortController();
  inFlight.set(key, ac);

  statusBar.setBusy(doc);
  try {
    const sarif = await runOcelint(doc.uri.fsPath, cfg, ac.signal, output);
    if (ac.signal.aborted) {
      return;
    }
    const diags = sarifToDiagnostics(sarif, doc);
    diag.set(doc.uri, diags);
    statusBar.refresh(doc, diag);
  } catch (e) {
    if (ac.signal.aborted) {
      return;
    }
    handleError(e, doc);
  } finally {
    if (inFlight.get(key) === ac) {
      inFlight.delete(key);
    }
  }
}

function handleError(e: unknown, doc: vscode.TextDocument): void {
  if (e instanceof OcelintNotFoundError) {
    void vscode.window
      .showErrorMessage(
        "OCELint: ocelint executable not found. Install with `pip install ocelint`.",
        "Open Settings",
        "Install Docs",
      )
      .then((pick) => {
        if (pick === "Open Settings") {
          void vscode.commands.executeCommand(
            "workbench.action.openSettings",
            "ocelint.executablePath",
          );
        } else if (pick === "Install Docs") {
          void vscode.env.openExternal(
            vscode.Uri.parse("https://github.com/azizketata/ocelint#install"),
          );
        }
      });
    statusBar.setError();
    return;
  }

  const msg = e instanceof Error ? e.message : String(e);
  output.appendLine(`[error] ${doc.uri.fsPath}: ${msg}`);
  void vscode.window
    .showErrorMessage(
      `OCELint: ${truncate(msg, 200)}`,
      "Show Output",
    )
    .then((pick) => {
      if (pick === "Show Output") {
        output.show();
      }
    });
  diag.delete(doc.uri);
  statusBar.setError();
}

function truncate(s: string, n: number): string {
  return s.length <= n ? s : s.slice(0, n - 1) + "…";
}
