import { spawn } from "node:child_process";
import * as vscode from "vscode";
import type { OcelintConfig } from "./config";
import type { SarifLog } from "./types";

export class OcelintNotFoundError extends Error {
  constructor() {
    super("ocelint executable not found");
    this.name = "OcelintNotFoundError";
  }
}

export async function runOcelint(
  filePath: string,
  cfg: OcelintConfig,
  signal: AbortSignal,
  output: vscode.OutputChannel,
): Promise<SarifLog> {
  const args = ["lint", filePath, "--format", "sarif"];
  if (cfg.configPath) {
    args.push("--config", cfg.configPath);
  }

  return new Promise((resolve, reject) => {
    let proc;
    try {
      proc = spawn(cfg.executablePath, args, { signal });
    } catch (e: unknown) {
      const code = (e as { code?: string } | undefined)?.code;
      if (code === "ENOENT") {
        return reject(new OcelintNotFoundError());
      }
      return reject(e);
    }

    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", (chunk: Buffer) => (stdout += chunk.toString()));
    proc.stderr.on("data", (chunk: Buffer) => (stderr += chunk.toString()));

    proc.on("error", (err: NodeJS.ErrnoException) => {
      if (err.code === "ENOENT") {
        reject(new OcelintNotFoundError());
      } else if (err.name === "AbortError") {
        reject(new Error("cancelled"));
      } else {
        reject(err);
      }
    });

    proc.on("close", (code: number | null) => {
      if (signal.aborted) {
        return reject(new Error("cancelled"));
      }
      if (stderr.trim()) {
        output.appendLine(`[stderr] ${stderr.trim()}`);
      }
      // Exit codes from ocelint:
      //   0 = clean / info-only, 1 = warn, 2 = error or parse failure.
      // Parse failure has empty stdout; rule errors still produce SARIF.
      if (code === 2 && !stdout.trim()) {
        return reject(new Error(stderr.trim() || "ocelint exited with code 2"));
      }
      try {
        resolve(JSON.parse(stdout) as SarifLog);
      } catch (e) {
        reject(
          new Error(
            `Failed to parse SARIF: ${(e as Error).message}\nstderr: ${stderr}`,
          ),
        );
      }
    });
  });
}
