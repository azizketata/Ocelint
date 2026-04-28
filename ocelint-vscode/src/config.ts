import * as vscode from "vscode";

export interface OcelintConfig {
  executablePath: string;
  lintOnSave: boolean;
  lintOnOpen: boolean;
  maxFileSizeMB: number;
  configPath: string;
}

export function getConfig(): OcelintConfig {
  const c = vscode.workspace.getConfiguration("ocelint");
  return {
    executablePath: c.get<string>("executablePath", "ocelint"),
    lintOnSave: c.get<boolean>("lintOnSave", true),
    lintOnOpen: c.get<boolean>("lintOnOpen", true),
    maxFileSizeMB: c.get<number>("maxFileSizeMB", 50),
    configPath: c.get<string>("configPath", ""),
  };
}
