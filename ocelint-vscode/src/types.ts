// Minimal SARIF 2.1.0 subset matching the shape ocelint emits.
// See ocelint's src/ocelint/cli/__init__.py:209-256 for the producer.

export interface SarifLog {
  version: string;
  runs?: SarifRun[];
}

export interface SarifRun {
  tool?: { driver?: SarifDriver };
  invocations?: { executionSuccessful?: boolean }[];
  artifacts?: { location?: { uri?: string } }[];
  results?: SarifResult[];
}

export interface SarifDriver {
  name: string;
  version?: string;
  informationUri?: string;
  rules?: SarifRule[];
}

export interface SarifRule {
  id: string;
  shortDescription?: { text?: string };
  defaultConfiguration?: { level?: string };
}

export interface SarifResult {
  ruleId?: string;
  level?: string;
  message?: { text?: string };
  locations?: SarifLocation[];
}

export interface SarifLocation {
  physicalLocation?: { artifactLocation?: { uri?: string } };
  logicalLocations?: { name?: string }[];
}
