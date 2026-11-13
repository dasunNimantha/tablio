// Bundle Monaco with the app instead of letting @monaco-editor/react pull
// it from cdn.jsdelivr.net at runtime. Loading editor code from a CDN
// would need a remote script-src in the Content Security Policy, breaks
// the editor offline, and runs whatever that URL serves with full IPC
// access. Import this module once, before any <Editor> mounts.
//
// Only the editor core and the SQL grammar are pulled in: the full
// `monaco-editor` entry would also bundle the TypeScript/CSS/HTML/JSON
// language services and ~80 grammars the app never uses.
import * as monaco from "monaco-editor/esm/vs/editor/edcore.main.js";
import "monaco-editor/esm/vs/basic-languages/sql/sql.contribution.js";
import EditorWorker from "monaco-editor/esm/vs/editor/editor.worker.js?worker";
import { loader } from "@monaco-editor/react";

self.MonacoEnvironment = {
  // SQL has no language worker; everything uses the generic editor worker
  // (served from 'self', so the CSP needs no blob: workers).
  getWorker: () => new EditorWorker(),
};

// A module namespace object is frozen, but the editor components cache
// per-instance state on the monaco object (as they could on the AMD
// global the CDN build provided), so hand the loader a plain copy.
loader.config({ monaco: { ...monaco } });
