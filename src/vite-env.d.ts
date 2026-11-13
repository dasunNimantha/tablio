/// <reference types="vite/client" />

// Monaco's editor core entry ships no typings of its own; it exposes the
// same API as editor.api.
declare module "monaco-editor/esm/vs/editor/edcore.main.js" {
  export * from "monaco-editor/esm/vs/editor/editor.api";
}
