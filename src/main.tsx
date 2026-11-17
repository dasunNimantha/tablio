import React from "react";
import ReactDOM from "react-dom/client";
import { getCurrentWindow } from "@tauri-apps/api/window";
import App from "./App";
import "./styles/global.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);

// The window is created hidden (tauri.conf.json); show it once React's
// first frame has painted so startup doesn't flash the empty webview.
if ((window as any).__TAURI_INTERNALS__) {
  requestAnimationFrame(() =>
    requestAnimationFrame(() => {
      getCurrentWindow().show().catch(() => {});
    }),
  );
}
