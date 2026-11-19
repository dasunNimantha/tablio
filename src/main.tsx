import React, { useLayoutEffect } from "react";
import ReactDOM from "react-dom/client";
import { getCurrentWindow } from "@tauri-apps/api/window";
import App from "./App";
import "./styles/global.css";

// The window is created hidden (tauri.conf.json). Show it once the first
// render has committed. requestAnimationFrame can't be used for this: a
// hidden WebKitGTK webview never runs frame callbacks, so the window stayed
// hidden until the 3 s fallback in lib.rs.
function ShowWindow() {
  useLayoutEffect(() => {
    if ((window as any).__TAURI_INTERNALS__) {
      getCurrentWindow().show().catch(() => {});
    }
  }, []);
  return null;
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ShowWindow />
    <App />
  </React.StrictMode>
);
