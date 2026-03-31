import { useState, useEffect, useCallback } from "react";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { Minus, X } from "lucide-react";
import "./TitleBar.css";

const appWindow = getCurrentWindow();

export function TitleBar() {
  const [maximized, setMaximized] = useState(false);

  useEffect(() => {
    appWindow.isMaximized().then(setMaximized);
    const unlisten = appWindow.onResized(() => {
      appWindow.isMaximized().then(setMaximized);
    });
    return () => {
      unlisten.then((fn) => fn());
    };
  }, []);

  const handleMinimize = useCallback(() => {
    appWindow.minimize();
  }, []);

  const handleToggleMaximize = useCallback(() => {
    appWindow.toggleMaximize();
  }, []);

  const handleClose = useCallback(() => {
    appWindow.close();
  }, []);

  return (
    <div className="titlebar" data-tauri-drag-region>
      <div className="titlebar-left" data-tauri-drag-region>
        <img
          src="/icon.png"
          alt="Tablio"
          className="titlebar-icon"
          draggable={false}
        />
        <span className="titlebar-title" data-tauri-drag-region>
          Tablio
        </span>
      </div>
      <div className="titlebar-controls">
        <button
          className="titlebar-btn titlebar-btn-minimize"
          onClick={handleMinimize}
        >
          <Minus size={14} strokeWidth={1.5} />
        </button>
        <button
          className="titlebar-btn titlebar-btn-maximize"
          onClick={handleToggleMaximize}
        >
          {maximized ? (
            <svg width="11" height="11" viewBox="0 0 11 11" fill="none" stroke="currentColor" strokeWidth="1.2">
              <rect x="0.5" y="2.5" width="8" height="8" rx="0.5" />
              <path d="M2.5 2.5V1a.5.5 0 0 1 .5-.5H10a.5.5 0 0 1 .5.5v7a.5.5 0 0 1-.5.5H8.5" />
            </svg>
          ) : (
            <svg width="11" height="11" viewBox="0 0 11 11" fill="none" stroke="currentColor" strokeWidth="1.2">
              <rect x="0.5" y="0.5" width="10" height="10" rx="0.5" />
            </svg>
          )}
        </button>
        <button
          className="titlebar-btn titlebar-btn-close"
          onClick={handleClose}
        >
          <X size={15} strokeWidth={1.5} />
        </button>
      </div>
    </div>
  );
}
