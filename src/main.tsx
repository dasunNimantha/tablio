import React from "react";
import ReactDOM from "react-dom/client";
import "./lib/monacoSetup";
import App from "./App";
import "./styles/global.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
