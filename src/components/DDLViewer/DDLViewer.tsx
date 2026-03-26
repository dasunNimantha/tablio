import { useCallback, useEffect, useRef, useState } from "react";
import Editor, { type BeforeMount, type Monaco } from "@monaco-editor/react";
import { api } from "../../lib/tauri";
import { Loader2, Copy, Check } from "lucide-react";
import "./DDLViewer.css";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  objectName: string;
  objectType: string;
}

function getCssVar(name: string, fallback: string) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
}

export function DDLViewer({
  connectionId,
  database,
  schema,
  objectName,
  objectType,
}: Props) {
  const [ddl, setDdl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [monacoTheme, setMonacoTheme] = useState<string>(() =>
    document.documentElement.getAttribute("data-theme") === "light"
      ? "tablio-light-0"
      : "tablio-dark-0"
  );
  const monacoRef = useRef<Monaco | null>(null);
  const themeVersionRef = useRef(0);

  useEffect(() => {
    let alive = true;
    const fetchDdl = async () => {
      setLoading(true);
      setError(null);
      try {
        const result = await api.getDdl({
          connection_id: connectionId,
          database,
          schema,
          object_name: objectName,
          object_type: objectType,
        });
        if (alive) setDdl(result);
      } catch (e) {
        if (alive) setError(String(e));
      } finally {
        if (alive) setLoading(false);
      }
    };
    fetchDdl();
    return () => { alive = false; };
  }, [connectionId, database, schema, objectName, objectType]);

  const copyTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (copyTimerRef.current) clearTimeout(copyTimerRef.current);
    };
  }, []);

  const syncTheme = useCallback(() => {
    const monaco = monacoRef.current;
    if (!monaco) return;

    const ver = ++themeVersionRef.current;
    const bg = getCssVar("--bg-primary", "#1e1e1e");
    const bgSurface = getCssVar("--bg-surface", "#252526");
    const textPrimary = getCssVar("--text-primary", "#d4d4d4");
    const textMuted = getCssVar("--text-muted", "#6e6e7c");
    const accent = getCssVar("--accent", "#6398ff");
    const isLight = document.documentElement.getAttribute("data-theme") === "light";

    const colors = {
      "editor.background": bg,
      "editor.foreground": textPrimary,
      "editorLineNumber.foreground": textMuted,
      "editor.selectionBackground": accent + "33",
      "editorWidget.background": bgSurface,
      "editorWidget.border": bgSurface,
    };

    const darkName = `tablio-dark-${ver}`;
    const lightName = `tablio-light-${ver}`;

    monaco.editor.defineTheme(darkName, {
      base: "vs-dark",
      inherit: true,
      rules: [
        { token: "string.sql", foreground: "98c379" },
        { token: "string", foreground: "98c379" },
        { token: "keyword", foreground: "6daaef" },
        { token: "number", foreground: "d19a66" },
        { token: "comment", foreground: "6a737d", fontStyle: "italic" },
        { token: "operator", foreground: "c8ccd4" },
      ],
      colors,
    });

    monaco.editor.defineTheme(lightName, {
      base: "vs",
      inherit: true,
      rules: [
        { token: "string.sql", foreground: "50a14f" },
        { token: "string", foreground: "50a14f" },
        { token: "keyword", foreground: "4078f2" },
        { token: "number", foreground: "986801" },
        { token: "comment", foreground: "a0a1a7", fontStyle: "italic" },
        { token: "operator", foreground: "383a42" },
      ],
      colors,
    });

    setMonacoTheme(isLight ? lightName : darkName);
  }, []);

  useEffect(() => {
    const observer = new MutationObserver(() => syncTheme());
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme", "style"],
    });
    return () => observer.disconnect();
  }, [syncTheme]);

  const handleEditorBeforeMount: BeforeMount = (monaco) => {
    monacoRef.current = monaco;
    syncTheme();
  };

  const handleCopy = async () => {
    if (ddl) {
      await navigator.clipboard.writeText(ddl);
      setCopied(true);
      if (copyTimerRef.current) clearTimeout(copyTimerRef.current);
      copyTimerRef.current = setTimeout(() => setCopied(false), 2000);
    }
  };

  if (loading) {
    return (
      <div className="ddl-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading DDL...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="ddl-error">
        <p>{error}</p>
      </div>
    );
  }

  return (
    <div className="ddl-viewer">
      <div className="ddl-toolbar">
        <span className="ddl-object-name">
          {objectType}: {schema}.{objectName}
        </span>
        <div style={{ flex: 1 }} />
        <button className="btn-ghost" onClick={handleCopy}>
          {copied ? <Check size={14} /> : <Copy size={14} />}
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <div className="ddl-editor-wrapper">
        <Editor
          height="100%"
          defaultLanguage="sql"
          value={ddl || ""}
          beforeMount={handleEditorBeforeMount}
          theme={monacoTheme}
          options={{
            readOnly: true,
            minimap: { enabled: false },
            fontSize: 15,
            fontFamily: "var(--font-mono)",
            lineNumbers: "on",
            scrollBeyondLastLine: false,
            wordWrap: "on",
            automaticLayout: true,
            tabSize: 2,
            padding: { top: 8 },
          }}
        />
      </div>
    </div>
  );
}
