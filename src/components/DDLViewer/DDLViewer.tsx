import { useEffect, useRef, useState } from "react";
import Editor from "@monaco-editor/react";
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
          theme="vs-dark"
          options={{
            readOnly: true,
            minimap: { enabled: false },
            fontSize: 13,
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
