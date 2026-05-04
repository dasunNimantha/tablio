import { useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { createPortal } from "react-dom";
import {
  resolveSshPassphrase,
  useConnectionStore,
  withHostKeyMismatchRetry,
} from "../stores/connectionStore";
import { api, ConnectionConfig, SshAuthMethod } from "../lib/tauri";
import {
  AlertCircle,
  CheckCircle,
  ChevronDown,
  Database,
  Folder,
  Key,
  Loader2,
  Lock,
  Shield,
  SlidersHorizontal,
  UserRound,
  X,
  XCircle,
} from "lucide-react";
import { open as openFileDialog } from "@tauri-apps/plugin-dialog";
import "./ConnectionDialog.css";

const MAX_FOLDER_NAME_LENGTH = 50;

const COLORS = [
  "#89b4fa", "#a6e3a1", "#f9e2af", "#f38ba8",
  "#cba6f7", "#89dceb", "#fab387", "#94e2d5",
];

const DB_TYPES = [
  { value: "postgres" as const, label: "PostgreSQL", short: "PG", defaultPort: 5432, defaultUser: "postgres", accent: "#89b4fa" },
  { value: "mysql" as const, label: "MySQL", short: "MY", defaultPort: 3306, defaultUser: "root", accent: "#f9e2af" },
  { value: "sqlite" as const, label: "SQLite", short: "SQ", defaultPort: 0, defaultUser: "", accent: "#94e2d5" },
  { value: "mariadb" as const, label: "MariaDB", short: "MA", defaultPort: 3306, defaultUser: "root", accent: "#fab387" },
  { value: "cockroachdb" as const, label: "CockroachDB", short: "CR", defaultPort: 26257, defaultUser: "root", accent: "#cba6f7" },
  { value: "tidb" as const, label: "TiDB", short: "TI", defaultPort: 4000, defaultUser: "root", accent: "#f38ba8" },
  { value: "cassandra" as const, label: "Cassandra / ScyllaDB", short: "CS", defaultPort: 9042, defaultUser: "cassandra", accent: "#a6e3a1" },
  { value: "mssql" as const, label: "Microsoft SQL Server", short: "MS", defaultPort: 1433, defaultUser: "sa", accent: "#7aa2f7" },
];

/** Set of every per-driver default username, used to detect whether the
 *  current `user` value is "still a default" so we can safely swap it
 *  when the user changes DB type. A username they typed by hand is
 *  preserved across DB-type changes. */
const DEFAULT_USERNAMES = new Set(
  DB_TYPES.map((d) => d.defaultUser).filter((u) => u.length > 0),
);

type ValidationField =
  | "name"
  | "host"
  | "port"
  | "user"
  | "database"
  | "ssh_host"
  | "ssh_port"
  | "ssh_user"
  | "ssh_password"
  | "ssh_key_path";
type ValidationErrors = Partial<Record<ValidationField, string>>;

type DialogSectionId =
  | "general"
  | "authentication"
  | "security"
  | "ssh"
  | "advanced";

interface DialogSection {
  id: DialogSectionId;
  label: string;
  description: string;
  icon: ReactNode;
}

const SECTION_FIELDS: Record<DialogSectionId, ValidationField[]> = {
  // General owns the basic identity *and* connection target — these
  // typically fit on one screen and splitting them across two nav items
  // wasted vertical space without adding clarity.
  general: ["name", "host", "port", "database"],
  authentication: ["user"],
  security: [],
  ssh: ["ssh_host", "ssh_port", "ssh_user", "ssh_password", "ssh_key_path"],
  advanced: [],
};

function getConnectionDialogSections(
  isSqlite: boolean,
  supportsTlsOptions: boolean,
): DialogSection[] {
  const sections: DialogSection[] = [
    {
      id: "general",
      label: "General",
      description: isSqlite
        ? "Name, type, folder and database file"
        : "Name, type and connection target",
      icon: <Database size={16} />,
    },
  ];

  if (!isSqlite) {
    sections.push(
      {
        id: "authentication",
        label: "Authentication",
        description: "User credentials and future auth methods",
        icon: <UserRound size={16} />,
      },
      {
        id: "security",
        label: "Security",
        description: supportsTlsOptions ? "TLS and certificates" : "Driver-managed transport",
        icon: <Shield size={16} />,
      },
      {
        id: "ssh",
        label: "SSH Tunnel",
        description: "Bastion host and SSH auth",
        icon: <Key size={16} />,
      },
    );
  }

  sections.push({
    id: "advanced",
    label: "Advanced",
    description: "Parameters and driver options",
    icon: <SlidersHorizontal size={16} />,
  });

  return sections;
}

function getSectionErrorCount(
  sectionId: DialogSectionId,
  errors: ValidationErrors,
): number {
  return SECTION_FIELDS[sectionId].filter((field) => !!errors[field]).length;
}

function getFirstInvalidSection(
  errors: ValidationErrors,
  sections: DialogSection[],
): DialogSectionId | null {
  return sections.find((section) => getSectionErrorCount(section.id, errors) > 0)?.id ?? null;
}

function formatConnectionTestError(error: unknown, form: ConnectionConfig): string {
  const raw = String(error).trim();
  const dbInfo = DB_TYPES.find((d) => d.value === form.db_type);
  const dbLabel = dbInfo?.label ?? "database";
  const user = form.user?.trim();

  const passwordFailure = raw.match(/password authentication failed for user "([^"]+)"/i);
  if (passwordFailure) {
    return `Database authentication failed for ${dbLabel} user "${passwordFailure[1]}". Check the username and password in Authentication.`;
  }

  if (/error returned from database:/i.test(raw)) {
    return `Database connection failed: ${raw.replace(/error returned from database:\s*/i, "")}`;
  }

  if (/ssh|known host|host key|agent|identity file|bastion/i.test(raw)) {
    return `SSH tunnel failed${form.ssh_host ? ` for ${form.ssh_host}` : ""}: ${raw}`;
  }

  if (user && /authentication|login/i.test(raw)) {
    return `Connection failed for ${dbLabel} user "${user}": ${raw}`;
  }

  return raw || "Connection test failed.";
}

function looksLikeSshConfigAlias(value: string): boolean {
  const host = value.trim();
  if (!host) return false;
  if (host === "localhost") return false;
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(host)) return false;
  if (/^[\da-f:]+$/i.test(host) && host.includes(":")) return false;
  return !host.includes(".");
}

function FormField({
  label,
  error,
  children,
  className = "",
  style,
  description,
}: {
  label: string;
  error?: string;
  children: ReactNode;
  className?: string;
  style?: CSSProperties;
  description?: string;
}) {
  return (
    <div
      className={`form-group${className ? ` ${className}` : ""}${error ? " form-group--error" : ""}`}
      style={style}
    >
      <label>{label}</label>
      {description && <div className="form-field-description">{description}</div>}
      {children}
      {error && <div className="field-error">{error}</div>}
    </div>
  );
}

function SectionCard({
  title,
  description,
  children,
  action,
}: {
  title: string;
  description: string;
  children: ReactNode;
  /** Optional control rendered on the right of the heading row, e.g.
   *  the SSH section's enable toggle. */
  action?: ReactNode;
}) {
  return (
    <section className="connection-section-card" aria-label={title}>
      <div className="connection-section-heading">
        <div className="connection-section-heading__text">
          <h3>{title}</h3>
          <p>{description}</p>
        </div>
        {action && <div className="connection-section-heading__action">{action}</div>}
      </div>
      <div className="connection-section-fields">{children}</div>
    </section>
  );
}

function ConnectionDialogNav({
  sections,
  activeSection,
  errors,
  showErrors,
  onSelect,
}: {
  sections: DialogSection[];
  activeSection: DialogSectionId;
  errors: ValidationErrors;
  showErrors: boolean;
  onSelect: (sectionId: DialogSectionId) => void;
}) {
  return (
    <nav className="connection-dialog-nav" aria-label="Connection settings sections">
      {sections.map((section) => {
        const errorCount = showErrors ? getSectionErrorCount(section.id, errors) : 0;
        return (
          <button
            key={section.id}
            type="button"
            className={`connection-nav-item${
              activeSection === section.id ? " connection-nav-item--active" : ""
            }${errorCount > 0 ? " connection-nav-item--error" : ""}`}
            onClick={() => onSelect(section.id)}
            data-section={section.id}
          >
            <span className="connection-nav-item__icon">{section.icon}</span>
            <span className="connection-nav-item__text">
              <span className="connection-nav-item__label">{section.label}</span>
              <span className="connection-nav-item__description">{section.description}</span>
            </span>
            {errorCount > 0 && (
              <span className="connection-nav-item__error" title={`${errorCount} validation error${errorCount === 1 ? "" : "s"}`}>
                <AlertCircle size={13} />
                {errorCount}
              </span>
            )}
          </button>
        );
      })}
    </nav>
  );
}

function ColorPicker({
  value,
  onChange,
}: {
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <div className="color-picker">
      {COLORS.map((c) => (
        <button
          key={c}
          type="button"
          className={`color-dot ${value === c ? "active" : ""}`}
          style={{ background: c }}
          onClick={() => onChange(c)}
          aria-label={`Use colour ${c}`}
        />
      ))}
    </div>
  );
}

export function normalizeConnectionForm(form: ConnectionConfig): ConnectionConfig {
  const sshEnabled = !!form.ssh_enabled;
  const sshAuth = form.ssh_auth_method ?? "password";
  const promptPassphrase =
    sshEnabled && sshAuth === "identityfile" && !!form.ssh_prompt_passphrase;

  return {
    ...form,
    name: form.name.trim(),
    host: form.db_type === "sqlite" ? "" : form.host.trim(),
    user: form.db_type === "sqlite" ? form.user : form.user.trim(),
    database: form.database.trim(),
    group: form.group?.trim() ? form.group.trim().slice(0, MAX_FOLDER_NAME_LENGTH) : null,
    ssh_enabled: sshEnabled,
    ssh_host: sshEnabled ? (form.ssh_host ?? "").trim() : "",
    ssh_port: sshEnabled ? form.ssh_port ?? 22 : 22,
    ssh_user: sshEnabled ? (form.ssh_user ?? "").trim() : "",
    ssh_auth_method: sshEnabled ? sshAuth : "password",
    // Identity-file path only meaningful with identityfile auth.
    ssh_key_path: sshEnabled && sshAuth === "identityfile" ? (form.ssh_key_path ?? "").trim() : "",
    // ssh-agent never reads a password / passphrase; clear any stored value
    // so it isn't accidentally persisted across an auth-method switch.
    ssh_password:
      sshEnabled && sshAuth !== "agent" && !promptPassphrase ? form.ssh_password ?? "" : "",
    ssh_prompt_passphrase: promptPassphrase,
  };
}

export function validateConnectionForm(
  form: ConnectionConfig,
  existingConnections: ConnectionConfig[],
): ValidationErrors {
  const errors: ValidationErrors = {};
  const normalized = normalizeConnectionForm(form);

  if (!normalized.name) {
    errors.name = "Connection name is required.";
  } else {
    const duplicateName = existingConnections.some(
      (conn) =>
        conn.id !== normalized.id &&
        conn.name.trim().toLowerCase() === normalized.name.toLowerCase(),
    );
    if (duplicateName) {
      errors.name = "A connection with this name already exists.";
    }
  }

  // SSH validation runs for every db type that uses TCP. SQLite is local
  // to the user's machine so SSH never applies; we just ignore the toggle
  // there. The shared SSH block must come before the early returns below
  // so it still runs for cassandra and the default branch.
  if (normalized.db_type !== "sqlite" && normalized.ssh_enabled) {
    if (!normalized.ssh_host) {
      errors.ssh_host = "SSH host is required.";
    }
    if (
      !Number.isInteger(normalized.ssh_port ?? NaN) ||
      (normalized.ssh_port ?? 0) < 1 ||
      (normalized.ssh_port ?? 0) > 65535
    ) {
      errors.ssh_port = "SSH port must be between 1 and 65535.";
    }
    if (!normalized.ssh_user) {
      errors.ssh_user = "SSH username is required.";
    }
    if (normalized.ssh_auth_method === "identityfile") {
      if (!normalized.ssh_key_path) {
        errors.ssh_key_path = "Identity file path is required.";
      }
    } else if (normalized.ssh_auth_method === "agent") {
      // ssh-agent supplies the credential at connect time; nothing to
      // validate here. The backend surfaces a clear error if SSH_AUTH_SOCK
      // is unset or the agent has no identities loaded.
    } else if (!normalized.ssh_password) {
      // Password auth: require a password (only when the user opted to
      // store it).
      errors.ssh_password = "SSH password is required.";
    }
  }

  if (normalized.db_type === "sqlite") {
    if (!normalized.database) {
      errors.database = "Database file path is required.";
    }
    return errors;
  }

  if (normalized.db_type === "cassandra") {
    if (!normalized.host) {
      errors.host = "Host is required.";
    }
    if (!Number.isInteger(normalized.port) || normalized.port < 1 || normalized.port > 65535) {
      errors.port = "Port must be between 1 and 65535.";
    }
    return errors;
  }

  if (!normalized.host) {
    errors.host = "Host is required.";
  }

  if (!Number.isInteger(normalized.port) || normalized.port < 1 || normalized.port > 65535) {
    errors.port = "Port must be between 1 and 65535.";
  }

  if (!normalized.user) {
    errors.user = "Username is required.";
  }

  return errors;
}

function DbTypeDropdown({
  value,
  onChange,
  className,
}: {
  value: ConnectionConfig["db_type"];
  onChange: (v: ConnectionConfig["db_type"]) => void;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const selected = DB_TYPES.find((d) => d.value === value)!;

  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  return (
    <div className={`form-group${className ? ` ${className}` : ""}`}>
      <label>Database Type</label>
      <div className="db-dropdown" ref={ref}>
        <button
          type="button"
          className="db-dropdown-trigger"
          onClick={() => setOpen((o) => !o)}
          aria-expanded={open}
        >
          <span className="db-dropdown-value">{selected.label}</span>
          <ChevronDown size={14} className={`db-dropdown-chevron ${open ? "open" : ""}`} />
        </button>
        {open && (
          <ul className="db-dropdown-menu">
            {DB_TYPES.map((dt) => (
              <li key={dt.value}>
                <button
                  type="button"
                  className={`db-dropdown-item ${dt.value === value ? "active" : ""}`}
                  onClick={() => {
                    onChange(dt.value);
                    setOpen(false);
                  }}
                >
                  <span className="db-dropdown-item-label">{dt.label}</span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function GroupInput({
  value,
  onChange,
  connections,
}: {
  value: string;
  onChange: (v: string) => void;
  connections: ConnectionConfig[];
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [dropdownStyle, setDropdownStyle] = useState<{
    top: number;
    left: number;
    width: number;
    maxHeight: number;
  } | null>(null);

  const existingGroups = useMemo(() => {
    const seen = new Map<string, string>();
    for (const c of connections) {
      const g = c.group?.trim();
      if (g && !seen.has(g.toLowerCase())) seen.set(g.toLowerCase(), g);
    }
    try {
      const raw = localStorage.getItem("tablio-empty-folders");
      if (raw) {
        for (const g of JSON.parse(raw) as string[]) {
          if (g && !seen.has(g.toLowerCase())) seen.set(g.toLowerCase(), g);
        }
      }
    } catch {}
    return Array.from(seen.values()).sort((a, b) => a.localeCompare(b));
  }, [connections]);

  const filtered = useMemo(() => {
    if (!value.trim()) return existingGroups;
    const lower = value.toLowerCase();
    return existingGroups.filter((g) => g.toLowerCase().includes(lower));
  }, [existingGroups, value]);

  useEffect(() => {
    if (!open) return;

    const updatePosition = () => {
      if (!inputRef.current) return;
      const rect = inputRef.current.getBoundingClientRect();
      const desiredWidth = Math.min(Math.max(rect.width - 56, 180), 240);
      const left = Math.min(rect.left, window.innerWidth - desiredWidth - 8);
      const availableBelow = Math.max(120, window.innerHeight - rect.bottom - 12);
      setDropdownStyle({
        top: rect.bottom + 4,
        left: Math.max(8, left),
        width: desiredWidth,
        maxHeight: Math.min(180, availableBelow),
      });
    };

    updatePosition();

    const handleClick = (e: MouseEvent) => {
      const target = e.target as Node;
      if (
        ref.current?.contains(target) ||
        dropdownRef.current?.contains(target)
      ) {
        return;
      }
      setOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    window.addEventListener("resize", updatePosition);
    window.addEventListener("scroll", updatePosition, true);
    return () => {
      document.removeEventListener("mousedown", handleClick);
      window.removeEventListener("resize", updatePosition);
      window.removeEventListener("scroll", updatePosition, true);
    };
  }, [open]);

  return (
    <div className="form-group group-input-wrapper" ref={ref}>
      <label>Group (optional)</label>
      <input
        ref={inputRef}
        value={value}
        onChange={(e) => {
          onChange(e.target.value.slice(0, MAX_FOLDER_NAME_LENGTH));
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        placeholder="e.g. Production, Development"
        autoComplete="off"
        maxLength={MAX_FOLDER_NAME_LENGTH}
      />
      {open && filtered.length > 0 && dropdownStyle && createPortal(
        <div
          ref={dropdownRef}
          className="group-suggestions"
          style={{
            position: "fixed",
            top: dropdownStyle.top,
            left: dropdownStyle.left,
            width: dropdownStyle.width,
            maxHeight: dropdownStyle.maxHeight,
          }}
        >
          {filtered.map((g) => (
            <button
              key={g}
              type="button"
              className="group-suggestion-item"
              onMouseDown={(e) => {
                e.preventDefault();
                onChange(g);
                setOpen(false);
              }}
            >
              <Folder size={12} />
              <span className="group-suggestion-text">{g}</span>
            </button>
          ))}
        </div>,
        document.body
      )}
    </div>
  );
}

interface SshTunnelSectionProps {
  form: ConnectionConfig;
  updateField: <K extends keyof ConnectionConfig>(field: K, value: ConnectionConfig[K]) => void;
  touchField: (field: ValidationField) => void;
  getFieldError: (field: ValidationField) => string | undefined;
}

function SshTunnelSection({
  form,
  updateField,
  touchField,
  getFieldError,
}: SshTunnelSectionProps) {
  const enabled = !!form.ssh_enabled;
  const authMethod: SshAuthMethod = form.ssh_auth_method ?? "password";
  const promptPassphrase = !!form.ssh_prompt_passphrase;
  const isIdentity = authMethod === "identityfile";
  const isAgent = authMethod === "agent";
  const sshHost = (form.ssh_host ?? "").trim();
  const shouldSuggestSshConfig = looksLikeSshConfigAlias(sshHost);
  // The password / passphrase field is only relevant for password auth and
  // for identity-file auth where the user opted to persist the passphrase.
  // ssh-agent never reads either, so the field is hidden entirely.
  const showPasswordField = !isAgent && !(isIdentity && promptPassphrase);

  const browseForKey = async () => {
    try {
      const path = await openFileDialog({
        multiple: false,
        directory: false,
        title: "Select SSH identity file",
      });
      if (typeof path === "string" && path) updateField("ssh_key_path", path);
    } catch {
      // User cancelled — no-op.
    }
  };


  // ssh-config import: read ~/.ssh/config and fill the SSH section from
  // the matching Host block. Empty fields are filled; non-empty fields
  // are left alone so the user's typed values aren't clobbered. We also
  // surface a status message so the user can tell whether anything was
  // applied.
  const [sshConfigStatus, setSshConfigStatus] = useState<{
    kind: "info" | "success" | "warning" | "error";
    message: string;
  } | null>(null);
  const applySshConfig = async () => {
    const alias = (form.ssh_host ?? "").trim();
    if (!alias) {
      setSshConfigStatus({
        kind: "info",
        message: "Type a Host alias from ~/.ssh/config first, then import it.",
      });
      return;
    }
    try {
      const resolved = await api.sshConfigLookup(alias);
      if (!resolved) {
        setSshConfigStatus({
          kind: "info",
          message: looksLikeSshConfigAlias(alias)
            ? `No matching Host block for "${alias}" in ~/.ssh/config.`
            : `"${alias}" looks like a direct host, so no SSH config entry was applied.`,
        });
        return;
      }
      const filled: string[] = [];
      if (resolved.hostName && !form.ssh_host?.trim().includes(".")) {
        // Replace the alias with the resolved hostname so subsequent
        // connect attempts hit the real bastion. Tracked so the status
        // message can mention what changed.
        updateField("ssh_host", resolved.hostName);
        filled.push("host");
      }
      if (
        resolved.port &&
        (form.ssh_port == null || form.ssh_port === 22 || form.ssh_port === 0)
      ) {
        updateField("ssh_port", resolved.port);
        filled.push("port");
      }
      if (resolved.user && !(form.ssh_user ?? "").trim()) {
        updateField("ssh_user", resolved.user);
        filled.push("user");
      }
      if (resolved.identityFile && !(form.ssh_key_path ?? "").trim()) {
        updateField("ssh_key_path", resolved.identityFile);
        if (form.ssh_auth_method !== "identityfile") {
          updateField("ssh_auth_method", "identityfile");
        }
        filled.push("identity file");
      }
      if (filled.length === 0) {
        setSshConfigStatus({
          kind: "info",
          message: `Matched ${resolved.alias}, but every relevant field already has a value.`,
        });
      } else if (resolved.hasUnsupportedDirectives) {
        setSshConfigStatus({
          kind: "warning",
          message: `Applied ${filled.join(", ")}. Note: this Host block uses ProxyJump / Match / Include — Tablio does not follow those, so the auto-fill may be incomplete.`,
        });
      } else {
        setSshConfigStatus({
          kind: "success",
          message: `Applied ${filled.join(", ")} from ${resolved.alias}.`,
        });
      }
    } catch (err) {
      setSshConfigStatus({
        kind: "error",
        message: `Could not read ~/.ssh/config: ${err instanceof Error ? err.message : String(err)}`,
      });
    }
  };

  // --- Cross-section advisories ---
  // We surface these in the SSH section because they only matter when
  // SSH is *enabled*, but they're triggered by other fields (SSL on
  // General-style settings; the chosen DB driver). Displayed inline so
  // the user sees them at the moment they enable the tunnel rather
  // than discovering them at connect time.
  const tlsVerifyConflict =
    enabled && !!form.ssl && !form.trust_server_cert && form.db_type !== "cassandra";
  const cassandraPeerDiscovery = enabled && form.db_type === "cassandra";
  // Warns only when the field actually contains something — empty
  // passphrases are unencrypted keys, not a security risk.
  const persistedPassphrase =
    enabled &&
    isIdentity &&
    !promptPassphrase &&
    !!form.ssh_password &&
    form.ssh_password.length > 0;

  if (!enabled) {
    // The enable toggle now lives in the section heading, so when the
    // tunnel is off the body is intentionally a brief explainer rather
    // than an empty card.
    return (
      <div className="connection-section-note">
        Tunnel database traffic through an SSH bastion. Turn on the toggle in
        the heading to configure the host, port and authentication.
      </div>
    );
  }

  return (
    <div className="ssh-tunnel-section ssh-tunnel-section--enabled" aria-label="SSH tunnel">
      <div className="ssh-tunnel-fields ssh-tunnel-fields--flush">
          {tlsVerifyConflict && (
            <div className="ssh-tunnel-warning" role="alert">
              <AlertCircle size={14} />
              <div>
                <strong>SSL hostname verification will fail.</strong>
                <span>
                  After tunneling, the driver connects to{" "}
                  <code>127.0.0.1</code> — the server certificate's hostname
                  won't match. Either turn on{" "}
                  <em>Trust server certificate</em> in the Security section, or
                  disable SSL.
                </span>
              </div>
            </div>
          )}
          {cassandraPeerDiscovery && (
            <div className="ssh-tunnel-warning" role="alert">
              <AlertCircle size={14} />
              <div>
                <strong>Cassandra cluster discovery bypasses the tunnel.</strong>
                <span>
                  The driver contacts the seed node through the tunnel, then
                  opens direct TCP connections to every peer it learns about.
                  This works for single-node clusters; multi-node clusters
                  need each peer reachable directly or its own forward.
                </span>
              </div>
            </div>
          )}
          <div className="form-row">
            <div
              className={`form-group flex-1${getFieldError("ssh_host") ? " form-group--error" : ""}`}
            >
              <label>Tunnel host</label>
              <div className="file-picker">
                <input
                  value={form.ssh_host ?? ""}
                  onChange={(e) => updateField("ssh_host", e.target.value)}
                  onBlur={() => touchField("ssh_host")}
                  placeholder="bastion.example.com (or an alias from ~/.ssh/config)"
                  aria-invalid={!!getFieldError("ssh_host")}
                />
                <button
                  type="button"
                  className="btn-secondary file-picker__browse"
                  onClick={applySshConfig}
                  title="Look up the current host in ~/.ssh/config and fill empty fields"
                >
                  Import config
                </button>
              </div>
              {getFieldError("ssh_host") && (
                <div className="field-error">{getFieldError("ssh_host")}</div>
              )}
              {sshConfigStatus && (
                <div
                  className={`form-field-description ssh-config-status ssh-config-status--${sshConfigStatus.kind}`}
                  role={sshConfigStatus.kind === "error" ? "alert" : undefined}
                  style={{ marginTop: 6 }}
                >
                  {sshConfigStatus.message}
                </div>
              )}
              {!sshConfigStatus && shouldSuggestSshConfig && (
                <div className="form-field-description ssh-config-hint">
                  Looks like an SSH config alias. Import config can fill host, user, port and identity file.
                </div>
              )}
            </div>
            <div
              className={`form-group${getFieldError("ssh_port") ? " form-group--error" : ""}`}
              style={{ width: 112 }}
            >
              <label>Port</label>
              <input
                type="number"
                value={form.ssh_port ?? 22}
                onChange={(e) => updateField("ssh_port", parseInt(e.target.value, 10) || 0)}
                onBlur={() => touchField("ssh_port")}
                min={1}
                max={65535}
                aria-invalid={!!getFieldError("ssh_port")}
              />
              {getFieldError("ssh_port") && (
                <div className="field-error">{getFieldError("ssh_port")}</div>
              )}
            </div>
          </div>

          {/* Username and auth method share a row: usernames are short and
             the auth toggle is `fit-content`, so this puts both fields on
             a single line and avoids the previous “tall thin column”. */}
          <div className="form-row form-row--ssh-identity">
            <div
              className={`form-group${getFieldError("ssh_user") ? " form-group--error" : ""}`}
            >
              <label>SSH username</label>
              <input
                value={form.ssh_user ?? ""}
                onChange={(e) => updateField("ssh_user", e.target.value)}
                onBlur={() => touchField("ssh_user")}
                placeholder="ubuntu"
                aria-invalid={!!getFieldError("ssh_user")}
              />
              {getFieldError("ssh_user") && (
                <div className="field-error">{getFieldError("ssh_user")}</div>
              )}
            </div>

            <div className="form-group">
              <label>Authentication</label>
              <div className="ssh-auth-toggle" role="radiogroup" aria-label="SSH authentication">
                <button
                  type="button"
                  role="radio"
                  aria-checked={authMethod === "password"}
                  className={`ssh-auth-toggle__btn${authMethod === "password" ? " ssh-auth-toggle__btn--active" : ""}`}
                  onClick={() => updateField("ssh_auth_method", "password")}
                >
                  <Lock size={14} />
                  Password
                </button>
                <button
                  type="button"
                  role="radio"
                  aria-checked={isIdentity}
                  className={`ssh-auth-toggle__btn${isIdentity ? " ssh-auth-toggle__btn--active" : ""}`}
                  onClick={() => updateField("ssh_auth_method", "identityfile")}
                >
                  <Key size={14} />
                  Identity file
                </button>
                <button
                  type="button"
                  role="radio"
                  aria-checked={isAgent}
                  className={`ssh-auth-toggle__btn${isAgent ? " ssh-auth-toggle__btn--active" : ""}`}
                  onClick={() => updateField("ssh_auth_method", "agent")}
                  title="Use the local ssh-agent (SSH_AUTH_SOCK on Linux/macOS, Pageant on Windows)"
                >
                  <Key size={14} />
                  SSH agent
                </button>
              </div>
            </div>
          </div>

          {isAgent && (
            <div className="form-field-description">
              Tablio will offer every identity loaded in your local ssh-agent
              until one is accepted. No passphrase is read or stored.
            </div>
          )}

          {isIdentity && (
            <div className="form-row">
              <div
                className={`form-group flex-1${getFieldError("ssh_key_path") ? " form-group--error" : ""}`}
              >
                <label>Identity file</label>
                <div className="file-picker">
                  <input
                    value={form.ssh_key_path ?? ""}
                    onChange={(e) => updateField("ssh_key_path", e.target.value)}
                    onBlur={() => touchField("ssh_key_path")}
                    placeholder="~/.ssh/id_ed25519"
                    aria-invalid={!!getFieldError("ssh_key_path")}
                  />
                  <button
                    type="button"
                    className="btn-secondary file-picker__browse"
                    onClick={browseForKey}
                  >
                    Browse…
                  </button>
                </div>
                {getFieldError("ssh_key_path") && (
                  <div className="field-error">{getFieldError("ssh_key_path")}</div>
                )}
              </div>
            </div>
          )}

          {(showPasswordField || isIdentity) && (
            <div
              className={`form-group${getFieldError("ssh_password") ? " form-group--error" : ""}`}
            >
              {/* For identity-file auth the passphrase label shares its
                 row with the "Ask when connecting" toggle so we don't
                 burn an extra row of vertical space on a single check. */}
              <div className="ssh-passphrase-header">
                <label htmlFor="ssh-password">
                  {isIdentity ? "Key passphrase" : "Password"}
                </label>
                {isIdentity && (
                  <label
                    className={`security-toggle security-toggle--inline${
                      promptPassphrase ? " security-toggle--active" : ""
                    }`}
                  >
                    <span className="security-toggle__control">
                      <input
                        className="security-toggle__input"
                        type="checkbox"
                        checked={promptPassphrase}
                        onChange={(e) => updateField("ssh_prompt_passphrase", e.target.checked)}
                      />
                      <span className="security-toggle__slider" aria-hidden="true" />
                    </span>
                    <span className="security-toggle__text">
                      <span className="security-toggle__label">Ask when connecting</span>
                    </span>
                  </label>
                )}
              </div>
              {showPasswordField && (
                <input
                  id="ssh-password"
                  type="password"
                  value={form.ssh_password ?? ""}
                  onChange={(e) => updateField("ssh_password", e.target.value)}
                  onBlur={() => touchField("ssh_password")}
                  placeholder={isIdentity ? "(leave blank if key is unencrypted)" : ""}
                  aria-invalid={!!getFieldError("ssh_password")}
                />
              )}
              {showPasswordField && getFieldError("ssh_password") && (
                <div className="field-error">{getFieldError("ssh_password")}</div>
              )}
            </div>
          )}

          {persistedPassphrase && (
            <div
              className="ssh-tunnel-warning ssh-tunnel-warning--info"
              role="status"
            >
              <AlertCircle size={14} />
              <div>
                <strong>Passphrase will be saved to disk.</strong>
                <span>
                  Tablio stores connection details in plain text. Enable
                  <em> Ask when connecting</em> to avoid saving it.
                </span>
              </div>
            </div>
          )}
      </div>
    </div>
  );
}

interface Props {
  onClose: () => void;
  editConfig?: ConnectionConfig;
  duplicate?: boolean;
}

export function ConnectionDialog({ onClose, editConfig, duplicate }: Props) {
  const addConnection = useConnectionStore((s) => s.addConnection);
  const updateConnection = useConnectionStore((s) => s.updateConnection);
  const connections = useConnectionStore((s) => s.connections);
  const isEdit = !!editConfig && !duplicate;

  const [form, setForm] = useState<ConnectionConfig>(() => {
    if (editConfig && duplicate) {
      return {
        ...editConfig,
        id: crypto.randomUUID(),
        name: `${editConfig.name} (copy)`,
        trust_server_cert: editConfig.trust_server_cert ?? false,
        group: null,
      };
    }
    if (editConfig) {
      return {
        ...editConfig,
        trust_server_cert: editConfig.trust_server_cert ?? false,
      };
    }
    return {
      id: crypto.randomUUID(),
      name: "",
      db_type: "postgres",
      host: "localhost",
      port: 5432,
      user: DB_TYPES.find((d) => d.value === "postgres")!.defaultUser,
      password: "",
      database: "",
      color: COLORS[0],
      ssl: false,
      trust_server_cert: false,
      ssh_enabled: false,
      ssh_host: "",
      ssh_port: 22,
      ssh_user: "",
      ssh_password: "",
      ssh_key_path: "",
      ssh_auth_method: "password",
      ssh_prompt_passphrase: false,
    };
  });

  const [testResult, setTestResult] = useState<"success" | "error" | null>(null);
  const [testError, setTestError] = useState("");
  const [saving, setSaving] = useState(false);
  const [showValidation, setShowValidation] = useState(false);
  const [touched, setTouched] = useState<Partial<Record<ValidationField, boolean>>>({});
  const [activeSection, setActiveSection] = useState<DialogSectionId>("general");
  const testingRef = useRef(false);

  const isSqlite = form.db_type === "sqlite";
  const supportsTlsOptions = !isSqlite && form.db_type !== "cassandra";
  const dialogSections = useMemo(
    () => getConnectionDialogSections(isSqlite, supportsTlsOptions),
    [isSqlite, supportsTlsOptions],
  );
  const validationErrors = useMemo(
    () => validateConnectionForm(form, connections),
    [form, connections],
  );
  const activeSectionMeta =
    dialogSections.find((section) => section.id === activeSection) ?? dialogSections[0];

  useEffect(() => {
    if (!dialogSections.some((section) => section.id === activeSection)) {
      setActiveSection(dialogSections[0].id);
    }
  }, [activeSection, dialogSections]);

  const touchField = (field: ValidationField) => {
    setTouched((prev) => (prev[field] ? prev : { ...prev, [field]: true }));
  };

  const getFieldError = (field: ValidationField) =>
    showValidation || touched[field] ? validationErrors[field] : undefined;

  const updateField = <K extends keyof ConnectionConfig>(field: K, value: ConnectionConfig[K]) => {
    setForm((prev) => ({ ...prev, [field]: value }));
    setTestResult(null);
    setTestError("");
  };

  const browseForSqliteFile = async () => {
    try {
      // No filters here on purpose: the `["*"]` "All files" pattern
      // is interpreted differently across platforms (GTK matches only
      // files with a dot, macOS NSOpenPanel handles it oddly), and
      // SQLite databases don't all use the same extension. Users with
      // an unusual filename can still type the path in the input.
      const path = await openFileDialog({
        multiple: false,
        directory: false,
        title: "Select SQLite database file",
      });
      if (typeof path === "string" && path) updateField("database", path);
    } catch {
      // User cancelled — no-op.
    }
  };

  const validateBeforeSubmit = () => {
    const normalized = normalizeConnectionForm(form);
    setForm(normalized);
    setShowValidation(true);
    const nextErrors = validateConnectionForm(normalized, connections);
    if (Object.keys(nextErrors).length > 0) {
      const firstInvalidSection = getFirstInvalidSection(nextErrors, dialogSections);
      if (firstInvalidSection) setActiveSection(firstInvalidSection);
      setTestResult(null);
      setTestError("Please fix the highlighted fields.");
      return null;
    }
    setTestError("");
    return normalized;
  };

  const handleDbTypeChange = (dbType: ConnectionConfig["db_type"]) => {
    const info = DB_TYPES.find((d) => d.value === dbType)!;
    setForm((f) => {
      // Only swap the username when it's still a default value (or
      // empty); a value the user typed by hand survives a DB-type
      // change so they don't lose their work flipping back and forth.
      const trimmedUser = f.user.trim();
      const isStillDefault =
        trimmedUser === "" || DEFAULT_USERNAMES.has(trimmedUser);
      return {
        ...f,
        db_type: dbType,
        port: info.defaultPort,
        host: dbType === "sqlite" ? "" : f.host || "localhost",
        user: isStillDefault ? info.defaultUser : f.user,
      };
    });
    setTestResult(null);
    setTestError("");
  };

  const [testing, setTesting] = useState(false);

  const handleTest = async () => {
    if (testingRef.current) return;
    const normalized = validateBeforeSubmit();
    if (!normalized) return;
    testingRef.current = true;
    setTestResult(null);
    setTestError("");
    setTesting(true);
    try {
      // Walk the same SSH UX as a live connect: prompt for the
      // identity-file passphrase if the user opted not to persist it,
      // and offer a Forget & Retry modal when the bastion's host key
      // has changed. Without this the user would just see a raw
      // `ssh_host_key_mismatch:{...}` error string.
      const effective = await resolveSshPassphrase(normalized);
      await withHostKeyMismatchRetry(normalized.name || "this connection", () =>
        api.testConnection(effective),
      );
      setTestResult("success");
    } catch (e) {
      setTestResult("error");
      setTestError(formatConnectionTestError(e, normalized));
    } finally {
      testingRef.current = false;
      setTesting(false);
    }
  };

  const handleSave = async () => {
    const normalized = validateBeforeSubmit();
    if (!normalized) return;
    setSaving(true);
    try {
      if (isEdit) {
        await updateConnection(normalized);
      } else {
        await addConnection(normalized);
      }
      onClose();
    } catch (e) {
      setTestError(String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="dialog-overlay">
      <div className="dialog connection-dialog">
        <div className="dialog-header">
          <h2>{isEdit ? "Edit Connection" : "New Connection"}</h2>
          <button className="btn-icon" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="dialog-body connection-dialog-body">
          <ConnectionDialogNav
            sections={dialogSections}
            activeSection={activeSection}
            errors={validationErrors}
            showErrors={showValidation}
            onSelect={setActiveSection}
          />

          <div className="connection-dialog-content">
            <SectionCard
              title={activeSectionMeta.label}
              description={activeSectionMeta.description}
              action={
                activeSection === "ssh" && !isSqlite ? (
                  <label
                    className={`security-toggle${
                      form.ssh_enabled ? " security-toggle--active" : ""
                    }`}
                  >
                    <span className="security-toggle__control">
                      <input
                        className="security-toggle__input"
                        type="checkbox"
                        checked={!!form.ssh_enabled}
                        onChange={(e) => updateField("ssh_enabled", e.target.checked)}
                      />
                      <span className="security-toggle__slider" aria-hidden="true" />
                    </span>
                    <span className="security-toggle__text">
                      <span className="security-toggle__label">
                        {form.ssh_enabled ? "Enabled" : "Disabled"}
                      </span>
                    </span>
                  </label>
                ) : undefined
              }
            >
              {activeSection === "general" && (
                <>
                  <div className="form-row">
                    <DbTypeDropdown
                      value={form.db_type}
                      onChange={handleDbTypeChange}
                      className="form-group--db-type"
                    />
                    <FormField
                      label="Connection Name"
                      error={getFieldError("name")}
                      className="flex-1"
                    >
                      <input
                        value={form.name}
                        onChange={(e) => updateField("name", e.target.value)}
                        onBlur={() => touchField("name")}
                        placeholder="My Database"
                        aria-invalid={!!getFieldError("name")}
                      />
                    </FormField>
                  </div>

                  <div className="form-row">
                    <div className="flex-1">
                      <GroupInput
                        value={form.group || ""}
                        onChange={(v) => updateField("group", v || null)}
                        connections={connections}
                      />
                    </div>
                    <FormField label="Color" className="form-group--color">
                      <ColorPicker
                        value={form.color}
                        onChange={(color) => setForm((f) => ({ ...f, color }))}
                      />
                    </FormField>
                  </div>

                  {isSqlite ? (
                    <FormField
                      label="Database File Path"
                      error={getFieldError("database")}
                      description="SQLite connections use a local database file instead of a network host. Tilde paths like ~/database.db are supported."
                    >
                      <div className="file-picker">
                        <input
                          value={form.database}
                          onChange={(e) => updateField("database", e.target.value)}
                          onBlur={() => touchField("database")}
                          placeholder="~/database.db"
                          aria-invalid={!!getFieldError("database")}
                        />
                        <button
                          type="button"
                          className="btn-secondary file-picker__browse"
                          onClick={browseForSqliteFile}
                        >
                          Browse…
                        </button>
                      </div>
                    </FormField>
                  ) : (
                    <>
                      <div className="form-row">
                        <FormField
                          label="Host"
                          error={getFieldError("host")}
                          className="flex-1"
                        >
                          <input
                            value={form.host}
                            onChange={(e) => updateField("host", e.target.value)}
                            onBlur={() => touchField("host")}
                            placeholder="localhost"
                            aria-invalid={!!getFieldError("host")}
                          />
                        </FormField>
                        <FormField
                          label="Port"
                          error={getFieldError("port")}
                          style={{ width: 112 }}
                        >
                          <input
                            type="number"
                            value={form.port}
                            onChange={(e) => updateField("port", parseInt(e.target.value, 10) || 0)}
                            onBlur={() => touchField("port")}
                            min={1}
                            max={65535}
                            aria-invalid={!!getFieldError("port")}
                          />
                        </FormField>
                      </div>

                      <FormField
                        label={form.db_type === "cassandra" ? "Keyspace (optional)" : "Database (optional)"}
                        error={getFieldError("database")}
                      >
                        <input
                          value={form.database}
                          onChange={(e) => updateField("database", e.target.value)}
                          onBlur={() => touchField("database")}
                          placeholder={form.db_type === "cassandra" ? "my_keyspace" : "mydb"}
                          aria-invalid={!!getFieldError("database")}
                        />
                      </FormField>
                    </>
                  )}
                </>
              )}

              {activeSection === "authentication" && !isSqlite && (
                <>
                  <div className="auth-method-card auth-method-card--active">
                    <Lock size={16} />
                    <div>
                      <strong>Password</strong>
                      <span>Built-in username/password authentication. More methods can be added here.</span>
                    </div>
                  </div>

                  <div className="form-row">
                    <FormField
                      label="Username"
                      error={getFieldError("user")}
                      className="flex-1"
                    >
                      <input
                        value={form.user}
                        onChange={(e) => updateField("user", e.target.value)}
                        onBlur={() => touchField("user")}
                        placeholder="postgres"
                        aria-invalid={!!getFieldError("user")}
                      />
                    </FormField>
                    <FormField label="Password" className="flex-1">
                      <input
                        type="password"
                        value={form.password}
                        onChange={(e) => updateField("password", e.target.value)}
                      />
                    </FormField>
                  </div>
                </>
              )}

              {activeSection === "security" && !isSqlite && (
                supportsTlsOptions ? (
                  <div className="security-options" aria-label="Transport security">
                    <label className={`security-toggle${form.ssl ? " security-toggle--active" : ""}`}>
                      <span className="security-toggle__control">
                        <input
                          className="security-toggle__input"
                          type="checkbox"
                          checked={form.ssl}
                          onChange={(e) => {
                            updateField("ssl", e.target.checked);
                            if (!e.target.checked) updateField("trust_server_cert", false);
                          }}
                        />
                        <span className="security-toggle__slider" aria-hidden="true" />
                      </span>
                      <span className="security-toggle__text">
                        <span className="security-toggle__label">SSL / TLS</span>
                      </span>
                    </label>

                    <label
                      className={`security-toggle security-toggle--nested${
                        form.trust_server_cert ? " security-toggle--active" : ""
                      }${
                        !form.ssl ? " security-toggle--disabled" : ""
                      }`}
                    >
                      <span className="security-toggle__control">
                        <input
                          className="security-toggle__input"
                          type="checkbox"
                          checked={form.trust_server_cert ?? false}
                          disabled={!form.ssl}
                          onChange={(e) => updateField("trust_server_cert", e.target.checked)}
                        />
                        <span className="security-toggle__slider" aria-hidden="true" />
                      </span>
                      <span className="security-toggle__text">
                        <span className="security-toggle__label">Trust server certificate</span>
                      </span>
                    </label>
                  </div>
                ) : (
                  <div className="connection-section-note">
                    TLS options are not exposed for Cassandra / ScyllaDB yet. This section is reserved
                    for future driver-specific transport settings.
                  </div>
                )
              )}

              {activeSection === "ssh" && !isSqlite && (
                <SshTunnelSection
                  form={form}
                  updateField={updateField}
                  touchField={touchField}
                  getFieldError={getFieldError}
                />
              )}

              {activeSection === "advanced" && (
                <div className="connection-section-note">
                  Connection parameters and driver-specific options will live here. Keeping this section
                  separate makes it easier to add future settings without crowding the basic connection flow.
                </div>
              )}
            </SectionCard>

            {testError && (
              <div className="connection-form-error" role="alert" aria-live="polite">
                <AlertCircle size={14} />
                <span>{testError}</span>
              </div>
            )}
          </div>
        </div>

        <div className="dialog-footer">
          <div className="dialog-footer-left">
            <button
              className={`btn-test-conn ${testing ? "btn-test-conn--testing" : ""}`}
              onClick={handleTest}
              disabled={testing}
            >
              {testing ? (
                <><Loader2 size={14} className="spin" /> Testing…</>
              ) : testResult ? (
                "Test again"
              ) : (
                "Test Connection"
              )}
            </button>
            {testResult === "success" && (
              <span className="connection-test-status connection-test-status--success" role="status">
                <CheckCircle size={14} /> Connected
              </span>
            )}
            {testResult === "error" && (
              <span className="connection-test-status connection-test-status--error" role="status">
                <XCircle size={14} /> Failed
              </span>
            )}
          </div>
          <div className="dialog-footer-right">
            <button className="btn-secondary" onClick={onClose}>
              Cancel
            </button>
            <button className="btn-primary" onClick={handleSave} disabled={saving}>
              {saving ? <Loader2 size={14} className="spin" /> : null}
              {testResult === "error" ? "Save Anyway" : isEdit ? "Save Changes" : "Save"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
