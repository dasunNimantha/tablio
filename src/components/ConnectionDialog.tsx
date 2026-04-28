import { useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { useConnectionStore } from "../stores/connectionStore";
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
  Server,
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
  { value: "postgres" as const, label: "PostgreSQL", short: "PG", defaultPort: 5432, accent: "#89b4fa" },
  { value: "mysql" as const, label: "MySQL", short: "MY", defaultPort: 3306, accent: "#f9e2af" },
  { value: "sqlite" as const, label: "SQLite", short: "SQ", defaultPort: 0, accent: "#94e2d5" },
  { value: "mariadb" as const, label: "MariaDB", short: "MA", defaultPort: 3306, accent: "#fab387" },
  { value: "cockroachdb" as const, label: "CockroachDB", short: "CR", defaultPort: 26257, accent: "#cba6f7" },
  { value: "tidb" as const, label: "TiDB", short: "TI", defaultPort: 4000, accent: "#f38ba8" },
  { value: "cassandra" as const, label: "Cassandra / ScyllaDB", short: "CS", defaultPort: 9042, accent: "#a6e3a1" },
  { value: "mssql" as const, label: "Microsoft SQL Server", short: "MS", defaultPort: 1433, accent: "#7aa2f7" },
];

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
  | "connection"
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
  general: ["name"],
  connection: ["host", "port", "database"],
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
      description: "Name, type, folder and colour",
      icon: <Database size={16} />,
    },
    {
      id: "connection",
      label: "Connection",
      description: isSqlite ? "Local database file" : "Host, port and database",
      icon: <Server size={16} />,
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
}: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <section className="connection-section-card" aria-label={title}>
      <div className="connection-section-heading">
        <h3>{title}</h3>
        <p>{description}</p>
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
    // Don't persist a passphrase the user opted out of saving.
    ssh_password: sshEnabled && !promptPassphrase ? form.ssh_password ?? "" : "",
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
    } else if (!normalized.ssh_password) {
      // Only require a password when the user opted to store it.
      // (For identity-file auth, ssh_password is the *passphrase* and may
      // be intentionally empty for unencrypted keys.)
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

  return (
    <div className="security-options ssh-tunnel-section" aria-label="SSH tunnel">
      <label className={`security-toggle${enabled ? " security-toggle--active" : ""}`}>
        <span className="security-toggle__control">
          <input
            className="security-toggle__input"
            type="checkbox"
            checked={enabled}
            onChange={(e) => updateField("ssh_enabled", e.target.checked)}
          />
          <span className="security-toggle__slider" aria-hidden="true" />
        </span>
        <span className="security-toggle__text">
          <span className="security-toggle__label">Use SSH tunneling</span>
          <span className="security-toggle__meta">connect through an SSH bastion</span>
        </span>
      </label>

      {enabled && (
        <div className="ssh-tunnel-fields">
          <div className="form-row">
            <div
              className={`form-group flex-1${getFieldError("ssh_host") ? " form-group--error" : ""}`}
            >
              <label>Tunnel host</label>
              <input
                value={form.ssh_host ?? ""}
                onChange={(e) => updateField("ssh_host", e.target.value)}
                onBlur={() => touchField("ssh_host")}
                placeholder="bastion.example.com"
                aria-invalid={!!getFieldError("ssh_host")}
              />
              {getFieldError("ssh_host") && (
                <div className="field-error">{getFieldError("ssh_host")}</div>
              )}
            </div>
            <div
              className={`form-group${getFieldError("ssh_port") ? " form-group--error" : ""}`}
              style={{ width: 100 }}
            >
              <label>Tunnel port</label>
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

          <div className="form-row">
            <div
              className={`form-group flex-1${getFieldError("ssh_user") ? " form-group--error" : ""}`}
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
          </div>

          <div className="form-group">
            <label>Authentication</label>
            <div className="ssh-auth-toggle" role="radiogroup" aria-label="SSH authentication">
              <button
                type="button"
                role="radio"
                aria-checked={!isIdentity}
                className={`ssh-auth-toggle__btn${!isIdentity ? " ssh-auth-toggle__btn--active" : ""}`}
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
            </div>
          </div>

          {isIdentity && (
            <div className="form-row">
              <div
                className={`form-group flex-1${getFieldError("ssh_key_path") ? " form-group--error" : ""}`}
              >
                <label>Identity file</label>
                <div className="ssh-file-picker">
                  <input
                    value={form.ssh_key_path ?? ""}
                    onChange={(e) => updateField("ssh_key_path", e.target.value)}
                    onBlur={() => touchField("ssh_key_path")}
                    placeholder="~/.ssh/id_ed25519"
                    aria-invalid={!!getFieldError("ssh_key_path")}
                  />
                  <button
                    type="button"
                    className="btn-secondary ssh-file-picker__browse"
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

          {!(isIdentity && promptPassphrase) && (
            <div className="form-row">
              <div
                className={`form-group flex-1${getFieldError("ssh_password") ? " form-group--error" : ""}`}
              >
                <label>{isIdentity ? "Key passphrase" : "Password"}</label>
                <input
                  type="password"
                  value={form.ssh_password ?? ""}
                  onChange={(e) => updateField("ssh_password", e.target.value)}
                  onBlur={() => touchField("ssh_password")}
                  placeholder={isIdentity ? "(leave blank if key is unencrypted)" : ""}
                  aria-invalid={!!getFieldError("ssh_password")}
                />
                {getFieldError("ssh_password") && (
                  <div className="field-error">{getFieldError("ssh_password")}</div>
                )}
              </div>
            </div>
          )}

          {isIdentity && (
            <label
              className={`security-toggle security-toggle--nested${
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
                <span className="security-toggle__label">Prompt for passphrase?</span>
                <span className="security-toggle__meta">
                  ask each time instead of saving the passphrase to disk
                </span>
              </span>
            </label>
          )}
        </div>
      )}
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
      user: "",
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
    setForm((f) => ({
      ...f,
      db_type: dbType,
      port: info.defaultPort,
      host: dbType === "sqlite" ? "" : f.host || "localhost",
    }));
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
      await api.testConnection(normalized);
      setTestResult("success");
    } catch (e) {
      setTestResult("error");
      setTestError(String(e));
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
                </>
              )}

              {activeSection === "connection" && (
                isSqlite ? (
                  <FormField
                    label="Database File Path"
                    error={getFieldError("database")}
                    description="SQLite connections use a local database file instead of a network host."
                  >
                    <input
                      value={form.database}
                      onChange={(e) => updateField("database", e.target.value)}
                      onBlur={() => touchField("database")}
                      placeholder="/path/to/database.db"
                      aria-invalid={!!getFieldError("database")}
                    />
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
                )
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
                        <span className="security-toggle__meta">(self-signed)</span>
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

            {testError && <div className="connection-form-error">{testError}</div>}
          </div>
        </div>

        <div className="dialog-footer">
          <button
            className={`btn-test-conn ${testing ? "btn-test-conn--testing" : ""} ${!testing && testResult === "success" ? "btn-test-conn--success" : ""} ${!testing && testResult === "error" ? "btn-test-conn--error" : ""}`}
            onClick={handleTest}
          >
            {testing ? (
              <><Loader2 size={14} className="spin" /> Testing…</>
            ) : testResult === "success" ? (
              <><CheckCircle size={14} /> Connected</>
            ) : testResult === "error" ? (
              <><XCircle size={14} /> Failed</>
            ) : (
              "Test Connection"
            )}
          </button>
          <div className="dialog-footer-right">
            <button className="btn-secondary" onClick={onClose}>
              Cancel
            </button>
            <button className="btn-primary" onClick={handleSave} disabled={saving}>
              {saving ? <Loader2 size={14} className="spin" /> : null}
              {isEdit ? "Save Changes" : "Save"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
