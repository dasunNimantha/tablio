import type { ConnectionConfig } from "./tauri";

const DB_LABELS: Record<ConnectionConfig["db_type"], string> = {
  postgres: "PostgreSQL",
  mysql: "MySQL",
  sqlite: "SQLite",
  mariadb: "MariaDB",
  cockroachdb: "CockroachDB",
  tidb: "TiDB",
  cassandra: "Cassandra / ScyllaDB",
  mssql: "Microsoft SQL Server",
};

export function dbTypeLabel(dbType: ConnectionConfig["db_type"] | undefined): string {
  return (dbType && DB_LABELS[dbType]) || "database";
}

export function errorText(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

/** Turns a raw driver / tunnel error into a sentence that says what failed
 *  and where to fix it. Shared by Test Connection and the explorer's
 *  connect actions so both report failures the same way. */
export function formatConnectionError(
  error: unknown,
  form: Pick<ConnectionConfig, "db_type" | "user" | "ssh_host">,
): string {
  const raw = errorText(error).trim();
  const dbLabel = dbTypeLabel(form.db_type);
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

  return raw || "Connection failed.";
}

/** Error message for a failed connect from the explorer: names the
 *  connection so the toast makes sense out of context. */
export function formatConnectFailure(
  error: unknown,
  conn: Pick<ConnectionConfig, "name" | "database" | "db_type" | "user" | "ssh_host">,
): string {
  const name = conn.name || conn.database || "connection";
  return `Couldn't connect to "${name}". ${formatConnectionError(error, conn)}`;
}

/** True when the error is a user-cancelled prompt (passphrase / host key)
 *  rather than a real failure worth reporting. */
export function isCancelledPrompt(error: unknown): boolean {
  return /prompt cancelled/i.test(errorText(error));
}
