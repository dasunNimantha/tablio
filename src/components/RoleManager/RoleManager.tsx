import { useEffect, useState, useCallback } from "react";
import {
  api,
  RoleInfo,
  CreateRoleRequest,
  AlterRoleRequest,
} from "../../lib/tauri";
import { ConfirmDialog } from "../ConfirmDialog";
import {
  Shield,
  Plus,
  Trash2,
  Edit,
  Loader2,
  Check,
  X,
  Users,
} from "lucide-react";
import "./RoleManager.css";

interface Props {
  connectionId: string;
}

interface RoleFormState {
  name: string;
  password: string;
  is_superuser: boolean;
  can_login: boolean;
  can_create_db: boolean;
  can_create_role: boolean;
  connection_limit: number;
}

const emptyForm = (): RoleFormState => ({
  name: "",
  password: "",
  is_superuser: false,
  can_login: false,
  can_create_db: false,
  can_create_role: false,
  connection_limit: -1,
});

function roleToForm(role: RoleInfo): RoleFormState {
  return {
    name: role.name,
    password: "",
    is_superuser: role.is_superuser,
    can_login: role.can_login,
    can_create_db: role.can_create_db,
    can_create_role: role.can_create_role,
    connection_limit: role.connection_limit,
  };
}

export function RoleManager({ connectionId }: Props) {
  const [roles, setRoles] = useState<RoleInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [editingRole, setEditingRole] = useState<RoleInfo | null>(null);
  const [dropTarget, setDropTarget] = useState<RoleInfo | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);
  const [form, setForm] = useState<RoleFormState>(emptyForm());

  const fetchRoles = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await api.listRoles(connectionId);
      setRoles(result);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [connectionId]);

  useEffect(() => {
    fetchRoles();
  }, [fetchRoles]);

  const handleCreate = async () => {
    if (!form.name.trim()) {
      setFormError("Name is required");
      return;
    }

    setSubmitting(true);
    setFormError(null);
    try {
      const request: CreateRoleRequest = {
        connection_id: connectionId,
        name: form.name.trim(),
        password: form.password.trim() || null,
        is_superuser: form.is_superuser,
        can_login: form.can_login,
        can_create_db: form.can_create_db,
        can_create_role: form.can_create_role,
        connection_limit: form.connection_limit < 0 ? -1 : form.connection_limit,
        valid_until: null,
      };
      await api.createRole(request);
      setShowCreateForm(false);
      setForm(emptyForm());
      fetchRoles();
    } catch (e) {
      setFormError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const handleEdit = async () => {
    if (!editingRole || !form.name.trim()) {
      setFormError("Name is required");
      return;
    }

    setSubmitting(true);
    setFormError(null);
    try {
      const request: AlterRoleRequest = {
        connection_id: connectionId,
        name: editingRole.name,
        password: form.password.trim() || undefined,
        is_superuser: form.is_superuser,
        can_login: form.can_login,
        can_create_db: form.can_create_db,
        can_create_role: form.can_create_role,
        connection_limit: form.connection_limit < 0 ? null : form.connection_limit,
        valid_until: null,
      };
      await api.alterRole(request);
      setEditingRole(null);
      setForm(emptyForm());
      fetchRoles();
    } catch (e) {
      setFormError(String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const handleDrop = async () => {
    if (!dropTarget) return;

    try {
      await api.dropRole({
        connection_id: connectionId,
        name: dropTarget.name,
      });
      setDropTarget(null);
      fetchRoles();
    } catch (e) {
      setError(String(e));
      setDropTarget(null);
    }
  };

  const openCreateForm = () => {
    setForm(emptyForm());
    setFormError(null);
    setShowCreateForm(true);
  };

  const openEditForm = (role: RoleInfo) => {
    setForm(roleToForm(role));
    setFormError(null);
    setEditingRole(role);
  };

  const closeForm = () => {
    setShowCreateForm(false);
    setEditingRole(null);
    setForm(emptyForm());
    setFormError(null);
  };

  const showForm = showCreateForm || editingRole !== null;

  return (
    <div className="role-manager">
      <div className="role-toolbar">
        <div className="role-toolbar-left">
          <Shield size={18} className="role-toolbar-icon" />
          <span className="role-toolbar-title">Users & Roles</span>
        </div>
        <button
          className="btn-primary btn-sm"
          onClick={openCreateForm}
          disabled={loading}
        >
          <Plus size={16} />
          Create Role
        </button>
      </div>

      {error && (
        <div className="role-error-banner">
          {error}
        </div>
      )}

      <div className="role-table-wrapper">
        {loading ? (
          <div className="role-loading">
            <Loader2 size={28} className="spin" />
            <span>Loading roles...</span>
          </div>
        ) : (
          <table className="role-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Login</th>
                <th>Superuser</th>
                <th>Create DB</th>
                <th>Create Role</th>
                <th>Replication</th>
                <th>Conn Limit</th>
                <th className="role-actions-header">Actions</th>
              </tr>
            </thead>
            <tbody>
              {roles.length === 0 ? (
                <tr>
                  <td colSpan={8} className="role-empty">
                    <Users size={32} strokeWidth={1.5} />
                    <span>No roles found</span>
                  </td>
                </tr>
              ) : (
                roles.map((role) => (
                  <tr key={role.name}>
                    <td className="role-name-cell">
                      <span className="role-name">{role.name}</span>
                    </td>
                    <td>
                      <span className={`role-badge ${role.can_login ? "role-badge-yes" : "role-badge-no"}`}>
                        {role.can_login ? <Check size={12} /> : <X size={12} />}
                      </span>
                    </td>
                    <td>
                      <span className={`role-badge ${role.is_superuser ? "role-badge-yes" : "role-badge-no"}`}>
                        {role.is_superuser ? <Check size={12} /> : <X size={12} />}
                      </span>
                    </td>
                    <td>
                      <span className={`role-badge ${role.can_create_db ? "role-badge-yes" : "role-badge-no"}`}>
                        {role.can_create_db ? <Check size={12} /> : <X size={12} />}
                      </span>
                    </td>
                    <td>
                      <span className={`role-badge ${role.can_create_role ? "role-badge-yes" : "role-badge-no"}`}>
                        {role.can_create_role ? <Check size={12} /> : <X size={12} />}
                      </span>
                    </td>
                    <td>
                      <span className={`role-badge ${role.is_replication ? "role-badge-yes" : "role-badge-no"}`}>
                        {role.is_replication ? <Check size={12} /> : <X size={12} />}
                      </span>
                    </td>
                    <td className="role-limit-cell">
                      {role.connection_limit < 0 ? "∞" : role.connection_limit}
                    </td>
                    <td className="role-actions-cell">
                      <button
                        className="btn-icon"
                        onClick={() => openEditForm(role)}
                        title="Edit"
                      >
                        <Edit size={14} />
                      </button>
                      <button
                        className="btn-icon"
                        onClick={() => setDropTarget(role)}
                        title="Drop"
                      >
                        <Trash2 size={14} />
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        )}
      </div>

      {showForm && (
        <div className="dialog-overlay" onClick={closeForm}>
          <div className="dialog role-form" onClick={(e) => e.stopPropagation()}>
            <div className="dialog-header">
              <h2>{editingRole ? "Edit Role" : "Create Role"}</h2>
              <button className="btn-icon" onClick={closeForm}>
                <X size={18} />
              </button>
            </div>
            <div className="dialog-body">
              {formError && (
                <div className="role-form-error">{formError}</div>
              )}
              <div className="form-group">
                <label>Name</label>
                <input
                  type="text"
                  value={form.name}
                  onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                  placeholder="role_name"
                  disabled={!!editingRole}
                />
              </div>
              <div className="form-group">
                <label>Password {editingRole && "(leave blank to keep current)"}</label>
                <input
                  type="password"
                  value={form.password}
                  onChange={(e) => setForm((f) => ({ ...f, password: e.target.value }))}
                  placeholder="••••••••"
                />
              </div>
              <div className="form-row form-checkboxes">
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={form.can_login}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, can_login: e.target.checked }))
                    }
                  />
                  Can Login
                </label>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={form.is_superuser}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, is_superuser: e.target.checked }))
                    }
                  />
                  Superuser
                </label>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={form.can_create_db}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, can_create_db: e.target.checked }))
                    }
                  />
                  Create DB
                </label>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={form.can_create_role}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, can_create_role: e.target.checked }))
                    }
                  />
                  Create Role
                </label>
              </div>
              <div className="form-group">
                <label>Connection Limit (-1 = no limit)</label>
                <input
                  type="number"
                  value={form.connection_limit < 0 ? "" : form.connection_limit}
                  onChange={(e) => {
                    const v = e.target.value;
                    setForm((f) => ({
                      ...f,
                      connection_limit: v === "" ? -1 : parseInt(v, 10) || 0,
                    }));
                  }}
                  placeholder="-1"
                  min={-1}
                />
              </div>
            </div>
            <div className="dialog-footer">
              <div />
              <div className="dialog-footer-right">
                <button className="btn-secondary" onClick={closeForm}>
                  Cancel
                </button>
                <button
                  className="btn-primary"
                  onClick={editingRole ? handleEdit : handleCreate}
                  disabled={submitting}
                >
                  {submitting ? (
                    <Loader2 size={16} className="spin" />
                  ) : editingRole ? (
                    "Save"
                  ) : (
                    "Create"
                  )}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {dropTarget && (
        <ConfirmDialog
          title="Drop Role"
          message={`Drop role "${dropTarget.name}"? This cannot be undone.`}
          confirmLabel="Drop"
          danger
          onConfirm={handleDrop}
          onCancel={() => setDropTarget(null)}
        />
      )}
    </div>
  );
}
