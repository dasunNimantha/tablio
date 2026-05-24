import { useMemo, useState, type ReactNode } from "react";
import { X, RotateCcw, Type, FileCode } from "lucide-react";
import { CustomSelect } from "../CustomSelect/CustomSelect";
import {
  useUserSettingsStore,
  UI_FONT_SIZE_MIN,
  UI_FONT_SIZE_MAX,
  EDITOR_FONT_SIZE_MIN,
  EDITOR_FONT_SIZE_MAX,
} from "../../stores/userSettingsStore";
import {
  UI_FONT_CANDIDATES,
  EDITOR_FONT_CANDIDATES,
  SYSTEM_DEFAULT_SENTINEL,
  detectAvailableFonts,
  type FontOption,
} from "../../lib/fontAvailability";
import "./PreferencesDialog.css";

/**
 * Preferences dialog — user-configurable font family + size for the
 * UI shell and the Monaco editor surface (issue #62).
 *
 * Layout mirrors the Connection Dialog: a left-rail nav (one entry
 * per logical group) + a right pane of `connection-section-card`
 * blocks with heading + form fields. The store updates live, so
 * there's no Save button — Done just closes the dialog.
 */

interface Props {
  onClose: () => void;
}

type SectionId = "interface" | "editor";

interface SectionDef {
  id: SectionId;
  label: string;
  description: string;
  icon: ReactNode;
}

const SECTIONS: SectionDef[] = [
  {
    id: "interface",
    label: "Interface",
    description: "Font for the sidebar, toolbars, and dialogs.",
    icon: <Type size={14} />,
  },
  {
    id: "editor",
    label: "SQL editor",
    description: "Font for the query editor and result grid.",
    icon: <FileCode size={14} />,
  },
];

/* ---------------------- shared field primitives ---------------------- */

function FontFamilyField({
  candidates,
  value,
  onChange,
}: {
  candidates: FontOption[];
  value: string | null;
  onChange: (family: string | null) => void;
}) {
  return (
    <div className="form-group">
      <label>Font family</label>
      <CustomSelect
        className="preferences-font-select"
        // CustomSelect identifies the active option by `value`, so
        // map `null` → the sentinel literal it can match.
        value={value ?? SYSTEM_DEFAULT_SENTINEL}
        options={candidates.map((c) => ({ value: c.family, label: c.label }))}
        onChange={(v) =>
          onChange(v === SYSTEM_DEFAULT_SENTINEL ? null : v)
        }
        searchable
        // The dialog's content scroll container clips inline
        // popovers — render the dropdown to document.body so the
        // full option list (and its search box) stays visible.
        portal
      />
    </div>
  );
}

function FontSizeField({
  value,
  min,
  max,
  onChange,
}: {
  value: number;
  min: number;
  max: number;
  onChange: (size: number) => void;
}) {
  return (
    <div className="form-group preferences-size-group">
      <label>Font size</label>
      <input
        className="preferences-size-input"
        type="number"
        min={min}
        max={max}
        value={value}
        onChange={(e) => {
          const next = parseInt(e.target.value, 10);
          // Empty input parses to NaN — let the store's clamp handle
          // it (falls back to the default), so we can still type
          // freely without the value snapping every keystroke.
          if (!Number.isNaN(next)) onChange(next);
        }}
      />
    </div>
  );
}

function SectionCard({
  title,
  description,
  previewClass,
  previewText,
  testId,
  children,
}: {
  title: string;
  description: string;
  previewClass: string;
  previewText: string;
  testId: string;
  children: ReactNode;
}) {
  return (
    <section className="connection-section-card" aria-label={title}>
      <div className="connection-section-heading">
        <div className="connection-section-heading__text">
          <h3>{title}</h3>
          <p>{description}</p>
        </div>
      </div>
      <div className="connection-section-fields">
        <div className="form-row">{children}</div>
        <div
          className={`preferences-preview ${previewClass}`}
          data-testid={testId}
        >
          {previewText}
        </div>
      </div>
    </section>
  );
}

/* ----------------------------- main ---------------------------------- */

export function PreferencesDialog({ onClose }: Props) {
  const settings = useUserSettingsStore((s) => s.settings);
  const setUiFontFamily = useUserSettingsStore((s) => s.setUiFontFamily);
  const setUiFontSize = useUserSettingsStore((s) => s.setUiFontSize);
  const setEditorFontFamily = useUserSettingsStore(
    (s) => s.setEditorFontFamily,
  );
  const setEditorFontSize = useUserSettingsStore((s) => s.setEditorFontSize);
  const resetToDefaults = useUserSettingsStore((s) => s.resetToDefaults);

  const [activeSection, setActiveSection] = useState<SectionId>("interface");

  // Probe what's installed once per mount. Bundled fonts + the
  // "System default" sentinel always pass through.
  const uiFonts = useMemo(
    () => detectAvailableFonts(UI_FONT_CANDIDATES),
    [],
  );
  const editorFonts = useMemo(
    () => detectAvailableFonts(EDITOR_FONT_CANDIDATES),
    [],
  );

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog connection-dialog preferences-dialog"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-header">
          <h2>Preferences</h2>
          <button
            className="btn-icon"
            onClick={onClose}
            aria-label="Close preferences"
          >
            <X size={18} />
          </button>
        </div>

        <div className="dialog-body connection-dialog-body">
          <nav
            className="connection-dialog-nav"
            aria-label="Preferences sections"
          >
            {SECTIONS.map((section) => (
              <button
                key={section.id}
                type="button"
                className={`connection-nav-item${
                  activeSection === section.id
                    ? " connection-nav-item--active"
                    : ""
                }`}
                onClick={() => setActiveSection(section.id)}
                data-section={section.id}
                data-testid={`preferences-nav-${section.id}`}
              >
                <span className="connection-nav-item__icon">
                  {section.icon}
                </span>
                <span className="connection-nav-item__text">
                  <span className="connection-nav-item__label">
                    {section.label}
                  </span>
                  <span className="connection-nav-item__description">
                    {section.description}
                  </span>
                </span>
              </button>
            ))}
          </nav>

          <div className="connection-dialog-content">
            {activeSection === "interface" && (
              <SectionCard
                title="Interface font"
                description="Applies to the sidebar, toolbars, tabs, and dialog text."
                previewClass="preferences-preview-ui"
                previewText="The quick brown fox jumps over the lazy dog"
                testId="preferences-ui-preview"
              >
                <FontFamilyField
                  candidates={uiFonts}
                  value={settings.uiFontFamily}
                  onChange={setUiFontFamily}
                />
                <FontSizeField
                  value={settings.uiFontSize}
                  min={UI_FONT_SIZE_MIN}
                  max={UI_FONT_SIZE_MAX}
                  onChange={setUiFontSize}
                />
              </SectionCard>
            )}
            {activeSection === "editor" && (
              <SectionCard
                title="SQL editor font"
                description="Applies to the Query Console, DDL viewer, and result grid cells."
                previewClass="preferences-preview-editor"
                previewText="SELECT id, name FROM users WHERE active = true;"
                testId="preferences-editor-preview"
              >
                <FontFamilyField
                  candidates={editorFonts}
                  value={settings.editorFontFamily}
                  onChange={setEditorFontFamily}
                />
                <FontSizeField
                  value={settings.editorFontSize}
                  min={EDITOR_FONT_SIZE_MIN}
                  max={EDITOR_FONT_SIZE_MAX}
                  onChange={setEditorFontSize}
                />
              </SectionCard>
            )}
          </div>
        </div>

        <div className="dialog-footer">
          <div className="dialog-footer-left">
            <button
              type="button"
              className="btn-ghost preferences-reset-btn"
              onClick={resetToDefaults}
              title="Reset font family and size for both UI and editor"
            >
              <RotateCcw size={14} />
              Reset to defaults
            </button>
          </div>
          <div className="dialog-footer-right">
            <button className="btn-primary" onClick={onClose}>
              Done
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
