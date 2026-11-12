import { useEffect } from "react";
import { useTabStore } from "../stores/tabStore";

/**
 * Report whether a component inside a tab holds unsaved work, so the
 * tab bar can confirm before closing that tab. `source` distinguishes
 * multiple reporters in one tab (e.g. "grid", "query").
 */
export function useReportTabDirty(tabId: string | undefined, source: string, dirty: boolean): void {
  useEffect(() => {
    if (!tabId) return;
    useTabStore.getState().setTabDirty(tabId, source, dirty);
  }, [tabId, source, dirty]);

  useEffect(() => {
    if (!tabId) return;
    return () => useTabStore.getState().setTabDirty(tabId, source, false);
  }, [tabId, source]);
}
