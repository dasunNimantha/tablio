import { createContext, useContext } from "react";

/**
 * Whether the tab a component lives in is the active (visible) tab.
 * Every tab stays mounted (hidden with display:none), so pollers use
 * this to pause in background tabs. Defaults to true outside a tab.
 */
export const TabActiveContext = createContext(true);

export function useIsTabActive(): boolean {
  return useContext(TabActiveContext);
}
