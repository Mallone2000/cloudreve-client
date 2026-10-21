import { version } from "@tauri-apps/plugin-os";

/**
 * Hook to detect if the current OS is Windows 10.
 * Windows 10 version strings start with "10.0" but the build number is below 22000.
 * Windows 11 also reports as "10.0" but with build number >= 22000.
 */
export function useIsWindows10(): boolean | null {
  const parts = version().split(".");
  if (parts.length < 3) return false;
  const buildNumber = parseInt(parts[2], 10);
  return !isNaN(buildNumber) && buildNumber < 22000;
}
