export interface DriveInfo {
  id: string;
  name: string;
  instance_url: string;
  sync_path: string;
  icon_path?: string;
  raw_icon_path?: string;
  enabled: boolean;
  user_id: string;
  remote_path: string;
  status: DriveStatus;
  max_file_size_mb?: number;
  approved_http_storage_origins?: string[];
  pending_http_storage_origins?: string[];
  capacity?: CapacitySummary;
}

export type DriveStatus = "active" | "event_push_lost" | "offline" | "credential_expired" | "transport_consent_required";

export interface CapacitySummary {
  total: number;
  used: number;
  label: string;
}
