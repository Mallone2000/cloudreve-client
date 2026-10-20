import { InsertDriveFile as DefaultFileIcon } from "@mui/icons-material";

interface FileIconProps {
  path: string;
  size?: number;
}

// The backend has no get_file_icon command. Avoid a failed IPC request for
// every visible task and show the same fallback icon immediately.
export default function FileIcon({ path, size = 24 }: FileIconProps) {
  const name = path.split(/[/\\]/).pop() || path;
  return <DefaultFileIcon sx={{ fontSize: size }} color="action" aria-label={`File: ${name}`} />;
}
