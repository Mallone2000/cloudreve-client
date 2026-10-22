import { Alert, Box, Button, CircularProgress, Container, IconButton, InputAdornment, Snackbar, Typography } from "@mui/material";
import CloseIcon from "@mui/icons-material/Close";
import { openUrl } from "@tauri-apps/plugin-opener";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { invoke } from '@tauri-apps/api/core';
import confetti from "canvas-confetti";
import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";
import defaultLogo from "../assets/cloudreve.svg";
import { FilledTextField } from "../common/StyledComponent";
import { listen } from '@tauri-apps/api/event';

type PageState = "url_input" | "waiting" | "final_setup" | "setting_up" | "success";

interface OAuthCallbackNotice {
  session_id: string;
  name: string;
}

interface BeginOAuthResult {
  session_id: string;
  authorize_url: string;
}

interface AddDriveProps {
  mode?: "add" | "reauthorize";
}

export default function AddDrive({ mode = "add" }: AddDriveProps) {
  const { t } = useTranslation();
  const { driveId, siteUrl: encodedSiteUrl, driveName: driveNameQuery } = useParams<{ driveId?: string; siteUrl?: string, driveName: string }>();
  const isReauthorize = mode === "reauthorize" && driveId && encodedSiteUrl;
  const decodedSiteUrl = encodedSiteUrl ? decodeURIComponent(encodedSiteUrl) : "";

  const [siteUrl, setSiteUrl] = useState(isReauthorize ? decodedSiteUrl : "");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [snackbarOpen, setSnackbarOpen] = useState(false);
  const [authorizeUrl, setAuthorizeUrl] = useState<string | null>(null);
  const [pageState, setPageState] = useState<PageState>(isReauthorize ? "url_input" : "url_input");
  const [localPath, setLocalPath] = useState("");
  const [folderNotEmpty, setFolderNotEmpty] = useState(false);
  const [driveName, setDriveName] = useState(driveNameQuery ? decodeURIComponent(driveNameQuery) : "");
  const oauthSessionRef = useRef<string | null>(null);
  const hasInitialized = useRef(false);

  // Listen for deeplink events from OAuth callback
  useEffect(() => {
    let unlisten: () => void;
    listen<OAuthCallbackNotice>('oauth-callback', (event) => {
      if (event.payload.session_id !== oauthSessionRef.current) {
        return;
      }
      if (event.payload.name) setDriveName(event.payload.name);
      setPageState("final_setup");
    }).then((fn) => {
      unlisten = fn;
    });

    return () => {
      if (unlisten) {
        unlisten();
      }
    };
  }, []);

  // Shared authorization logic
  const startAuthorization = useCallback(async (urlToAuthorize: string) => {
    setLoading(true);
    setSnackbarOpen(false);

    try {
      const result = await invoke<BeginOAuthResult>("begin_oauth", {
        siteUrl: urlToAuthorize.trim(),
      });
      oauthSessionRef.current = result.session_id;
      setAuthorizeUrl(result.authorize_url);
      setPageState("waiting");
      await openUrl(result.authorize_url);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      setError(t("addDrive.errors.connectionFailed", { message }));
      setSnackbarOpen(true);
    } finally {
      setLoading(false);
    }
  }, [t]);

  // Auto-start authorization flow when in reauthorize mode
  useEffect(() => {
    if (isReauthorize && !decodedSiteUrl.startsWith("http://") && !hasInitialized.current) {
      hasInitialized.current = true;
      startAuthorization(decodedSiteUrl);
    }
  }, [isReauthorize, decodedSiteUrl, startAuthorization]);

  // Trigger confetti effect when entering success state
  useEffect(() => {
    if (pageState === "success") {
      confetti({
        particleCount: 100,
        spread: 70,
        origin: { y: 0.6 },
      });
    }
  }, [pageState]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    await startAuthorization(siteUrl);
  };

  const handleOpenAuthorizeUrl = async () => {
    if (authorizeUrl) {
      await openUrl(authorizeUrl);
    }
  };

  const handleBack = () => {
    setAuthorizeUrl(null);
    setPageState("url_input");
    setLocalPath("");
    setFolderNotEmpty(false);
    oauthSessionRef.current = null;
  };

  const handleCloseSnackbar = () => {
    setSnackbarOpen(false);
  };

  const checkDirEmpty = async (path: string) => {
    try {
      const empty = await invoke<boolean>('is_dir_empty', { path });
      setFolderNotEmpty(!empty);
    } catch {
      setFolderNotEmpty(false);
    }
  };

  const handleBrowseFolder = async () => {
    const selected = await openDialog({
      directory: true,
      multiple: false,
      title: t("addDrive.selectFolder"),
    });
    if (selected) {
      setLocalPath(selected);
      await checkDirEmpty(selected);
    }
  };

  const handleFinish = async (e: React.FormEvent) => {
    e.preventDefault();
    setPageState("setting_up");

    try {
      await invoke('finish_oauth', {
        sessionId: oauthSessionRef.current,
        driveName,
        localPath,
        driveId: isReauthorize ? driveId : undefined,
      });
      // Success - switch to success state
      setPageState("success");
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      setError(t("addDrive.errors.addDriveFailed", { message }));
      setPageState("url_input");
      setSnackbarOpen(true);
    }
  }

  const handleOpenDriveAndClose = async () => {
    await invoke("open_folder_and_close_window", { path: localPath, label: "add-drive" });
  }

  return (
    <Container maxWidth="sm" sx={{ bgcolor: "background.paper", minHeight: "100vh", position: "relative" }}>
      <IconButton
        onClick={() => invoke("close_window", { label: "add-drive" })}
        sx={{
          position: "absolute",
          top: 8,
          right: 8,
          zIndex: 10,
          color: "text.secondary",
        }}
        size="small"
      >
        <CloseIcon fontSize="small" />
      </IconButton>
      <Box
        sx={{
          minHeight: "100vh",
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
          alignItems: "center",
          py: 4,
        }}
      >
        <Box
          sx={{
            p: 4,
            py: 2,
            width: "100%",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: 1,
            borderRadius: 3,
          }}
        >
          {pageState !== "setting_up" && pageState !== "success" && <Box
            component="img"
            src={defaultLogo}
            alt="Cloudreve"
            sx={{
              width: 120,
              height: "auto",
              mb: 2,
            }}
          />}

          {pageState === "success" ? (
            // Success state - all done!
            <>
              <Typography
                variant="h1"
                sx={{ fontSize: 64, mt: 2 }}
              >
                🎉
              </Typography>
              <Typography
                sx={{ mt: 2 }}
                variant="h5"
                component="h1"
                fontWeight={500}
                textAlign="center"
              >
                {isReauthorize ? t("addDrive.reauthorizeSuccessTitle") : t("addDrive.successTitle")}
              </Typography>

              <Box
                sx={{
                  width: "100%",
                  display: "flex",
                  flexDirection: "column",
                  gap: 2,
                  mt: 2,
                }}
              >
                {isReauthorize ? (
                  <Button
                    variant="contained"
                    size="large"
                    fullWidth
                    onClick={() => invoke("close_window", { label: "add-drive" })}
                  >
                    {t("addDrive.close")}
                  </Button>
                ) : (
                  <Button
                    variant="contained"
                    size="large"
                    fullWidth
                    onClick={handleOpenDriveAndClose}
                  >
                    {t("addDrive.openDrive", { name: driveName })}
                  </Button>
                )}
              </Box>
            </>
          ) : pageState === "setting_up" ? (
            // Setting up state - loading indicator
            <>
              <CircularProgress size={48} sx={{ mt: 2 }} />
              <Typography
                sx={{ mt: 2 }}
                variant="h6"
                component="h1"
                fontWeight={500}
                textAlign="center"
              >
                {t("addDrive.settingUp")}
              </Typography>
            </>
          ) : pageState === "final_setup" ? (
            // Final setup state - local path input (hidden in reauthorize mode)
            <>
              <Typography
                sx={{ mt: 2 }}
                variant="h5"
                component="h1"
                fontWeight={500}
              >
                {isReauthorize ? t("addDrive.reauthorizeTitle") : t("addDrive.finalSetupTitle")}
              </Typography>

              <Typography
                variant="body2"
                color="text.secondary"
                textAlign="center"
              >
                {isReauthorize ? t("addDrive.reauthorizeDescription") : t("addDrive.finalSetupDescription")}
              </Typography>

              <Box
                component="form"
                onSubmit={handleFinish}
                sx={{
                  width: "100%",
                  display: "flex",
                  flexDirection: "column",
                  gap: 2,
                  mt: 2,
                }}
              >
                <FilledTextField
                  disabled={mode === "reauthorize"}
                  fullWidth
                  autoComplete="off"
                  label={t("addDrive.localDriveName")}
                  value={driveName}
                  onChange={(e) => setDriveName(e.target.value)}
                  variant="filled"
                  required
                />

                {!isReauthorize && (
                  <>
                    <FilledTextField
                      fullWidth
                      autoComplete="off"
                      label={t("addDrive.localPath")}
                      placeholder={t("addDrive.localPathPlaceholder")}
                      value={localPath}
                      onChange={(e) => {
                        setLocalPath(e.target.value);
                        setFolderNotEmpty(false);
                      }}
                      onBlur={() => localPath && checkDirEmpty(localPath)}
                      variant="filled"
                      required
                      slotProps={{
                        input: {
                          endAdornment: (
                            <InputAdornment position="end">
                              <Button
                                onClick={handleBrowseFolder}
                                size="small"
                              >
                                {t("addDrive.browse")}
                              </Button>
                            </InputAdornment>
                          ),
                        },
                      }}
                    />
                    {folderNotEmpty && (
                      <Alert severity="error" sx={{ mt: -1 }}>
                        {t("addDrive.folderNotEmpty")}
                      </Alert>
                    )}
                  </>
                )}

                <Button
                  type="submit"
                  variant="contained"
                  size="large"
                  fullWidth
                  disabled={folderNotEmpty}
                >
                  {isReauthorize ? t("addDrive.reauthorizeConfirm") : t("addDrive.finish")}
                </Button>

                <Button
                  variant="text"
                  size="large"
                  fullWidth
                  onClick={handleBack}
                >
                  {t("addDrive.back")}
                </Button>
              </Box>
            </>
          ) : pageState === "waiting" ? (
            // Waiting for sign-in state
            <>
              <Typography
                sx={{ mt: 2 }}
                variant="h5"
                component="h1"
                fontWeight={500}
              >
                {t("addDrive.waitingTitle")}
              </Typography>

              <Typography
                variant="body2"
                color="text.secondary"
                textAlign="center"
              >
                {t("addDrive.waitingDescription")}
              </Typography>

              <Box
                sx={{
                  width: "100%",
                  display: "flex",
                  flexDirection: "column",
                  gap: 2,
                  mt: 2,
                }}
              >
                <Button
                  variant="contained"
                  size="large"
                  fullWidth
                  onClick={handleOpenAuthorizeUrl}
                >
                  {t("addDrive.reopenBrowser")}
                </Button>

                <Button
                  variant="text"
                  size="large"
                  fullWidth
                  onClick={handleBack}
                >
                  {t("addDrive.back")}
                </Button>
              </Box>
            </>
          ) : (
            // Initial URL input state
            <>
              <Typography
                sx={{ mt: 2 }}
                variant="h5"
                component="h1"
                fontWeight={500}
              >
                {t("addDrive.title")}
              </Typography>

              <Typography
                variant="body2"
                color="text.secondary"
                textAlign="center"
              >
                {t("addDrive.description")}
              </Typography>

              <Box
                component="form"
                onSubmit={handleSubmit}
                sx={{
                  width: "100%",
                  display: "flex",
                  flexDirection: "column",
                  gap: 2,
                  mt: 2,
                }}
              >
                <FilledTextField
                  fullWidth
                  autoComplete="off"
                  slotProps={{
                    input: {
                      readOnly: loading,
                    },
                  }}
                  label={t("addDrive.siteUrl")}
                  placeholder={t("addDrive.siteUrlPlaceholder")}
                  value={siteUrl}
                  onChange={(e) => {
                    setSiteUrl(e.target.value);
                  }}
                  variant="filled"
                  type="url"
                  required
                />

                {siteUrl.trim().toLowerCase().startsWith("http://") && (
                  <Alert severity="error">This client requires HTTPS. Use the site's HTTPS address.</Alert>
                )}

                <Button
                  type="submit"
                  variant="contained"
                  size="large"
                  loading={loading}
                  disabled={siteUrl.trim().toLowerCase().startsWith("http://")}
                  fullWidth
                >
                  {t("addDrive.connect")}
                </Button>
              </Box>
            </>
          )}
        </Box>
      </Box>
      <Snackbar
        open={snackbarOpen}
        autoHideDuration={6000}
        onClose={handleCloseSnackbar}
        message={error}
      />
    </Container>
  );
}
