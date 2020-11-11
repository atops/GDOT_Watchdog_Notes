

tsos_detector_alert_notes <- tsos_detector_alert_notes %>% 
    filter(!is.na(LastModified)) %>% 
    mutate(
        Detector = as.integer(Detector),
        across(where(is.factor), as.character),
        across(where(is.logical), as.integer),
        LastModified = as_datetime(LastModified))
tsos_detector_alert_notes <- tsos_detector_alert_notes %>% 
    add_column(
        .before = "SignalID",
        uuid = uuid::UUIDgenerate(n = nrow(tsos_detector_alert_notes)))

tsos_camera_alert_notes <- tsos_camera_alert_notes %>%
    as_tibble %>%
    filter(!is.na(LastModified)) %>% 
    mutate(
        across(where(is.factor), as.character),
        across(where(is.logical), as.integer),
        LastModified = as_datetime(LastModified))


dbWriteTable(
    conn, "tsos_detector_alert_notes", tsos_detector_alert_notes, 
    row.names = FALSE, 
    overwrite = FALSE, 
    append = TRUE)
tbl(conn, "tsos_detector_alert_notes")

dbWriteTable(
    conn, "tsos_camera_alert_notes", tsos_camera_alert_notes, 
    row.names = FALSE, 
    overwrite = FALSE, 
    append = TRUE)
tbl(conn, "tsos_camera_alert_notes")

q <- paste("CREATE TABLE `tsos_detector_alert_notes` (",
           "`uuid` VARCHAR(36),",
           "`SignalID` INT DEFAULT NULL,",
           "`Detector` INT DEFAULT NULL,",
           "`Cause.of.Malfunction` text,",
           "`Repair.Status` text,",
           "`Under.Construction` TINYINT DEFAULT NULL,",
           "`TEAMS.Task.Created` TINYINT DEFAULT NULL,",
           "`Captured.in.Mark1` TINYINT DEFAULT NULL,",
           "`ATSPM.Config.Correct` TINYINT DEFAULT NULL,",
           "`Priority` text,",
           "`Comments` text,",
           "`LastModified` DATETIME",
           ") ENGINE=InnoDB DEFAULT CHARSET=latin1")
dbSendQuery(conn, q)
dbSendQuery(conn, "CREATE UNIQUE INDEX idx_tsos_detectors ON tsos_detector_alert_notes (uuid)")

q <- paste("CREATE TABLE `tsos_camera_alert_notes` (",
           "`uuid` VARCHAR(36),",
           "`CameraID` text,",
           "`Cause.of.Malfunction` text,",
           "`Repair.Status` text,",
           "`Under.Construction` TINYINT DEFAULT NULL,",
           "`TEAMS.Task.Created` TINYINT DEFAULT NULL,",
           "`Captured.in.Mark1` TINYINT DEFAULT NULL,",
           "`ATSPM.Config.Correct` TINYINT DEFAULT NULL,",
           "`Priority` text,",
           "`Comments` text,",
           "`LastModified` DATETIME",
           ") ENGINE=InnoDB DEFAULT CHARSET=latin1")
dbSendQuery(conn, q)
dbSendQuery(conn, "CREATE UNIQUE INDEX idx_tsos_cameras ON tsos_camera_alert_notes (uuid)")


dbSendQuery(conn, "CREATE USER 'SPM'@'%' IDENTIFIED BY 'Metrics12_';")
dbSendQuery(conn, "GRANT ALL PRIVILEGES ON mark1 . * TO 'SPM'@'%';")




# Run Once to Create. -----------------
# tsos_detector_alert_notes <- data.frame(
#     SignalID = integer(),
#     Detector = character(),
#     Cause.of.Malfunction = factor(
#         levels = c("", "Resurface", "Hardware Failure", "POD", "Construction",
#                    "False Alarm", "Other - Add Comment")),
#     Repair.Status = factor(
#         levels = c("", "Troubleshooting", "OCR Created", "Under Repair")),
#     Under.Construction = logical(),
#     TEAMS.Task.Created = logical(),
#     Captured.in.Mark1 = logical(),
#     ATSPM.Config.Correct = logical(),
#     Priority = factor(levels = c("High", "Medium", "Low", "NA")),
#     Comments = character(),
#     LastModified = as.Date(character())
# )
# qsave(tsos_detector_alert_notes, "tsos_detector_alert_notes.qs")
# s3write_using(
#     tsos_detector_alert_notes,
#     qsave,
#     bucket = conf$bucket, object = "tsos_detector_alert_notes.qs",
#     opts = list(
#         key = cred$AWS_ACCESS_KEY_ID,
#         secret = cred$AWS_SECRET_ACCESS_KEY))

# 
# # Run Once to Create.
# com_levels <- levels(tsos_detector_alert_notes$Cause.of.Malfunction)
# tsos_camera_alert_notes <- tsos_detector_alert_notes %>%
#     rename(CameraID = SignalID) %>%
#     mutate(CameraID = factor(CameraID)) %>%
#     select(-Detector) %>%
#     mutate(Cause.of.Malfunction = factor(levels=com_levels[com_levels != "False Alarm"]))
# qsave(tsos_camera_alert_notes, "tsos_camera_alert_notes.qs")
# s3write_using(
#     tsos_camera_alert_notes,
#     qsave, 
#     bucket = conf$bucket, object = "tsos_camera_alert_notes.qs",
#     opts = list(
#         key = cred$AWS_ACCESS_KEY_ID,
#         secret = cred$AWS_SECRET_ACCESS_KEY))
# -------------------------------------


# Populate initial dataframe for TSOS notes
#tsos_detector_alert_notes <- qread("tsos_detector_alert_notes.qs")
#tsos_camera_alert_notes <- qread("tsos_camera_alert_notes.qs")
