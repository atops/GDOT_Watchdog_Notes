
suppressMessages({
    library(aws.s3)
    library(qs)
    library(DBI)
    library(pool)
    library(shiny)
    library(tidyverse)
    library(lubridate)
    library(glue)
    library(DT)
    library(fontawesome)
    library(yaml)
    library(arrow)
    library(readxl)
    source("dtedit_ben.R")
})

#conf_mode <- "production"
#source("Monthly_Report_UI_Functions.R")


# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

DARK_GRAY = "#636363"
BLACK = "#000000"
DARK_GRAY_BAR = "#252525"
LIGHT_GRAY_BAR = "#bdbdbd"

RED2 = "#e41a1c"
GDOT_BLUE = "#045594"; GDOT_BLUE_RGB = "#2d6797"
GDOT_YELLOW = "#EEB211"; GDOT_YELLOW_RGB = "rgba(238, 178, 17, 0.80)"

conf <- read_yaml("Monthly_Report.yaml")
cred <- read_yaml("Monthly_Report_AWS.yaml")


VERY_LIGHT_BLUE <- "#deebf7"
yes_icon <- fa("check-circle", fill = GDOT_BLUE, height = "2em") %>% as.character()  # "black"
no_icon <- fa("circle", fill = LIGHT_GRAY_BAR, height = "2em") %>% as.character()  #  (very) LIGHT_BLUE

styl <- "font-family: Source Sans Pro; font-size: 14px; padding-top: 12px"



get_aurora_connection <- function(f = RMySQL::dbConnect) {
    
    f(drv = RMySQL::MySQL(),
      host = cred$RDS_HOST,
      port = 3306,
      dbname = cred$RDS_DATABASE,
      username = cred$RDS_USERNAME,
      password = cred$RDS_PASSWORD)
}

get_aurora_connection_pool <- function() {
    get_aurora_connection(dbPool)
}



get_det_config_ben <- function() {
    s3read_using(qread, 
        bucket = conf$bucket, 
        object = "ATSPM_Det_Config_Good_Latest.qs",
        opts = list(
            key = cred$AWS_ACCESS_KEY_ID,
            secret = cred$AWS_SECRET_ACCESS_KEY,
            region = cred$AWS_DEFAULT_REGION)
        ) %>% 
        filter(!is.na(ApproachDesc)) %>%
        transmute(
            SignalID = factor(SignalID), 
            Description = paste(trimws(PrimaryName), trimws(SecondaryName), sep = " & "), 
            CallPhase, 
            Detector, 
            ApproachDesc, 
            LaneNumber,
            Type = factor(Name), 
            Selector = paste0(Detector, ": ", ApproachDesc, " Lane ", LaneNumber, "/", Type))
}



# New version on 4/5. To join with MaxView ID
# Updated on 4/14 to add 'Include' flag
get_cam_config <- function(object, bucket, corridors) {
    
    cam_config0 <- aws.s3::s3read_using(
        function(x) read_excel(x, range = "A1:M3280"), 
        object = object, 
        bucket = bucket,
        opts = list(
            key = cred$AWS_ACCESS_KEY_ID,
            secret = cred$AWS_SECRET_ACCESS_KEY,
            region = cred$AWS_DEFAULT_REGION)) %>%
        filter(Include == TRUE) %>%
        transmute(
            CameraID = factor(CameraID), 
            Location, 
            Zone_Group, 
            Zone,
            Corridor = factor(Corridor), 
            SignalID = factor(`MaxView ID`),
            As_of_Date = date(As_of_Date)) %>%
        distinct()
    
    corrs <- corridors %>%
        select(SignalID, Zone_Group, Zone, Corridor, Subcorridor)
    
    cams <- cam_config0 %>% 
        left_join(corrs, by=c("SignalID")) %>% 
        filter(!(is.na(Zone.x) & is.na(Zone.y)))
    
    cams1 <- cams %>% filter(is.na(Zone.y)) %>% 
        transmute(
            CameraID,
            Location,
            Zone_Group = Zone_Group.x,
            Zone = Zone.x,
            Corridor = Corridor.x,
            Subcorridor,
            As_of_Date)
    
    cams2 <- cams %>% filter(!is.na(Zone.y)) %>% 
        transmute(
            CameraID,
            Location,
            Zone_Group = Zone_Group.y,
            Zone = Zone.y,
            Corridor = Corridor.y,
            Subcorridor,
            As_of_Date)
    
    bind_rows(cams1, cams2) %>%
        mutate(
            Description = paste(CameraID, Location, sep = ": "),
            CameraID = factor(CameraID),
            Zone_Group = factor(Zone_Group),
            Zone = factor(Zone),
            Corridor = factor(Corridor),
            Subcorridor = factor(Subcorridor)) %>%
        arrange(Zone_Group, Zone, Corridor, CameraID)
}

get_all_corridors_ben <- function() {
    s3read_using(
        qs::qread, bucket = conf$bucket, object = "all_Corridors_Latest.qs",
        opts = list(
            key = cred$AWS_ACCESS_KEY_ID,
            secret = cred$AWS_SECRET_ACCESS_KEY,
            region = cred$AWS_DEFAULT_REGION)) %>%
        arrange(as.integer(as.character(SignalID)), Name) %>%
        mutate(Description = factor(Description, levels = unique(Description)))
}



get_alerts_ben <- function() {
    
    # Read Alerts from S3
    alerts <- s3read_using(
        qread, bucket = conf$bucket, object = "mark/watchdog/alerts.qs",
        opts = list(
            key = cred$AWS_ACCESS_KEY_ID,
            secret = cred$AWS_SECRET_ACCESS_KEY,
            region = cred$AWS_DEFAULT_REGION))
    
    most_recent_date <- alerts %>% 
        group_by(Alert) %>% 
        summarize(maxDate = max(Date), .groups = "drop")
    
    # Just take the two types we care about tracking
    detector_alerts <- alerts %>% 
        filter(Alert %in% c("Count", "Bad Vehicle Detection")) %>%
        left_join(most_recent_date, by = c("Alert")) %>%
        group_by(Zone_Group, Zone, Corridor, SignalID, CallPhase, Detector, Alert) %>%
        filter(
            Date == maxDate, 
            ApproachDesc == max(ApproachDesc)) %>%
        group_by(SignalID, Detector) %>% filter(Name == min(Name)) %>%
        ungroup() %>%
        arrange(Zone, Corridor, SignalID, Detector, CallPhase)

    detector_alerts$Alert_Stk <- glue("{detector_alerts$Alert} ({detector_alerts$streak} days)")
    
    detector_alerts <- detector_alerts %>% 
        group_by(Zone, Corridor, SignalID, CallPhase, Detector, Name, ApproachDesc) %>%
        summarize(alerts = paste0(Alert_Stk, collapse = "|"), .groups = "keep") %>%
        mutate(
            SignalID = as.integer(as.character(SignalID)),
            CallPhase = as.character(CallPhase),
            Detector = as.character(Detector),
            Captured.in.Mark1 = TRUE) %>%
        ungroup() %>%
        arrange(SignalID, CallPhase, Detector)

    camera_alerts <- alerts %>% 
        filter(Alert == "No Camera Image") %>%
        rename(CameraID = SignalID) %>%
        left_join(most_recent_date, by = c("Alert")) %>%
        group_by(Zone, Corridor, CameraID, Alert) %>%
        filter(Date == maxDate) %>%
        ungroup() %>%
        select(-c(Zone_Group, Date, maxDate, CallPhase, Detector, ApproachDesc)) %>%
        arrange(Zone, Corridor, CameraID)
        
    camera_alerts$Alert_Stk <- glue("{camera_alerts$Alert} ({camera_alerts$streak} days)")
    
    camera_alerts <- camera_alerts %>%
        mutate(CameraID = as.character(CameraID),
               Captured.in.Mark1 = TRUE) %>%
        select(-c(Alert, streak)) %>%
        rename(alerts = Alert_Stk)
        
    rm(alerts)

    return(list(camera_alerts = camera_alerts, detector_alerts = detector_alerts))
}
# alerts <- get_alerts_ben()
# qsave(alerts$camera_alerts, "camera_alerts.qs")
# qsave(alerts$detector_alerts, "detector_alerts.qs")

print("Create connection pool...")
conn_pool <- get_aurora_connection_pool()
print("Created.")


##### Create the Shiny server
server <- function(input, output, session) {

    
    
    
    onStop(function() {
        poolReturn(conn)
    })
    
    
    print("Checkout connection from pool...")
    conn <- pool::poolCheckout(conn_pool)
    print("Checked out.")
    
    # Read notes from database
    tsos_detector_alert_notes <- tbl(
        conn, "tsos_detector_alert_notes_test") %>%
        collect() %>%
        # the below doesn't work in MySQL before collect
        group_by(SignalID, Detector) %>% 
        filter(LastModified == max(LastModified, na.rm = TRUE)) %>%
        ungroup() %>%
        mutate(
            SignalID = as.integer(SignalID),
            Detector = as.character(Detector),
            Cause.of.Malfunction = factor(
                Cause.of.Malfunction,
                levels = c("", "Resurface", "Hardware Failure", "POD", "Construction",
                           "False Alarm", "Other - Add Comment")),
            Repair.Status = factor(
                Repair.Status,
                levels = c("", "Troubleshooting", "OCR Created", "Under Repair")),
            Under.Construction = as.logical(Under.Construction),
            TEAMS.Task.Created = as.logical(TEAMS.Task.Created),
            Captured.in.Mark1 = as.logical(Captured.in.Mark1),
            ATSPM.Config.Correct = as.logical(ATSPM.Config.Correct),
            Priority = factor(Priority, levels = c("High", "Medium", "Low", "NA")),
            Comments = as.character(Comments),
            LastModified = as_datetime(LastModified)
        )
    
    
    tsos_camera_alert_notes <- tbl(
        conn, "tsos_camera_alert_notes_test") %>%
        collect() %>%
        # the below doesn't work in MySQL before collect
        group_by(CameraID) %>% 
        filter(LastModified == max(LastModified, na.rm = TRUE)) %>%
        ungroup() %>%
        mutate(
            CameraID = as.character(CameraID),
            Cause.of.Malfunction = factor(
                Cause.of.Malfunction,
                levels = c("", "Resurface", "Hardware Failure", "POD", "Construction",
                           "Other - Add Comment")),
            Repair.Status = factor(
                Repair.Status,
                levels = c("", "Troubleshooting", "OCR Created", "Under Repair")),
            Under.Construction = as.logical(Under.Construction),
            TEAMS.Task.Created = as.logical(TEAMS.Task.Created),
            Captured.in.Mark1 = as.logical(Captured.in.Mark1),
            ATSPM.Config.Correct = as.logical(ATSPM.Config.Correct),
            Priority = factor(Priority, levels = c("High", "Medium", "Low", "NA")),
            Comments = as.character(Comments),
            LastModified = as_datetime(LastModified)
        )
    

    alerts <- get_alerts_ben()
    det_config <- get_det_config_ben()
    all_corridors <- get_all_corridors_ben()
    corridors <- all_corridors %>% 
        filter(as.integer(as.character(SignalID)) > 0)
    cam_config <- get_cam_config(
        object = conf$cctv_config_filename, 
        bucket = conf$bucket,
        corridors = all_corridors)

    camera_alerts <- alerts$camera_alerts %>%
        as.data.frame() %>%
        left_join(
            tsos_camera_alert_notes,
            by = c("CameraID"), suffix = c(".mark1", ".notes")) %>%
        mutate(
            Comments = as.character(Comments),
            across(where(is.logical), ~replace_na(.x, FALSE)),
            across(where(is.character), ~replace_na(.x, "")),
            Captured.in.Mark1 = Captured.in.Mark1.mark1 | Captured.in.Mark1.notes) %>%
        select(-c(Captured.in.Mark1.mark1, Captured.in.Mark1.notes))
    detector_alerts <- alerts$detector_alerts %>%
        as.data.frame() %>%
        left_join(
            tsos_detector_alert_notes,
            by = c("SignalID", "Detector"), suffix = c(".mark1", ".notes")) %>%
        mutate(
            Comments = as.character(Comments),
            across(where(is.logical), ~replace_na(.x, FALSE)),
            across(where(is.character), ~replace_na(.x, "")),
            Captured.in.Mark1 = Captured.in.Mark1.mark1 | Captured.in.Mark1.notes) %>%
        select(-c(Captured.in.Mark1.mark1, Captured.in.Mark1.notes))

    # Debug step
    #print(as_tibble(detector_alerts))
    
    ##### Callback functions
    detector.update.callback <- function(data, olddata, row) {
        data[row, "uuid"] <- uuid::UUIDgenerate()
        data[row, "LastModified"] <- now()
        detector_alerts[row,] <- data[row,]
        return(detector_alerts)
    }
    
    detector.insert.callback <- function(data, row) {
        data[row, "uuid"] <- uuid::UUIDgenerate()
        data[row, "LastModified"] <- now()
        detector_alerts <- rbind(detector_alerts, data[row,])
        return(detector_alerts)
    }
    
    detector.delete.callback <- function(data, row) {
        detector_alerts <- data[-row,]
        return(detector_alerts)
    }

    camera.update.callback <- function(data, olddata, row) {
        data[row, "uuid"] <- uuid::UUIDgenerate()
        data[row, "LastModified"] <- now()
        camera_alerts[row,] <- data[row,]
        return(camera_alerts)
    }

    camera.insert.callback <- function(data, row) {
        data[row, "uuid"] <- uuid::UUIDgenerate()
        data[row, "LastModified"] <- now()
        camera_alerts <- rbind(camera_alerts, data[row,])
        return(camera_alerts)
    }
    
    camera.delete.callback <- function(data, row) {
        camera_alerts <- data[-row,]
        return(camera_alerts)
    }
    
    da <- reactive({
        
        view_cols <- c(
            "Zone", "Corridor", "SignalID", "CallPhase", "Detector", "Name", "ApproachDesc",
            "alerts", "Cause.of.Malfunction", "Repair.Status", "Under.Construction",
            "TEAMS.Task.Created", "Captured.in.Mark1", "ATSPM.Config.Correct",
            "Priority", "Comments", "LastModified")
        # view_col_names <- c(
        #     "Zone", "Corridor", "SignalID", "Phase", "Detector", "Name", "Approach",
        #     "Alert (Streak)", "Cause of Malfunction", "Repair Status", "Under Construction",
        #     "TEAMS Task Created", "Captured in Mark1", "ATSPM Config Correct",
        #     "Priority", "Comments", "LastModified")
        edit_cols <- c(
            "Cause.of.Malfunction", "Repair.Status", 
            "Under.Construction", "TEAMS.Task.Created", "ATSPM.Config.Correct", 
            "Priority", "Comments") # "Captured.in.Mark1", 
        boolean_cols <- 10:13
        centered_cols <- c(2:4, 10:14)
        watchdog_cols <- 0:7
        
        dtedit(
            session,
            input,
            output,
            name = "detector_alerts_dt",
            thedata = detector_alerts,
            edit.cols = edit_cols,
            edit.label.cols = gsub("\\.", " ", edit_cols),
            selectize = TRUE,
            modal.size = 'm',
            view.cols = view_cols,
            label.add = "Add",
            label.edit = "Edit",
            label.delete = "Delete",
            label.copy = "Duplicate",
            show.delete = T,
            show.insert = T,
            show.copy = T,
            callback.update = detector.update.callback,
            callback.insert = detector.insert.callback,
            callback.delete = detector.delete.callback,
            corridors = corridors,
            det_config = det_config,
            cam_config = NULL,
            datatable.options = list(
                dom = 'tp', scrollX = TRUE, scrollY = 600, pageLength = 500,
                autoWidth = TRUE,
                initComplete = JS(
                    "function(settings, json) {",
                    "$(this.api().table().container()).css({'font-size': '10pt', 'font-family': 'Source Sans Pro'});",
                    "}"),
                columnDefs = list(list(className = 'dt-center', targets = centered_cols),
                                  list(width = '120px', targets = c(0)),
                                  list(width = '200px', targets = c(1)),
                                  list(width = '40px', targets = c(2,3,4)),
                                  list(width = '180px', targets = c(8)),
                                  list(width = '120px', targets = c(9)),
                                  list(width = '50px', targets = c(10,11,12,13))),
                rowCallback = JS(
                    "function(row, data, index) {",
                    sprintf("function icon(x) { let result; if (x==true) {result = '%s';} else {result = '%s'} return result }", yes_icon, no_icon),
                    paste(glue("$(this.api().cell(index, {boolean_cols}).node()).html( icon(data[index,{boolean_cols}]) );"), collapse = " "),
                    paste(glue("$(this.api().cell(index, {watchdog_cols}).node()).css('background-color', '{VERY_LIGHT_BLUE}');"), collapse = " "),
                    "}"),
                
                # preDrawCallback = JS(
                #     "function (settings) {",
                #     "pageScrollPos = $('div.dataTables_scrollBody').scrollTop();",
                #     "alert(pageScrollPos)",
                #     "},")
                
                #drawCallback = JS(
                #    "function (settings) {",
                #    "$('div.dataTables_scrollBody').scrollTop(pageScrollPos);",
                #    "}")
                
                #drawCallback = JS("function(settings) { $(this.api().row(50).node()).scrollIntoView() }")
                ),
            
            colnames = c(
                "Zone", "Corridor", "SignalID", "Phase", "Detector", "Name", "Approach",
                "Alert (Streak)", "Cause of Malfunction", "Repair Status", "Under Construction",
                "TEAMS Task Created", "Captured in Mark1", "ATSPM Config Correct",
                "Priority", "Comments", "LastModified"),
            filter = "top",
            class = 'cell-border stripe compact',
            escape = FALSE
        )
    })
    
    observe({
        # The Magic (also need to return da as a reactive instead of wrapping dtedit in an observe block)
        detector_alerts <<- da()$thedata
        # Get records not already in the database
        det_alert_notes_to_upload <- detector_alerts %>% 
            select(names(tsos_detector_alert_notes)) %>%
            filter(
                !is.na(LastModified),
                !uuid %in% dbReadTable(conn, "tsos_detector_alert_notes_test")$uuid) %>%
            mutate(across(where(is.logical), as.integer))
        
        # Upload records not already in the database
        dbWriteTable(
            conn, "tsos_detector_alert_notes_test", det_alert_notes_to_upload, 
            row.names = FALSE, 
            overwrite = FALSE, 
            append = TRUE)
    })


    ca <- reactive({
        
        view_cols <- c(
            "Zone", "Corridor", "CameraID", "Name", 
            "alerts", "Cause.of.Malfunction", "Repair.Status", "Under.Construction",
            "TEAMS.Task.Created", "Captured.in.Mark1", "ATSPM.Config.Correct",
            "Priority", "Comments", "LastModified")
        # view_col_names <- c(
        #     "Zone", "Corridor", "CameraID", "Name",
        #     "Alert (Streak)", "Cause of Malfunction", "Repair Status", "Under Construction",
        #     "TEAMS Task Created", "Captured in Mark1", "ATSPM Config Correct",
        #     "Priority", "Comments", "LastModified")
        edit_cols <- c(
            "Cause.of.Malfunction", "Repair.Status", 
            "Under.Construction", "TEAMS.Task.Created", "ATSPM.Config.Correct", 
            "Priority", "Comments") # "Captured.in.Mark1", 
        boolean_cols <- 7:10
        centered_cols <- 7:11
        watchdog_cols <- 0:4
        
        dtedit(
            session,
            input,
            output,
            name = "camera_alerts_dt",
            thedata = camera_alerts,
            edit.cols = edit_cols,
            edit.label.cols = gsub("\\.", " ", edit_cols),
            selectize = TRUE,
            modal.size = 'm',
            view.cols = view_cols,
            label.add = "Add",
            label.edit = "Edit",
            label.delete = "Delete",
            label.copy = "Duplicate",
            show.delete = T,
            show.insert = T,
            show.copy = T,
            callback.update = camera.update.callback,
            callback.insert = camera.insert.callback,
            callback.delete = camera.delete.callback,
            corridors = NULL,
            det_config = NULL,
            cam_config = cam_config,
            datatable.options = list(
                dom = 'tp', scrollX = TRUE, scrollY = 600, pageLength = 500,
                autoWidth = FALSE,
                initComplete = JS(
                    "function(settings, json) {",
                    "$(this.api().table().container()).css({'font-size': '10pt', 'font-family': 'Source Sans Pro'});",
                    "}"),
                columnDefs = list(list(className = 'dt-center', targets = centered_cols),
                                  list(width = '120px', targets = c(0)),
                                  list(width = '200px', targets = c(1)),
                                  list(width = '180px', targets = c(5)),
                                  list(width = '120px', targets = c(6)),
                                  list(width = '50px', targets = c(7,8,9,10))),
                rowCallback = JS(
                    "function(row, data, index) {",
                    sprintf("function icon(x) { let result; if (x==true) {result = '%s';} else {result = '%s'} return result }", yes_icon, no_icon),
                    paste(glue("$(this.api().cell(index, {boolean_cols}).node()).html( icon(data[index,{boolean_cols}]) );"), collapse = " "),
                    paste(glue("$(this.api().cell(index, {watchdog_cols}).node()).css('background-color', '{VERY_LIGHT_BLUE}');"), collapse = " "),
                    "}")
            ),
            colnames = c(
                "Zone", "Corridor", "CameraID", "Name",
                "Alert (Streak)", "Cause of Malfunction", "Repair Status", "Under Construction",
                "TEAMS Task Created", "Captured in Mark1", "ATSPM Config Correct",
                "Priority", "Comments", "LastModified"),
            filter = "top",
            class = 'cell-border stripe compact',
            escape = FALSE
        )
    })

    observe({
        # The Magic (also need to return ca as a reactive instead of wrapping dtedit in an observe block)
        camera_alerts <<- ca()$thedata
        # Get records not already in the database
        cam_alert_notes_to_upload <- camera_alerts %>% 
            select(names(tsos_camera_alert_notes)) %>%
            filter(
                !is.na(LastModified),
                !uuid %in% dbReadTable(conn, "tsos_camera_alert_notes_test")$uuid) %>%
            mutate(across(where(is.logical), as.integer))
        
        # Upload records not already in the database
        dbWriteTable(
            conn, "tsos_camera_alert_notes_test", cam_alert_notes_to_upload, 
            row.names = FALSE, 
            overwrite = FALSE, 
            append = TRUE)
    })
}


##### Create the shiny UI
ui <- fluidPage(
    
    includeCSS("www/styles.css"),
    titlePanel(title = "", windowTitle = "Mark1 Watchdog Alerts"),
    
    h3('Watchdog Alerts and Notes', style = "font-family: Source Sans Pro"),
    tabsetPanel(
        type = "tabs",
        tabPanel(
            "Detectors",
            uiOutput("detector_alerts_dt"),
            div("Some detector explanatory text here", style = styl)),
        tabPanel(
            "Cameras", 
            uiOutput("camera_alerts_dt"),
            div("Some camera explanatory text here.", style = styl))
    )
    
    #div("------------------------------------------"),
    #DTOutput("alerts_datatable")
)


##### Start the shiny app
shinyApp(ui = ui, server = server)
