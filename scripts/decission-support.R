#!/usr/bin/env Rscript

# JULEA - Flexible storage framework
# Copyright (C) 2023-2023 Julian Benda
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

packages <- c(
  "ggplot2",
  "plotly",
  "dplyr",
  "scales",
  "ggpubr",
  "knitr")
missing <- packages[!(packages %in% installed.packages()[, "Package"])]
if (length(missing)) {
  msg <- paste0(
    "Not all requirued packages are installed, you can install them with",
    "\n$ R",
    "\n> install.packages(c(\"",
    paste(missing, collapse = "\", \""),
    "\"))",
    "\n> q()")
  stop(msg)
}

suppressMessages(library(ggplot2))
suppressMessages(library(plotly))
suppressMessages(library(dplyr))
suppressMessages(library(scales))
suppressMessages(library(ggpubr))

options(knitr.kable.NA = "")
options(warn = 1)

first_round <- TRUE
last_frame <- NA

# test if all records of the same backend kind (object,kv,db)
# have executed the same order of commands
test_validity <- function(data) {
  res <- TRUE
  data <- subset(
    data,
    select = -c(time, process_uid, program_name, bson, duration))
  for (x in  unique(data$backend)) {
    first_round <<- TRUE
    check <- function(frame, key) {
      if (res == FALSE) {
        return()
      }
      frame <- frame %>% subset(select = -c(backend))
      if (first_round != TRUE) {
        if (nrow(frame) != nrow(last_frame)) {
          warning("number error, the number of accesses differ between runs")
          res <<- FALSE
         } else if (!Reduce(f = "&", x = frame == last_frame)) {
          warning("value error, the access type between runs differ")
          res <<- FALSE
        }
      }
      first_round <<- FALSE
      last_frame <<- frame
    }
    data %>% filter(backend == x) %>% group_by(type, path) %>% group_walk(check)
  }
  return(res)
}

hovertemplate <- "%{text}<extra></extra>"
replace_col_names <- function(dfs, old, new) {
  old_new <- data.frame(old, new)
  dfs <- lapply(dfs, function(df) {
    apply(old_new, 1, function(x) {
      names(df)[which(names(df) == x[1])] <<- x[2]
      return(df)
    })
    return(df)
  })
  return(dfs)
}
# create duration plot and table for one backend type
plot_durations <- function(data, key, ..., format = "simple") {

  # generate shorted backend label
  # removes path if backend only appears once
  confs <-  unique(data %>% select("type", "path"))
  map <- apply(
    confs, 1,
    function(x) paste(x[1], if (sum(confs == x[1]) > 1) x[2] else ""))
  names(map) <- paste0(confs$type, confs$path)
  data$backend <- map[paste0(data$type, data$path)]

  # analyse for runtime per operation type
  sepperate_data <- data %>%
    group_by(operation, backend) %>%
    summarise(duration = sum(duration), cnt = n(), .groups = "drop_last") %>%
    group_modify(function(df, keys) {
      df$speed_up <- max(df$duration) / df$duration
      df$time_diff <- max(df$duration) - df$duration
      return(df)
    })

  # check and convert duration unit as sensible
  unit <- "s"
  if (max(sepperate_data$duration) < 10.) {
    sepperate_data$duration <- sepperate_data$duration * 1000.
    data$duration <- data$duration * 1000.
    unit <- "ms"
  }
  label <- labs(y = paste0("time in ", unit), fill = "backend", x = "")

  # plotting
  # sort categories after largest time save
  sepperate_data$backend <- as.factor(sepperate_data$backend)
  sepperate_data$operation <- reorder(
    factor(sepperate_data$operation),
    sepperate_data$time_diff,
    FUN = max, decreasing = TRUE, na.rm = TRUE)
  off <- 0
  sepperate_data <- sepperate_data %>%
    group_modify(function(df, keys) {
      df$base <- off
      off <<- df$duration + off
      return(df)
    })
  sepperate_data <- arrange(sepperate_data, operation)

  # static plot
  operation_factor <- factor(
    sepperate_data$operation,
    level = levels(sepperate_data$operation),
    labels = paste(
      unique(sepperate_data$operation),
      "\n#", unique(sepperate_data$cnt)))
  seperate <- ggplot(sepperate_data,
      aes(y = duration, x = operation_factor, fill = backend)) +
    geom_col(position = "dodge") +
    theme(axis.title.x = element_blank(),
          axis.text.x = element_text(angle = 45)) +
    label

  # inteacrtive plotting
  text_format <- ~sprintf(
      "<b>%s</b><br><br>number: %s<br>duration: %.2f%s<br>avg: %.2f%s",
      backend, cnt, duration, unit, duration / cnt, unit)
  sepperate_ly <- plot_ly(
      data = sepperate_data,
      x = operation_factor, y = ~duration,
      color = ~backend,
      colors = hue_pal()(length(unique(sepperate_data$backend))),
      text = text_format, textposition = "none", hovertemplate = hovertemplate,
      type = "bar") %>%
    layout(
      xaxis = list(
        categoryorder = "array",
        categoryarray = levels(operation_factor)))
  total_ly <- plot_ly(
      data = sepperate_data,
      x = ~backend, y = ~duration, base = ~base,
      color = ~operation,
      colors = ~dichromat_pal("DarkRedtoBlue.12")(length(unique(operation))),
      text = text_format, textposition = "none", hovertemplate = hovertemplate,
      type = "bar", offsetgroup = 0) %>%
    layout(
      xaxis = list(
        categoryorder = "array",
        categoryarray = levels(sepperate_data$backend)))

  # table
  sepperate_data <- sepperate_data %>%
    group_modify(function(df, keys) {
      cnt <- sprintf("#%s", df$cnt[1])
      df$cnt <- sprintf("%.2f", df$duration / df$cnt)
      df <- df %>% arrange(duration) %>% add_row(cnt = cnt, .before = 0)
      return(df)}) %>%
    ungroup()
  # clean nonsens values
  sepperate_data[! is.na(sepperate_data$duration), ]$operation <- NA
  # remove column with plotting only values
  sepperate_data <- subset(sepperate_data, select = -c(base))

  # analyse for total runtime
  total_data <- data %>%
    group_by(backend) %>%
    summarise(duration = sum(duration), .groups = "drop")
  total_data <- total_data[order(total_data$backend), ]
  # plotting
  total <- ggplot(total_data,
    aes(y = duration, x = "all operations", fill = backend)) +
    geom_col(position = "dodge") +
    label
  total_data$speed_up <- max(total_data$duration) / total_data$duration
  total_data$time_diff <- max(total_data$duration) - total_data$duration
  total_data <- arrange(total_data, duration)

  # generate HTML/text table with links
  plot_file_name <- paste(key[1], "time.svg", sep = "_")
  plot_html_file_name <- paste(key[1], "time.html", sep = "_")
  renamed <- replace_col_names( # rename column for table
    list(total_data = total_data, sepperate_data = sepperate_data),
    c("duration", "time_diff", "speed_up"),
    c(sprintf("duration (%s)", unit),
      sprintf("time diff (%s)", unit),
      "speed up")
  )
  table_sepperate_str <- knitr::kable(
      renamed$sepperate_data,
      format, digits = 2, table.attr = "class=\"fancy\"")
  table_total_str <- knitr::kable(
      renamed$total_data, format,
      digits = 2, table.attr = "class=\"fancy\"")

  if (format == "html") {
    cat(
      "<h2 id=\"", as.character(key[1]), "\">",
      as.character(key[1]),
      "</h2>", sep = "")
    cat(
      "<table><tbody><tr><td rowspan=\"2\">",
      table_sepperate_str,
      "</td><td style=\"height:10px\">",
      table_total_str,
      "</td></tr><tr><td style=\"vertical-align:top\">",
      "<a href=\"./",
      plot_html_file_name,
      "\" style=\"display: inline-block\">",
      "<object data=\"./",
      plot_file_name,
      "\" alt=\"barplot of results listed in table\"",
      "style=\"width:100%;pointer-events: none\"></object></a>",
      "</td></tr></tbody></table>", sep = "")
  } else {
    cat(
      as.character(key[1]),
      paste0("plot: ",
        plot_file_name), "",
        table_total_str, "",
        table_sepperate_str, "",
        "", sep = "\n")
  }

  # combining static plots and store them
  plot <- ggarrange(seperate, total,
    widths = c(4, 1), ncol = 2, nrow = 1,
    common.legend = TRUE, legend = "bottom")
  ggsave(plot_file_name, plot,
    unit = "in", width = 11, height = 6, bg = "white")

  # combine interactive plots and store them
  n <- length(unique(sepperate_data$backend)) - 1
  htmlwidgets::saveWidget(
    subplot(
      sepperate_ly,
      total_ly,
      widths = c(0.8, 0.2)) %>%
    layout(
      font = list(size = 18),
      yaxis = list(title = label$y),
      hoverlabel = list(bgcolor = "white"),
      barmode = "group",
      legend = list(traceorder = "normal")) %>%
    htmlwidgets::onRender(
      sprintf(
        read_file("interactive.js"),
        n)),
    plot_html_file_name,
    selfcontained = FALSE,
    libdir = "lib")
}

# main
# setup, load data
args <- commandArgs(trailingOnly = FALSE)
script_dir <- dirname(sub("--file=", "", args[grep("--file=", args)]))
read_file <- function(name) {
  return(readLines(paste(script_dir, name, sep = "/")))
}
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop(
    paste(
      "usage:",
      "script", # TODO check if script name can be determined
      "<summary.csv> <- generated by decission-support.sh",
      "[(print|html)] <- output as simple print(default) or as html",
      sep = "\n"),
    call. = FALSE)
}
data <- read.csv(args[1])
as_html <- length(args) >= 2 & args[2] == "html"

# check data
res <- test_validity(data)
if (res == FALSE) {
  stop(
    "Access reports between different configurations are not consistent!",
    call. = FALSE)
}

# print html header
if (as_html) {
  cat(
    "<!DOCTYPE html><html lang=\"en\">",
    "<head><style type=\"text/css\">",
    paste(read_file("style.css"), collapse = "\n"),
    "</style></head><body><header>",
    "<nav><ul>", sep = "")
  for (backend in unique(data$backend)) {
    cat("<li><a href=\"#", backend, "\">", backend, "</a></li>", sep = "")
  }
  cat("</ul></nav></header>")
}

data %>%
  group_by(backend) %>%
  group_walk(plot_durations, format = if (as_html) "html" else "simple")

# print html closing
if (as_html) {
  cat("</body></html>")
}
