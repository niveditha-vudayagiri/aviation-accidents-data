#----------------------------------------------------------------------------------------------------#
#                     SCRAPER FOR THE AVIATION SAFETY NETWORK ACCIDENT DATABASE                      #
#----------------------------------------------------------------------------------------------------#

# Author: Niveditha Vudayagiri
# Data source: https://aviation-safety.net/database/

#-------#
# Setup #
#-------#

# Install and load necessary packages
if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
library(pacman)

p_load(dplyr, rvest, purrr, stringr, tidyr, httr)

#--------------------------#
# Create URLs for scraping #
#--------------------------#

# Define base URL and years of interest
years <- 1919:2024
base_url <- "https://asn.flightsafety.org/database/year/"

# Helper function to get the highest page number for a given year URL with user agent
get_pagenumber <- function(url) {
  
  # Use httr::GET() with user agent
  page <- tryCatch({
    httr::GET(url, httr::user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")) %>%
      content("text") %>%
      read_html()
  }, error = function(e) NULL)
  
  if (is.null(page)) return(1)  # return 1 if the page is inaccessible
  
  page %>%
    html_nodes(".pagenumbers a") %>%
    html_attr("href") %>%
    str_extract("\\d+$") %>%  # extract the last number (page number)
    as.numeric() %>%
    max(na.rm = TRUE, default = 1)  # return 1 if no page numbers are found
}

# Get the page numbers for each year URL
pagenumbers <- map_dbl(years, ~ get_pagenumber(paste0(base_url, .x)))

# Generate full URLs for all pages of each year
urls <- map2(years, pagenumbers, ~ str_c(base_url, .x, "/", seq_len(.y))) %>%
  flatten_chr()

#-----------------#
# Scrape database #
#-----------------#

# Function to extract accident data table from a URL
get_table <- function(url) {
  tryCatch({
    # Scraping the page using GET request with a user agent header
    page <- httr::GET(url, httr::user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"))
    
    # Check if the page content was successfully fetched
    if (httr::status_code(page) != 200) {
      stop("Failed to fetch the page, HTTP status code: ", httr::status_code(page))
    }
    
    # Parse the page content into HTML
    page_html <- read_html(page)
 
    # Extract the data table
    df <- page_html %>%
      html_table(header = TRUE, fill = TRUE) %>%
      .[[1]]  # Extract the first table
    
    # Extracting the links from the first column (assuming the links are in the first column)
    links <- page_html %>%
      html_nodes(".nobr a") %>%  # Select <a> tags with the class 'nobr'
      html_attr("href") 
    
    # Return the dataframe with the links in a new column
    df$links <- links
    
    return(df)
  }, error = function(e) {
    message("Error occurred: ", e$message)  # Optional: Print the error message for debugging
    NULL  # Return NULL if an error occurs
  })
}


# Function to extract additional data from the link
get_additional_data <- function(link) {
  tryCatch({
    link %>%
      httr::GET(user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")) %>%
      read_html() %>%
      html_nodes("table") %>%
      html_table(header = TRUE, fill = TRUE) %>%
      .[[1]] %>%
      # Add any processing of the additional table you need here
      as.data.frame()
  }, error = function(e) {
    message("Error occurred: ", e$message)
    NULL  # Return NULL if an error occurs
  })
}

# Function to pivot additional_data into columns
pivot_additional_data <- function(additional_data) {
  if (is.null(additional_data)) {
    return(NULL)
  }
  
  # Assuming additional_data is a data frame with the first column as key and second column as value
  additional_data_df <- as.data.frame(additional_data)
  
  # Rename columns for clarity
  colnames(additional_data_df) <- c("key", "value")
  
  # Pivot the data into wide format where 'key' becomes column headers and 'value' is the data
  additional_data_wide <- additional_data_df %>%
    tidyr::pivot_wider(names_from = key, values_from = value, values_fn = list) %>%
    unnest(cols = everything(), keep_empty = TRUE)  # Unnest values into columns
  
  return(additional_data_wide)
}

# Function to extract links and fetch additional data
extract_links_and_additional_data <- function(df) {
  # Extract links from the first column (assuming the links are in the 11th column)
  links <- df[[11]]  # Adjust the column index if necessary
  print(links)
  
  # Concatenate the base URL with each link (use paste0 for proper string concatenation)
  full_links <- paste0("https://asn.flightsafety.org", links)
  
  # Fetch additional data for each full link
  additional_data <- lapply(full_links, get_additional_data)
  print(additional_data)
  
  # Pivot the additional data from key-value pairs into columns
  additional_data_pivoted <- lapply(additional_data, pivot_additional_data)
  
  # Combine the pivoted data into the original dataframe
  df <- bind_cols(df, bind_rows(additional_data_pivoted))
  
  return(df)
}

# Scrape data from each URL
aviationsafetynet_accident_data <- lapply(urls, get_table)
aviationsafetynet_accident_data_combined <- lapply(aviationsafetynet_accident_data, extract_links_and_additional_data)

aviationsafetynet_accident_data_final <- do.call(rbind, lapply(aviationsafetynet_accident_data_combined, data.frame))

# Define the file path
csv_file_path <- file.path("~", "Documents", "DCU", "DMV","Data Scraping", "aviation_data.csv")

# Save the data as a CSV file
write.csv(aviationsafetynet_accident_data_final, csv_file_path, row.names = FALSE)

# Confirm the file has been saved
cat("Data saved to:", csv_file_path)