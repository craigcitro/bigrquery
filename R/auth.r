#' @importFrom httr oauth_endpoint oauth_app oauth2.0_token
bq_env <- new.env(parent = emptyenv())

#' Get and set access credentials
#'
#' Since the majority of bigquery API requests need to be authenticated
#' bigrquery maintains package-wide OAuth authentication credentials in a
#' private environment. In ordinary operation, you should never need to use
#' these functions but they are provided in case you want to switch
#' credentials mid-stream.
#'
#' @section API console:
#' To manage your google projects, use the API console:
#' \url{https://cloud.google.com/console}
#'
#' @keywords internal
#' @export
#' @param value new access credentials, as returned by
#'  \code{\link[httr]{oauth2.0_token}}
get_access_cred <- function() {
  cred <- bq_env$access_cred
  if (is.null(cred)) {
    for (f in bq_env$credential_fetchers) {
      error <- FALSE
      tryCatch({
        cred <- f()
      }, error = function(e) {
        write(paste0("Error when fetching OAuth2.0 credentials: ", e$message), stderr())
        error <- TRUE
      })
      if (error) {
        next
      }
      if (!is.null(cred)) {
        set_access_cred(cred)
        break
      }
    }
  }
  if(is.null(cred)) {
    stop("Failed to create OAuth2.0 credentials for executing BigQuery request.")
  }
  cred
}

#' @rdname get_access_cred
#' @export
set_access_cred <- function(value) {
  bq_env$access_cred <- value
}

#' @rdname get_access_cred
#' @export
reset_access_cred <- function() {
  set_access_cred(NULL)
}

get_sig <- function() {
  stop("Deprecated: use get_access_cred directly", call. = FALSE)
}

#' Add a new method for fetching OAuth2 credentials.
#'
#' This function will add a function to the list of default mechanisms used
#' when fetching credentials. The single argument should be a function
#' which takes an oauth endpoint, an oauth app, and a list of scopes, and
#' returns either NULL or a valid token.
#'
#' @rdname get_access_cred
#' @export
#' @keywords internal
register_credential_fetcher <- function(f) {
  fetchers <- c(f, bq_env$credential_fetchers)
  bq_env$credential_fetchers <- fetchers
}

#' Set the default mechanism for fetching OAuth2 tokens, namely the
#' "three-legged OAuth dance" (3LO). This simply asks the user to
#' authenticate our app via a browser.
fetch_oauth2_creds <- function() {
  endpoint <- oauth_endpoint(NULL, "auth", "token",
                           base_url = "https://accounts.google.com/o/oauth2")
  app <- oauth_app("google",
                     "465736758727.apps.googleusercontent.com",
                     "fJbIIyoIag0oA6p114lwsV2r")

  scopes <- c(
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform")

  oauth2.0_token(endpoint, app, scopes)
}
register_credential_fetcher(fetch_oauth2_creds)

#' Fetches a valid OAuth token using the previously set service
#' token file via the "two-legged OAuth dance" (2LO).
fetch_service_token_creds <- function() {

  endpoint <- httr::oauth_endpoints("google")

  scope = "https://www.googleapis.com/auth/bigquery"

  httr::oauth_service_token(endpoint, bq_env$service_token, scope)
}

#' Sets the service token to use when fetching the OAuth token
#' creds. This will throw an error if the file does not exist,
#' we do not have permissions to read it, or if it is not valid JSON.
#'
#' @param token_file The absolute or relative path to the file containing the service token, a string
#' @seealso Google API documentation:
#'   \url{https://developers.google.com/identity/protocols/OAuth2ServiceAccount}
#' @export
set_service_token <- function(token_file) {

  #' If the file doesn't exist, jsonlite::fromJSON will attempt to parse
  #' the string literal, which throws a less-than-intuitive error to the
  #' user of bigrquery::set_service_token
  if (!file.exists(token_file)) {
    stop("Invalid service token file; file does not exist")
  }

  #' This will throw an error if we don't have permissions to read the
  #' file or if the file contains invalid JSON.
  bq_env$service_token <- jsonlite::fromJSON(token_file)

  register_credential_fetcher(fetch_service_token_creds)
}
