# Client code to connect to SparkRBackend

# Creates a SparkR client connection object
# if one doesn't already exist
init <- function(hostname = "localhost", port = 20151L, timeout=60) {
  if (exists(".sparkRCon", envir=.sparkREnv)) {
    cat("SparkRBackend client connection already exists\n")
    return(get(".sparkRCon", envir=.sparkREnv))
  }

  con <- socketConnection(host=hostname, port=port, server=FALSE,
                          blocking=TRUE, open="wb", timeout=timeout)

  assign(".sparkRCon", con, envir=.sparkREnv)
  get(".sparkRCon", envir=.sparkREnv)
}

launchBackend <- function(
    port = 20151L, 
    javaOpts="-Xms2g -Xmx2g",
    javaHome=Sys.getenv("JAVA_HOME")) {
  if (javaHome != "") {
    java_bin <- paste(javaHome, "bin", "java", sep="/")
  } else {
    java_bin <- "java"
  }
  jar <- get("assemblyJarPath", .sparkREnv)
  mainClass <- "edu.berkeley.cs.amplab.sparkr.SparkRBackend"
  command <- paste(java_bin, javaOpts, "-cp", jar, mainClass, port, sep=" ")
  cat("Launching java with command ", command, "\n")
  invisible(system(command, intern=FALSE, ignore.stdout=F, ignore.stderr=F, wait=F))
}

getConnection <- function() {
  if (!exists(".sparkRCon", .sparkREnv)) {
    stop("No connection to backend found")
  }

  get(".sparkRCon", .sparkREnv)
}

invokeJava <- function(rpcName, ...) {
  conn <- getConnection()
  
  rc <- rawConnection(raw(0), "r+")
  
  writeString(rc, rpcName)
  writeList(rc, list(...))

  bytesToSend <- rawConnectionValue(rc)
  writeInt(conn, length(bytesToSend))
  writeBin(bytesToSend, conn)
  
  # TODO: check the status code to output error information
  returnStatus <- readInt(conn)
  stopifnot(returnStatus == 0)
  readObject(conn)
} 