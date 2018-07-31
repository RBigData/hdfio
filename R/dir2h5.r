csv2h5_validation_dir = function(files, h5_fp)
{
  # Validate colnames across csv files
  colnames = csv_colnames(files[1])
  for (file in files[-1])
  {
    colnames2 = csv_colnames(file)
    if (!identical(colnames, colnames2))
      close_and_stop(h5_fp, "column names are not the same across files; are they actually mergeable? Re-run with yolo=TRUE to ignore")
  }
}



csv2h5_dir = function(files, h5_fp, dataset, format, yolo, verbose, compression, ...)
{
  if (format == "column")
  {
    writer = write_h5df_column
    writer_init = write_h5df_column_init
  }
  
  else if (format == "compound") 
  {
    writer = write_h5df_compound
    writer_init = write_h5df_compound_init
  }
  
  if (isTRUE(yolo))
  {
    verbprint(verbose, "Living dangerously...\n")
    strlens = NULL
  }
  else
  {
    verbprint(verbose, "Checking input files for common header lines...")
    csv2h5_validation_dir(files, h5_fp)
    verbprint(verbose, "ok!\n")
    
    verbprint(verbose, "Scanning all input files for storage info...")
    strlens = csv2h5_get_strlen(files)
    verbprint(verbose, "ok!\n")
  }
  
  
  start_ind = 1
  
  n = length(files)
  verbprint(verbose, paste("Processing", length(files), "files:\n"))
  for (file in files)
  {
    verbprint(verbose, paste0("    ", file, ": reading..."))
    # TODO batch process csv files?
    
    x = csv_reader(file, ...)
    
    if (start_ind == 1)
      types = writer_init(x, h5_fp, dataset, strlens=strlens, compression=compression)
    
    verbprint(verbose, "ok! writing...")
    writer(x, start_ind, h5_fp, dataset,types)
    verbprint(verbose, "ok!\n")
    
    start_ind = start_ind + NROW(x)
  }
  
  verbprint(verbose, "done!\n")
  invisible(TRUE)
}



#' dir2h5
#' 
#' Convert a directory of csv files to HDF5 datasets.
#' 
#' @param csvdir
#' Valid directory containing csv files
#' @param h5out
#' Output file.
#' @param dataset
#' Dataset in input file to read or \code{NULL}. In the latter case (e.g. \code{NULL}), the dataset (named "data") will be contained 
#' within a group named as the input dataset
#' @param combined
#' #' Logical.  If \code{TRUE}, the csv files will be writen as a single HDF5 dataset.  If \code{FALSE},
#' the datasets will be contained within distinct groups indexed by csv name.
#' @param format
#' Method chosen for writing out h5 file.  If \code{column}, each column of the input dataset is written 
#' out on disk as x_i with "i" being an arbitrary column index, ranging as intengers from 1:ncol(dataframe). 
#' If \code{compound}, the entire input dataset is written out on disk 
#' as a complete dataframe.
#' @param compression
#' HDF5 compression level. An integer, 0 (least compression) to 9 (most compression).
#' @param yolo
#' Logical. If \code{FALSE}...
#' @param verbose
#' Logical. Detailed information on \code{R} processes are shown for HDF5 processes. Default is \code{FALSE}.
#' 
#' @examples
#' \dontrun{ #TODO FIXME
#' library(utils)
#' library(hdfio)
#' library(hdf5r)
#' df = data.frame(x=seq(1:5), y = c(runif(5)), z= c("Peter", "Amber", "John", "Lindsey", "Steven")) 
#' utils::write.csv(x = df, file = paste(tempdir(),"df.csv",sep="/"), row.names = FALSE) 
#' df2 <- data.frame(a = runif(10), b=seq(1:10))
#' utils::write.csv(x = df2, file = paste(tempdir(),"df2.csv",sep="/"), row.names = FALSE) 
#' list.files(tempdir())
#' 
#' #dir2h5 (column format)
#' dir2h5(tempdir(), h5out = paste(tempdir(),"result.h5",sep="/"), dataset=NULL, combined=FALSE, format = "column", compression=4) 
#' 
#' #Results
#' result <- h5file(paste(tempdir(), "result.h5",sep = "/))
#' result$ls(recursive=TRUE)
#' 
#' #dir2h5 (compound format)
#' dir2h5(tempdir(), h5out = paste(tempdir(),"result2.h5",sep="/"),
#' dataset=NULL, combined=FALSE, format = "compound", compression=4)
#' #Results:
#' result2 <- h5file(paste(tempdir(), "result2.h5",sep = "/"))
#' result2$ls(recursive=TRUE)
#' }
#' 
#' @rdname dir2h5
#' @export
dir2h5 = function(csvdir, h5out, dataset=NULL, recursive=FALSE, combined=TRUE, header="all", format="column", compression=4, yolo=FALSE, verbose=FALSE, ...)
{
  check.is.string(csvdir)
  check.is.string(h5out)
  if (!is.null(dataset))
    check.is.string(dataset)
  check.is.flag(recursive)
  check.is.flag(combined)
  if (is.character(header))
    header = match.arg(tolower(header), c("all", "first", "auto"))
  else
    check.is.flag(header)
  format = match.arg(tolower(format), c("column", "compound"))
  check.is.natnum(compression)
  if (compression > 9 || compression < 0)
    stop("argument 'compression' must be an integer in the range 0 to 9")
  check.is.flag(yolo)
  check.is.flag(verbose)
  
  
  h5_fp = h5file(h5out, mode="a")
  
  if (is.null(dataset))
  {
    if (isTRUE(combined))
    {
      dataset = h5_infer_dataset(csvdir, dir=TRUE)
      h5_check_dataset(h5_fp, dataset)
    }
  }
  
  files = dir(csvdir, pattern="*\\.csv$", full.names=TRUE, ignore.case=TRUE)
  if (length(files) == 0)
    close_and_stop(h5_fp, paste0("no csv files found in csvdir=", csvdir))
  
  if (isTRUE(combined))
    csv2h5_dir(files, h5_fp, dataset, format, yolo, verbose, compression, ...)
  else
  {
    verbprint(verbose, paste("Processing", length(files), "files:\n"))
    for (file in files)
    {
      verbprint(verbose, paste0("    ", file, " "))
      dataset = h5_infer_dataset(file)
      csv2h5_file(file, h5_fp, dataset, format, yolo, verbose, compression, ...)
    }
  }
  
  
  h5close(h5_fp)
}
