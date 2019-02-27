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
  
  n = length(files)
  verbprint(verbose, "Processing ", length(files), " files:\n")
  preprint = "    "
  
  if (isTRUE(yolo))
  {
    verbprint(verbose, preprint, "Living dangerously...")
    strlens = NULL
    t_header = t_scan = timeprint(verbose)
  }
  else
  {
    t_header0 = verbprint(verbose, preprint, "Checking column names...")
    csv2h5_validation_dir(files, h5_fp)
    t_header1 = verbprint(verbose, "ok!")
    t_header = timeprint(verbose, t_header0, t_header1)
    
    t_scan0 = verbprint(verbose, preprint, "Scanning all input files for storage info...")
    strlens = csv2h5_get_strlen(files)
    t_scan1 = verbprint(verbose, "ok!")
    t_scan = timeprint(verbose, t_scan0, t_scan1)
  }
  
  
  start_ind = 1
  t_read = 0
  t_write = 0
  for (file in files)
  {
    verbprint(verbose, preprint, file, "\n")
    # TODO batch process csv files?
    
    t_read0 = verbprint(verbose, preprint, preprint, "reading...")
    x = csv_reader(file, ...)
    t_read1 = verbprint(verbose, "ok!")
    t_read = t_read + timeprint(verbose, t_read0, t_read1)
    
    t_write0 = verbprint(verbose, preprint, preprint, "writing...")
    if (start_ind == 1)
      types = writer_init(x, h5_fp, dataset, strlens=strlens, compression=compression)
    
    writer(x, start_ind, h5_fp, dataset,types)
    t_write1 = verbprint(verbose, "ok!")
    t_write = t_write + timeprint(verbose, t_write0, t_write1)
    
    start_ind = start_ind + NROW(x)
  }
  
  t_header + t_scan + t_read + t_write
}



#' dir2h5
#' 
#' Convert a directory of csv files to HDF5 datasets.
#' 
#' @details
#' TODO
#' 
#' @param csvdir
#' Valid directory containing csv files
#' @param h5out
#' Output file.
#' @param dataset
#' Name of the data within the HDF5 file. If none is supplied, then this will be
#' inferred from the input file name.
#' @param recursive
#' TODO
#' @param combined
#' Should the csv files will be writen as a single HDF5 dataset?
#' @param header
#' TODO
#' @param format
#' One of \code{column} or \code{compound}.
#' @param compression
#' HDF5 compression level. An integer between 0 (least) to 9 (most).
#' @param yolo
#' Do you want to skip input file checks? Faster, but dangerous.
#' @param verbose
#' Option to enable 
#' @param ...
#' Additional arguments passed to \code{fread()}. Can not include \code{file},
#' \code{skip}, \code{nrows}, \code{verbose}, \code{showProgress}, or
#' \code{data.table}.
#' 
#' @return
#' Invisibly returns \code{TRUE}.
#' 
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
      if (substr(dataset, 1, 1) == ".")
        close_and_stop(h5_fp, paste0("invalid inferred dataset name: \"", dataset, "\"; please provide a valid argument to 'dataset'"))
      
      h5_check_dataset(h5_fp, dataset)
    }
  }
  
  files = dir(csvdir, pattern="*\\.csv$", full.names=TRUE, ignore.case=TRUE)
  if (length(files) == 0)
    close_and_stop(h5_fp, paste0("no csv files found in csvdir=", csvdir))
  
  if (isTRUE(combined))
    t_total = csv2h5_dir(files, h5_fp, dataset, format, yolo, verbose, compression, ...)
  else
  {
    t_total = 0
    verbprint(verbose, paste("Processing", length(files), "file(s):\n"))
    for (file in files)
    {
      verbprint(verbose, paste0("    ", file, "\n"))
      dataset = h5_infer_dataset(file)
      t_total = t_total + csv2h5_file(file, h5_fp, dataset, format, yolo, verbose, compression, ..., verbose_preprint="        ")
    }
  }
  
  verbprint(verbose, "Total time: ", timefmt(t_total), "s\n\n")
  
  h5close(h5_fp)
  invisible(TRUE)
}
