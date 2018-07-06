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



csv2h5_get_strlen = function(files, lens=integer(ncols) - 1L)
{
  for (file in files)
  {
    nrows = csv_nrows(file)
    num_chunks = chunker_numchunks(file)
    indices = chunker_indices(nrows, num_chunks)
    
    ncols = csv_ncols(file)
    
    for (i in 1:length(indices))
    {
      skip = indices[[i]][1]
      end = indices[[i]][2]
      
      if (length(indices) == 1L)
        nr = Inf # needed for multithreaded fread
      else
        nr = end - skip + 1
      
      if (skip == 1)
        x = csv_reader(file, skip=skip-1, nrows=nr, stringsAsFactors=FALSE)
      else
        x = csv_reader(file, skip=skip, nrows=nr, stringsAsFactors=FALSE)
      
      lens = pmax(lens, sapply(x, get_max_str_len))
    }
  }
  
  lens
}



# -----------------------------------------------------------------------------
# writers
# -----------------------------------------------------------------------------

csv2h5_dir = function(files, h5_fp, dataset, format, stringsAsFactors, yolo, verbose, compression)
{
  if (format == "column")
  {
    writer = write_h5df_column
    writer_init = write_h5df_column_init
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
    
    x = csv_reader(file, stringsAsFactors=stringsAsFactors)
    
    if (start_ind == 1)
      types = writer_init(x, h5_fp, dataset, strlens=strlens, compression=compression)
    
    verbprint(verbose, "ok! writing...")
    writer(x, start_ind, h5_fp, dataset, types)
    verbprint(verbose, "ok!\n")
    
    start_ind = start_ind + NROW(x)
  }
  
  verbprint(verbose, "done!\n")
  
  invisible(TRUE)
}



csv2h5_file = function(file, h5_fp, dataset, format, stringsAsFactors, yolo, verbose, compression)
{
  if (format == "column")
  {
    writer = write_h5df_column
    writer_init = write_h5df_column_init
  }
  
  nrows = csv_nrows(file)
  num_chunks = chunker_numchunks(file)
  indices = chunker_indices(nrows, num_chunks)
  
  if (isTRUE(yolo))
    strlens = NULL
  else
    strlens = csv2h5_get_strlen(file)
  
  n = length(indices)
  progress_printer(0, n, verbose)
  for (i in 1:n)
  {
    progress_printer(i, n, verbose)
    
    start_ind = indices[[i]][1]
    end = indices[[i]][2]
    
    nr = end - start_ind + 1
    
    if (start_ind == 1)
      x = csv_reader(file, skip=start_ind-1, nrows=nr, stringsAsFactors=stringsAsFactors)
    else
      x = csv_reader(file, skip=start_ind, nrows=nr, stringsAsFactors=stringsAsFactors)
    
    if (start_ind == 1)
      types = writer_init(x, h5_fp, dataset, strlens=strlens, compression=compression)
    
    writer(x, start_ind, h5_fp, dataset, types)
  }
}



# -----------------------------------------------------------------------------
# interface
# -----------------------------------------------------------------------------

#' csv2h5
#' 
#' Convert a csv file or a directory of csv files to HDF5 dataset.
#' 
#' @details
#' TODO
#' 
#' @param file
#' Input file.
#' @param csvdir
#' TODO
#' @param h5out
#' Output file.
#' @param dataset
#' TODO
#' @param format
#' TODO
#' @param compression
#' HDF5 compression level. An integer, 0 (least compression) to 9 (most
#' compression).
#' @param stringsAsFactors
#' TODO
#' @param yolo
#' TODO
#' @param verbose
#' TODO
#' @param combined
#' TODO
#' 
#' @return
#' Invisibly returns \code{TRUE} on success.
#' 
#' @name csv2h5
#' @rdname csv2h5
NULL



#' @rdname csv2h5
#' @export
csv2h5 = function(file, h5out, dataset=NULL, format="column", compression=0, stringsAsFactors=FALSE, yolo=FALSE, verbose=FALSE)
{
  check.is.string(file)
  check.file(file)
  check.is.string(h5out)
  if (!is.null(dataset))
    check.is.string(dataset)
  else
    dataset = h5_infer_dataset(file)
  format = match.arg(tolower(format), c("column")) # TODO compound
  check.is.natnum(compression)
  if (compression > 9 || compression < 0)
    stop("argument 'compression' must be an integer in the range 0 to 9")
  check.is.flag(stringsAsFactors)
  check.is.flag(yolo)
  check.is.flag(verbose)
  
  h5_fp = h5file(h5out, mode="a")
  h5_check_dataset(h5_fp, dataset)
  
  csv2h5_file(file, h5_fp, dataset, format, stringsAsFactors, yolo, verbose, compression)
  
  h5close(h5_fp)
}



#' @rdname csv2h5
#' @export
dir2h5 = function(csvdir, h5out, dataset=NULL, combined=TRUE, format="column", compression=0, stringsAsFactors=FALSE, yolo=FALSE, verbose=FALSE)
{
  check.is.string(csvdir)
  check.is.string(h5out)
  if (!is.null(dataset))
    check.is.string(dataset)
  check.is.flag(combined)
  format = match.arg(tolower(format), c("column")) # TODO compound
  check.is.natnum(compression)
  if (compression > 9 || compression < 0)
    stop("argument 'compression' must be an integer in the range 0 to 9")
  check.is.flag(stringsAsFactors)
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
  
  files = dir(csvdir, pattern="*.csv", full.names=TRUE, ignore.case=TRUE)
  if (length(files) == 0)
    close_and_stop(h5_fp, paste0("no csv files found in csvdir=", csvdir))
  
  if (isTRUE(combined))
    csv2h5_dir(files, h5_fp, dataset, format, stringsAsFactors, yolo, verbose, compression)
  else
  {
    verbprint(verbose, paste("Processing", length(files), "files:\n"))
    for (file in files)
    {
      verbprint(verbose, paste0("    ", file, " "))
      dataset = h5_infer_dataset(file)
      csv2h5_file(file, h5_fp, dataset, format, stringsAsFactors, yolo, verbose, compression)
    }
  }
  
  
  h5close(h5_fp)
}
