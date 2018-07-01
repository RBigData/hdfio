csv2h5_validation_dir = function(files, h5_fp, dataset, format, stringsAsFactors)
{
  # Validate colnames across csv files
  colnames = csv_colnames(files[1])
  for (file in 2:files[-1])
  {
    colnames2 = csv_colnames(file)
    if (!identical(colnames, colnames2))
      close_and_stop(h5_fp, "column names are not the same across files; are they actually mergeable? Re-run with yolo=TRUE to ignore")
  }
  
  
  # TODO string size
}



csv2h5_validation_file = function()
{
  # TODO string size
}



# -----------------------------------------------------------------------------
# hdfio_column format
# -----------------------------------------------------------------------------

csv2h5_column_dir = function(files, h5_fp, dataset, format, stringsAsFactors, yolo)
{
  if (!isTRUE(yolo))
    csv2h5_validation_dir(files, h5_fp, dataset, format, stringsAsFactors)
  
  if (format == "column")
    writer = write_h5df_column
  
  start_ind = 1
  for (file in files)
  {
    x = data.table::fread(file, stringsAsFactors=stringsAsFactors)
    writer(x, start_ind, h5_fp, dataset)
    
    start_ind = start_ind + NROW(x)
  }
}



csv2h5_column_file = function(file, h5_fp, dataset, format, stringsAsFactors, yolo)
{
  # TODO csv2h5_validation_file
  
  if (format == "column")
    writer = write_h5df_column
  
  nrows = csv_nrows(file)
  num_chunks = chunker_numchunks(file)
  indices = chunker_indices(nrows, num_chunks)
  
  for (i in 1:length(indices))
  {
    skip = indices[[i]][1]
    end = indices[[i]][2]
    
    nr = end - skip + 1
    
    if (skip == 1)
      x = data.table::fread(file, skip=skip-1, nrows=nr, stringsAsFactors=stringsAsFactors)
    else
      x = data.table::fread(file, skip=skip, nrows=nr, stringsAsFactors=stringsAsFactors)
    
    writer(x, skip, h5_fp, dataset)
  }
}



# -----------------------------------------------------------------------------
# interface
# -----------------------------------------------------------------------------

#' csv2h5
#' 
#' Convert a csv to HDF5 dataset.
#' 
#' @details
#' TODO
#' 
#' @param csvfile
#' Input file.
#' @param csvdir
#' TODO
#' @param h5file
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
#' 
#' @return
#' Invisibly returns \code{TRUE} on success.
#' 
#' @export
csv2h5 = function(csvfile, csvdir=NULL, h5out, dataset, format="column", compression=0, stringsAsFactors=FALSE, yolo=FALSE)
{
  if (!is.null(csvdir))
    check.is.string(csvdir)
  else
    check.is.string(csvfile)
  
  check.is.string(h5out)
  format = match.arg(tolower(format), c("column")) # TODO compound
  check.is.natnum(compression)
  compression = as.integer(compression)
  if (compression > 9 || compression < 0)
    stop("argument 'compression' must be an integer in the range 0 to 9")
  check.is.flag(stringsAsFactors)
  check.is.flag(yolo)
  
  h5_fp = h5file(h5out, mode="a")
  
  if (!is.null(csvdir))
  {
    files = dir(csvdir, pattern="*.csv", full.names=TRUE)
    if (length(files) == 0)
      close_and_stop(h5_fp, paste0("no csv files found in csvdir=", csvdir))
    
    csv2h5_column_dir(files, h5_fp, dataset, format, stringsAsFactors, yolo)
  }
  else
    csv2h5_column_file(csvfile, h5_fp, dataset, format, stringsAsFactors, yolo)
  
  h5close(h5_fp)
}
