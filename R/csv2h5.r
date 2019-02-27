csv2h5_get_strlen = function(files, lens=integer(ncols) - 1L, ...)
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
        x = csv_reader(file, skip=skip-1, nrows=nr, ...)
      else
        x = csv_reader(file, skip=skip, nrows=nr, ...)
      
      lens = pmax(lens, sapply(x, get_max_str_len))
    }
  }
  
  lens
}



csv2h5_file = function(file, h5_fp, dataset, format, yolo, verbose, compression, ..., verbose_preprint="")
{
  if (format == "column")
  {
    writer = write_h5df_column
    writer_init = write_h5df_column_init
  }
  
  else if (format == "compound") {
    writer = write_h5df_compound
    writer_init = write_h5df_compound_init
  }
  
  nrows = csv_nrows(file)
  num_chunks = chunker_numchunks(file)
  indices = chunker_indices(nrows, num_chunks)
  
  if (isTRUE(yolo))
  {
    verbprint(verbose, verbose_preprint, "Living dangerously...")
    strlens = NULL
    t_scan = timeprint(verbose)
  }
  else
  {
    t_scan0 = verbprint(verbose, verbose_preprint, "Detecting column storage...")
    strlens = csv2h5_get_strlen(file)
    t_scan1 = verbprint(verbose, "ok!")
    t_scan = timeprint(verbose, t_scan0, t_scan1)
  }
  
  
  n = length(indices)
  progress_printer(0, n, verbose, verbose_preprint)
  t_conv0 = proc.time()
  for (i in 1:n)
  {
    progress_printer(i, n, verbose, verbose_preprint)
    
    start_ind = indices[[i]][1]
    end = indices[[i]][2]
    
    nr = end - start_ind + 1
    
    if (start_ind == 1)
      x = csv_reader(file, skip=start_ind-1, nrows=nr, ...)
    else
      x = csv_reader(file, skip=start_ind, nrows=nr, ...)
    
    if (start_ind == 1)
      types = writer_init(x, h5_fp, dataset, strlens=strlens, compression=compression)
      
      writer(x, start_ind, h5_fp, dataset,types)
  }
  
  t_conv1 = proc.time()
  verbprint(verbose, "ok!")
  t_conv = timeprint(verbose, t_conv0, t_conv1)
  
  t_scan + t_conv
}



#' csv2h5
#' 
#' Convert a csv file to HDF5 dataset.
#' 
#' @details
#' TODO
#' 
#' @param file
#' Input file.
#' @param h5out
#' Output file.
#' @param dataset
#' Name of the data within the HDF5 file. If none is supplied, then this will be
#' inferred from the input file name.
#' @param header
#' TODO
#' @param format
#' One of \code{column} or \code{compound}.
#' @param compression
#' HDF5 compression level. An integer between 0 (least) to 9 (most).
#' @param yolo
#' Do you want to skip input file checks? Faster, but dangerous.
#' @param verbose
#' TODO
#' @param ...
#' Additional arguments passed to \code{fread()}. Can not include \code{file},
#' \code{skip}, \code{nrows}, \code{verbose}, \code{showProgress}, or
#' \code{data.table}.
#' 
#' @return
#' Invisibly returns \code{TRUE}.
#' 
#' @export
csv2h5 = function(file, h5out, dataset=NULL, header="auto", format="column", compression=4, yolo=FALSE, verbose=FALSE, ...)
{
  check.is.string(file)
  check.file(file)
  check.is.string(h5out)
  if (!is.null(dataset))
    check.is.string(dataset)
  else
    dataset = h5_infer_dataset(file)
  if (is.character(header))
    header = match.arg(tolower(header), "auto")
  else
    check.is.flag(header)
  format = match.arg(tolower(format), c("column", "compound")) 
  check.is.natnum(compression)
  if (compression > 9 || compression < 0)
    stop("argument 'compression' must be an integer in the range 0 to 9")
  check.is.flag(yolo)
  check.is.flag(verbose)
  
  
  h5_fp = h5file(h5out, mode="a")
  h5_check_dataset(h5_fp, dataset)
  
  verbprint(verbose, paste("Processing 1 file(s):\n"))
  verbprint(verbose, paste0("    ", file, "\n"))
  t_total = csv2h5_file(file, h5_fp, dataset, format, yolo, verbose, compression, ..., verbose_preprint="        ")
  verbprint(verbose, "Total time: ", timefmt(t_total), "s\n\n")
  
  h5close(h5_fp)
  invisible(TRUE)
}
