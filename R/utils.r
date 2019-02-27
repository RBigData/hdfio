glue = function(a, b)
{
  paste0(a, "/", b)
}



verbprint = function(verbose, msg)
{
  if (isTRUE(verbose))
    cat(msg)
  
  invisible()
}



csv_reader = function(file, skip="__auto__", nrows=Inf, ...)
{
  data.table::fread(file=file, skip=skip, nrows=nrows, verbose=FALSE, showProgress=FALSE, data.table=FALSE, ...)
}



ndigits = function(n)
{
  if (n==0)
    1L
  else
    as.integer(log10(n) + 1.0)
}



progress_printer = function(i, n, verbose, preprint="")
{
  if (!isTRUE(verbose))
    return(invisible())
  
  
  nd = ndigits(n)
  trailing = "..."
  
  if (i == 0)
    cat(paste0(preprint, "Processing batch "), paste0(rep(" ", nd), collapse=""))
  else
  {
    nd_i = ndigits(i) + nchar(trailing)
    for (j in 1:(nd_i + nd + 1L))
      cat("\b")
    
    cat(i)
  }
  
  cat(paste0("/", n, trailing))
}



check.file = function(file, h5=FALSE)
{
  if (!file.exists(file))
    stop("file does not exist")
  
  if (isTRUE(h5) && !is.h5file(file))
    stop("file is not a valid HDF5 file")
  
  invisible(TRUE)
}
