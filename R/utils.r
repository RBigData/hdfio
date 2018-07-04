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



csv_reader = function(file, skip="__auto__", nrows=Inf, stringsAsFactors=FALSE, ...)
{
  data.table::fread(file=file, skip=skip, nrows=nrows, stringsAsFactors=stringsAsFactors, verbose=FALSE, showProgress=FALSE, data.table=FALSE, ...)
}



ndigits = function(n)
{
  if (n==0)
    1L
  else
    as.integer(log10(n) + 1.0)
}



progress_printer = function(i, n, verbose)
{
  if (!isTRUE(verbose))
    return(invisible())
  
  
  nd = ndigits(n)
  
  if (i == 0)
  {
    cat("Processing batch ")
    cat(paste0(rep(" ", nd), collapse=""))
  }
  else
  {
    nd_i = ndigits(i)
    for (j in 1:(nd_i + nd + 1L))
      cat("\b")
    
    cat(i)
  }
  
  cat("/")
  cat(n)
  
  if (i == n)
    cat("\n")
}
