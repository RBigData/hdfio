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
  data.table::fread(file=file, skip=skip, nrows=nrows, stringsAsFactors=stringsAsFactors, verbose=FALSE, data.table=FALSE, ...)
}
