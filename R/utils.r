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
