#' @useDynLib hdfio R_get_max_str_len
get_max_str_len_wrapper = function(x)
{
  .Call(R_get_max_str_len, x)
}



get_max_str_len = function(x)
{
  if (is.factor(x))
  {
    maxlen = get_max_str_len_wrapper(levels(x))
    if (anyNA(levels(x)))
      maxlen = max(2L, maxlen)
    
    maxlen
  }
  else if (typeof(x) == "character")
    get_max_str_len_wrapper(x)
  else
    -1L
}
