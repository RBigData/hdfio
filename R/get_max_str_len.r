get_max_str_len = function(x)
{
  if (is.factor(x))
  {
    maxlen = .Call(R_get_max_str_len, levels(x))
    if (anyNA(levels(x)))
      maxlen = max(2L, maxlen)
    
    maxlen
  }
  else if (typeof(x) == "character")
    .Call(R_get_max_str_len, x)
  else
    return(-1L)
}
