close_and_stop = function(h5_fp, msg)
{
  h5close(h5_fp)
  stop(msg, call.=FALSE)
}



h5_detect_format = function(h5_fp, dataset, verbose=FALSE)
{
  if (isTRUE(verbose))
    cat("detecting format...")
  
  attrs = h5attributes(h5_fp)
  
  if (!is.null(attrs$TABLE_FORMAT))
    fmt = attrs$TABLE_FORMAT
  else if (!is.null(attrs$PYTABLES_FORMAT_VERSION))
  {
    attrs_table = h5attributes(h5_fp[[dataset]])
    
    if (isTRUE(attrs_table$pandas_type == "frame_table"))
      fmt = "pytables_table"
    else if (isTRUE(attrs_table$pandas_type == "frame"))
      fmt = "pytables_fixed"
    else
      fmt = "unknown"
  }
  else
    fmt = "unknown"
  
  if (isTRUE(verbose))
    cat(fmt, "\n")
  
  fmt
}



h5_get_dataset = function(h5_fp, dataset)
{
  if (is.null(dataset))
  {
    datasets = names(h5_fp)
    if (length(datasets) != 1)
    {
      h5close(h5_fp)
      stop("multiple datasets available")
    }
    else
      dataset = datasets[1]
  }
  
  dataset
}



h5_dim = function(h5_fp, dataset)
{
  format = h5_detect_format(h5_fp, dataset, verbose=FALSE)
  
  if (format == "hdfio")
  {
    # TODO
  }
  else if (format == "pandas_table")
  {
    nrows = h5_fp[[dataset]][["table"]]$dims
    # FIXME this is really stupid...
    ncols = ncol(h5_fp[[dataset]][["table"]][1]) - 1L # we don't count the index since we drop it in the reader
  }
  else if (format == "pandas_fixed")
  {
    nrows = h5_fp[[dataset]][["axis1"]]$maxdims
    ncols = h5_fp[[dataset]][["axis0"]]$maxdims
  }
  else
  {
    # TODO
  }
  
  c(nrows, ncols)
}



h5_colnames = function(h5_fp, dataset)
{
  format = h5_detect_format(h5_fp, dataset, verbose=FALSE)
  
  if (format == "hdfio")
  {
    # TODO
  }
  else if (format == "pandas_table")
  {
    cn = colnames(h5_fp[[dataset]][["table"]][1])
    cn[-grep("^index$", cn, perl=TRUE)] # don't include the index since we drop it in the reader
  }
  else if (format == "pandas_fixed")
    h5_fp[["mydata"]][["axis0"]][]
  else
  {
    # TODO
  }
}
