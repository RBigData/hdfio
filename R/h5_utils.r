close_and_stop = function(h5_fp, msg)
{
  h5close(h5_fp)
  stop(msg, call.=FALSE)
}



h5_is_string = function(h5_fp, dataset, varname)
{
  typename = as.character(h5_fp[[glue(dataset, varname)]]$get_type()$get_class())
  typename == "H5T_STRING"
}



h5_check_dataset = function(h5_fp, dataset)
{
  if (existsGroup(dataset))
    close_and_stop(h5_fp, "dataset already exists in h5 file")
  
  invisible()
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
  
  if (format == "hdfio_column")
  {
    f = h5_fp[[dataset]]
    nrows = f[[list.datasets(f)[1]]]$dims
    ncols = length(list.datasets(f))
  }
  else if (format == "pytables_table")
  {
    nrows = h5_fp[[glue(dataset, "table")]]$dims
    # FIXME this is really stupid...
    ncols = ncol(h5_fp[[glue(dataset, "table")]][1]) - 1L # we don't count the index since we drop it in the reader
  }
  else if (format == "pytables_fixed")
  {
    nrows = h5_fp[[glue(dataset, "axis1")]]$maxdims
    ncols = h5_fp[[glue(dataset, "axis0")]]$maxdims
  }
  else
    stop("unknown format")
  
  c(nrows, ncols)
}



h5_colnames = function(h5_fp, dataset)
{
  format = h5_detect_format(h5_fp, dataset, verbose=FALSE)
  
  if (format == "hdfio_column")
    h5attr(h5_fp[[dataset]], "VARNAMES")
  else if (format == "pytables_table")
  {
    cn = colnames(h5_fp[[glue(dataset, "table")]][1])
    cn[-grep("^index$", cn, perl=TRUE)] # don't include the index since we drop it in the reader
  }
  else if (format == "pytables_fixed")
    h5_fp[[glue(dataset, "axis0")]][]
  else
    stop("unknown format")
}



h5_list_datasets = function(h5_fp, dataset)
{
  datasets = list.groups(h5_fp)
  drop = grep("_i_table", datasets)
  if (length(drop) > 0)
    datasets[-drop]
  else
    datasets
}



h5_infer_dataset = function(x, dir=FALSE)
{
  if (isTRUE(dir))
    basename(file)
  else
    gsub("[.].*", "", basename(x))
}
