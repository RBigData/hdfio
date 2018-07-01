# -----------------------------------------------------------------------------
# hdfio readers
# -----------------------------------------------------------------------------

read_atomic_column = function(h5_fp, dataset, varname, rows)
{
  ds = glue(dataset, varname)
  
  if (is.null(rows))
    h5_fp[[ds]][]
  else
    h5_fp[[ds]][rows]
}



read_factor_column = function(h5_fp, dataset, varname, rows)
{
  ds = glue(dataset, varname)
  levels = h5attributes(h5_fp[[ds]])$LEVELS
  
  x = read_atomic_column(h5_fp, dataset, varname, rows)
  
  levels(x) = levels
  class(x) = "factor"
  x
}



read_h5df_column = function(h5_fp, dataset, rows, cols, strings)
{
  if (is.null(cols))
  {
    if (strings == FALSE)
    {
      datasets = list.datasets(h5_fp[[dataset]])
      is_string = sapply(datasets, function(ds) h5_is_string(h5_fp, dataset, ds))
      cols = which(!is_string)
      colnames = h5attributes(h5_fp[[dataset]])$VARNAMES[cols]
    }
    else
    {
      colnames = h5attributes(h5_fp[[dataset]])$VARNAMES
      cols = 1:length(colnames)
    }
  }
  else
  {
    colnames = h5attributes(h5_fp[[dataset]])$VARNAMES
    if (max(cols) > length(colnames))
      close_and_stop(h5_fp, "some 'cols' indices larger than the number of columns in the dataset")
    
    colnames = colnames[cols]
  }
  
  
  x = vector(mode="list", length=length(cols))
  names(x) = colnames
  
  for (j in 1:length(cols))
  {
    nm = paste0("x", cols[j])
    class = h5attributes(h5_fp[[glue(dataset, nm)]])$CLASS
    
    if (length(class) == 0)
      x[[j]] = read_atomic_column(h5_fp, dataset, nm, rows)
    else if (class == "logical")
    {
      x[[j]] = read_atomic_column(h5_fp, dataset, nm, rows)
      class(x[[j]]) = "logical"
    }
    else if (class == "factor")
      x[[j]] = read_factor_column(h5_fp, dataset, nm, rows)
    else
      close_and_stop(h5_fp, INTERNAL_ERROR)
  }
  
  data.table::setDF(x)
  x
}



# -----------------------------------------------------------------------------
# pytables readers
# -----------------------------------------------------------------------------

# pandas.HDFStore.put(format="fixed")
read_pytables_fixed = function(h5_fp, dataset, rows)
{
  columns = list.datasets(h5_fp[[dataset]])
  items = columns[grep("items", columns)]
  columns = columns[grep("values", columns)]
  
  if (any(grepl("block2", columns)))
    close_and_stop(h5_fp, "file has string data written from pandas/pytables in 'fixed' format, which can not be portably read. Please re-write with format='table'")
  
  colnames = h5_fp[[glue(dataset, "axis0")]][]
  rownames = h5_fp[[glue(dataset, "axis1")]][]
  
  n = length(colnames)
  df = vector(mode="list", length=n)
  names(df) = colnames
  
  for (ind in 1:length(columns))
  {
    col_ind = columns[ind]
    ds = glue(dataset, col_ind)
    n_block = h5_fp[[ds]]$dims[1]
    
    for (j_block in 1:n_block)
    {
      if (is.null(rows))
        col = h5_fp[[ds]][j_block, ]
      else
        col = h5_fp[[ds]][j_block, rows]
      
      df_j = h5_fp[[glue(dataset, items[ind])]][j_block]
      df[[df_j]] = col
    }
  }
  
  data.table::setDF(df, rownames=rownames) 
  df
}



# pandas.HDFStore.put(format="table")
read_pytables_table = function(h5_fp, dataset, rows)
{
  if (is.null(rows))
    df = h5_fp[[glue(dataset, "table")]][]
  else
    df = h5_fp[[glue(dataset, "table")]][rows]
  
  df$index = NULL
  
  df
}



# -----------------------------------------------------------------------------
# interface
# -----------------------------------------------------------------------------

#' read_h5df
#' 
#' TODO
#' 
#' @param h5in
#' Input file.
#' @param dataset
#' Dataset in input file to read or \code{NULL}. In the latter case, TODO
#' @param rows
#' TODO
#' @param cols
#' TODO
#' @param strings
#' Only available for 'hdfio_columns' format files. Should string columns be read?
#' @param verbose
#' TODO
#' 
#' @return
#' A dataframe.
#' 
#' @examples
#' library(hdfio)
#' 
#' h5in = system.file("exampledata/pytables_table.h5", package="hdfio")
#' df = read_h5df(h5in, "mydata")
#' df
#' 
#' @seealso
#' \code{\link{write_h5df}}
#' 
#' @export
read_h5df = function(h5in, dataset=NULL, rows=NULL, cols=NULL, strings=TRUE, verbose=FALSE)
{
  check.is.string(h5in)
  check.is.flag(strings)
  check.is.flag(verbose)
  if (!is.null(dataset))
    check.is.string(dataset)
  if (!is.null(rows))
  {
    if (length(rows) == 0 || !all(is.inty(rows)) || any(rows < 1))
      stop("argument 'rows' must be a vector of positive integers")
  }
  if (!is.null(cols))
  {
    if (length(cols) == 0 || !all(is.inty(cols)) || any(cols < 1))
      stop("argument 'cols' must be a vector of positive integers")
  }
  
  
  h5_fp = h5file(h5in, mode="r")
  dataset = h5_get_dataset(h5_fp, dataset)
  fmt = h5_detect_format(h5_fp, dataset, verbose)
  
  if (!is.null(cols) && fmt != "hdfio_column")
    close_and_stop(h5_fp, "argument 'cols' can only be a vector of indices if format is hdfio_column")
  if (!isTRUE(strings) && fmt != "hdfio_column")
    close_and_stop(h5_fp, "argument 'strings' must be TRUE if format is not hdfio_column")
  if (!is.null(cols) && !isTRUE(strings))
    close_and_stop(h5_fp, "must have 'strings=TRUE' when columns are specified via 'cols'")
  
  if (fmt == "hdfio_column")
    df = read_h5df_column(h5_fp, dataset, rows, cols, strings)
  else if (fmt == "pytables_table")
    df = read_pytables_table(h5_fp, dataset, rows)
  else if (fmt == "pytables_fixed")
    df = read_pytables_fixed(h5_fp, dataset, rows)
  else
    close_and_stop(h5_fp, "unknown format")
  
  h5close(h5_fp)
  
  df
}
