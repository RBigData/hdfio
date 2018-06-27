# -----------------------------------------------------------------------------
# hdfio readers
# -----------------------------------------------------------------------------

read_atomic_column = function(h5_fp, dataset, varname, rows)
{
  if (is.null(rows))
    h5_fp[[dataset]][[varname]][]
  else
    h5_fp[[dataset]][[varname]][rows]
}



read_factor_column = function(h5_fp, dataset, varname, rows)
{
  levels = h5attributes(h5_fp[[dataset]][[varname]])$LEVELS
  
  x = read_atomic_column(h5_fp, dataset, varname, rows)
  
  levels(x) = levels
  class(x) = "factor"
  x
}



read_h5df_column = function(h5_fp, dataset, rows)
{
  cols = list.datasets(h5_fp[[dataset]])
  x = vector(mode="list", length=length(cols))
  names(x) = h5attributes(h5_fp[[dataset]])$VARNAMES
  
  for (j in 1:length(cols))
  {
    nm = paste0("x", j)
    class = h5attributes(h5_fp[[dataset]][[nm]])$CLASS
    
    if (is.null(class))
      x[[j]] = read_atomic_column(h5_fp, dataset, nm, rows)
    else
      x[[j]] = read_factor_column(h5_fp, dataset, nm, rows)
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
  
  colnames = h5_fp[[dataset]][["axis0"]][]
  rownames = h5_fp[[dataset]][["axis1"]][]
  
  n = length(colnames)
  df = vector(mode="list", length=n)
  names(df) = colnames
  
  for (ind in 1:length(columns))
  {
    col_ind = columns[ind]
    n_block = h5_fp[[dataset]][[col_ind]]$dims[1]
    for (j_block in 1:n_block)
    {
      if (is.null(rows))
        col = h5_fp[[dataset]][[col_ind]][j_block, ]
      else
        col = h5_fp[[dataset]][[col_ind]][j_block, rows]
      
      df_j = h5_fp[[dataset]][[items[ind]]][j_block]
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
    df = h5_fp[[dataset]][["table"]][]
  else
    df = h5_fp[[dataset]][["table"]][rows]
  
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
#' @param verbose
#' TODO
#' 
#' @return
#' A dataframe.
#' 
#' @examples
#' library(hdfio)
#' 
#' h5in = system.file("exampledata/pandas_table.h5", package="hdfio")
#' df = read_h5df(h5in, "mydata")
#' df
#' 
#' @seealso
#' \code{\link{write_h5df}}
#' 
#' @export
read_h5df = function(h5in, dataset=NULL, rows=NULL, verbose=FALSE)
{
  check.is.string(h5in)
  check.is.flag(verbose)
  if (!is.null(dataset))
    check.is.string(dataset)
  if (!is.null(rows))
  {
    if (length(rows) == 0 || any(rows < 1) || !all(is.inty(rows)))
      stop("argument 'rows' must be a vector of positive integers")
  }
  
  h5_fp = h5file(h5in, mode="r")
  dataset = h5_get_dataset(h5_fp, dataset)
  fmt = h5_detect_format(h5_fp, dataset, verbose)
  
  if (fmt == "hdfio_column")
    df = read_h5df_column(h5_fp, dataset, rows)
  else if (fmt == "pytables_table")
    df = read_pytables_table(h5_fp, dataset, rows)
  else if (fmt == "pytables_fixed")
    df = read_pytables_fixed(h5_fp, dataset, rows)
  else
    close_and_stop(h5_fp, "unknown format")
  
  h5close(h5_fp)
  
  df
}
