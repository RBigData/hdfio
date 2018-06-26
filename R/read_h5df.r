# -----------------------------------------------------------------------------
# pytables readers
# -----------------------------------------------------------------------------

# pandas.HDFStore.put(format="fixed")
read_pytables_fixed = function(h5_fp, dataset, rows)
{
  columns = list.datasets(h5_fp[[dataset]])
  columns = columns[grep("block", columns)]
  
  if (any(grepl("block2", columns)))
    close_and_stop("file has string data written in 'fixed' format, which can not be portably read. Please re-write from pandas/pytables with format='table'")
  
  colnames = h5_fp[[dataset]][["axis0"]][]
  rownames = h5_fp[[dataset]][["axis1"]][]
  
  df = vector(mode="list", length=n)
  names(df) = colnames
  
  for (i in seq(2, length(columns), by=2))
  {
    item = columns[i]
    
    if (is.null(rows))
      tmp = h5_fp[[dataset]][[item]][1, ]
    else
      tmp = h5_fp[[dataset]][[item]][1, rows]
    
    df[h5_fp[[dataset]][columns[i-1]]][1]
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
#' 
#' @return
#' A dataframe.
#' 
#' @examples
#' library(hdfio)
#' 
#' h5in = system.file("exampledata/from_pandas.h5", package="hdfio")
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
  
  if (fmt == "pytables_table")
    df = read_pytables_table(h5_fp, dataset, rows)
  else if (fmt == "pytables_fixed")
    df = read_pytables_fixed(h5_fp, dataset, rows)
  else
    close_and_stop(h5_fp, "unknown format")
  
  h5close(h5_fp)
  
  df
}
