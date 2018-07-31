csv2h5_get_strlen = function(files, lens=integer(ncols) - 1L, ...)
{
  for (file in files)
  {
    nrows = csv_nrows(file)
    num_chunks = chunker_numchunks(file)
    indices = chunker_indices(nrows, num_chunks)
    
    ncols = csv_ncols(file)
    
    for (i in 1:length(indices))
    {
      skip = indices[[i]][1]
      end = indices[[i]][2]
      
      if (length(indices) == 1L)
        nr = Inf # needed for multithreaded fread
      else
        nr = end - skip + 1
      
      if (skip == 1)
        x = csv_reader(file, skip=skip-1, nrows=nr, ...)
      else
        x = csv_reader(file, skip=skip, nrows=nr, ...)
      
      lens = pmax(lens, sapply(x, get_max_str_len))
    }
  }
  
  lens
}



csv2h5_file = function(file, h5_fp, dataset, format, yolo, verbose, compression, ...)
{
  if (format == "column")
  {
    writer = write_h5df_column
    writer_init = write_h5df_column_init
  }
  
  else if (format == "compound") {
    writer = write_h5df_compound
    writer_init = write_h5df_compound_init
  }
  
  nrows = csv_nrows(file)
  num_chunks = chunker_numchunks(file)
  indices = chunker_indices(nrows, num_chunks)
  
  if (isTRUE(yolo))
  {
    verbprint(verbose, "Living dangerously...\n")
    strlens = NULL
  }
  else
  {
    verbprint(verbose, "Scanning all input files for storage info...")
    strlens = csv2h5_get_strlen(file)
    verbprint(verbose, "ok!\n")
  }
  
  
  n = length(indices)
  progress_printer(0, n, verbose)
  for (i in 1:n)
  {
    progress_printer(i, n, verbose)
    
    start_ind = indices[[i]][1]
    end = indices[[i]][2]
    
    nr = end - start_ind + 1
    
    if (start_ind == 1)
      x = csv_reader(file, skip=start_ind-1, nrows=nr, ...)
    else
      x = csv_reader(file, skip=start_ind, nrows=nr, ...)
    
    if (start_ind == 1)
      types = writer_init(x, h5_fp, dataset, strlens=strlens, compression=compression)
      
      writer(x, start_ind, h5_fp, dataset,types)
  }
  
  verbprint(verbose, "done!\n")
  
  invisible(TRUE)
}



#' csv2h5
#' 
#' Convert a csv file to HDF5 dataset.
#' 
#' @details
#' TODO
#' @param file
#' Input file.
#' @param h5out
#' Output file.
#' @param dataset
#' Dataset in input file to read or \code{NULL}. In the latter case (e.g. \code{NULL}), the dataset will be contained 
#' within a group named as the input dataset.
#' @param format
#' Method chosen for writing out h5 file.  If \code{column}, each column of the input dataset is written 
#' out on disk as x_i with "i" being an arbitrary column index, ranging as intengers from 1:ncol(dataframe). If \code{compound}, 
#' the entire input dataset is written out on disk as a complete dataframe. Default is \code{column}.
#' @param compression
#' HDF5 compression level. An integer, 0 (least compression) to 9 (most
#' compression).
#' @param yolo
#' Logical. Information on \code{R} processes are shown for HDF5 processes. Default is \code{FALSE}.
#' @param verbose
#' TODO
#' @param ...
#' Additional arguments passed to \code{fread()}. Can not include \code{file},
#' \code{skip}, \code{nrows}, \code{verbose}, \code{showProgress}, or
#' \code{data.table}.
#' 
#' @examples
#' \dontrun{ #TODO FIXME
#' #Write df to csv in temp directory 
#' library(hdfio)
#' df = data.frame(x=seq(1:5), y = c(runif(5)), z= c("Peter", "Amber", "John", "Lindsey", "Steven")) 
#' utils::write.csv(x = df, file = paste(tempdir(),"df.csv",sep="/"), row.names = FALSE) 
#' 
#' #Read in single csv file (column type)
#' csv2h5(paste(tempdir(),"df.csv",sep="/"), h5out = paste(tempdir(),"result_col.h5",sep="/"), dataset=NULL, format = "column", compression=4)  
#' result_col <- hdf5r::h5file(paste(tempdir(), "result_col.h5",sep = "/"))
#' result_col$ls(recursive=TRUE)
#' 
#' #Read in single csv file (compound type)
#' csv2h5(paste(tempdir(),"df.csv",sep="/"), h5out = paste(tempdir(),"result_comp.h5",sep="/"), dataset=NULL, format = "compound", compression=4) 
#' result_comp <- hdf5r::h5file(paste(tempdir(), "result_comp.h5",sep = "/"))
#' result_comp$ls(recursive=TRUE)
#' }
#' 
#' @rdname csv2h5
#' @export
csv2h5 = function(file, h5out, dataset=NULL, header="auto", format="column", compression=4, yolo=FALSE, verbose=FALSE, ...)
{
  check.is.string(file)
  check.file(file)
  check.is.string(h5out)
  if (!is.null(dataset))
    check.is.string(dataset)
  else
    dataset = h5_infer_dataset(file)
  if (is.character(header))
    header = match.arg(tolower(header), "auto")
  else
    check.is.flag(header)
  format = match.arg(tolower(format), c("column", "compound")) 
  check.is.natnum(compression)
  if (compression > 9 || compression < 0)
    stop("argument 'compression' must be an integer in the range 0 to 9")
  check.is.flag(yolo)
  check.is.flag(verbose)
  
  h5_fp = h5file(h5out, mode="a")
  h5_check_dataset(h5_fp, dataset)
  
  csv2h5_file(file, h5_fp, dataset, format, yolo, verbose, compression, ...)
  
  h5close(h5_fp)
}
