chunker = function(rows_total, rows_chunk)
{
  if (rows_total %% rows_chunk == 0)
    return(rep(rows_chunk, rows_total/rows_chunk))
  
  num = as.integer(rows_total/rows_chunk)
  
  c(
    rep(rows_chunk, num),
    rows_total - num*rows_chunk
  )
}



# split 'rows_total' rows into 'num_chunks' chunks; gives start/end indices
chunker_indices = function(rows_total, num_chunks)
{
  rows_chunk = floor(rows_total / num_chunks)
  chunklen = chunker(rows_total=rows_total, rows_chunk=rows_chunk)
  num_chunks = length(chunklen)
  
  start = 0:(num_chunks - 1L) * chunklen[1] + 1
  end = chunklen + start - 1
  
  lapply(1:num_chunks, function(i) c(start[i], end[i]))
}



chunker_numchunks = function(file)
{
  invisible(gc(verbose=FALSE, reset=TRUE))
  
  check = tryCatch(ram <- memuse::Sys.meminfo()$freeram, error=identity)
  if (inherits(check, "simpleError"))
    ram = memuse::mu(1, "gib")
  else
    ram = .65*ram
  
  # ram = memuse::mu(10, "mib") # for testing
  
  size = memuse::Sys.filesize(file)
  ceiling(as.numeric(size/ram))
}
