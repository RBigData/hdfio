library(hdfio)

f = system.file("exampledata/pandas_table.h5", package="hdfio")
df = read_h5df(f, "mydata")
stopifnot(identical(dim(df), c(5L, 3L)))

err = tryCatch(read_h5df(f, "unknown format"), error=identity)
stopifnot(inherits(err, "simpleError"))


g = system.file("exampledata/not_h5df.h5", package="hdfio")
err = tryCatch(read_h5df(g, "mydata"), error=identity)$message
truth = "unknown format"
stopifnot(identical(err, truth))
