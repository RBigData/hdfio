library(hdfio)

f = system.file("exampledata/pandas_table.h5", package="hdfio")
test = is_h5df(f, "mydata")
stopifnot(identical(test, TRUE))

test = is_h5df(f, "/")
stopifnot(identical(test, FALSE))

g = system.file("exampledata/not_h5df.h5", package="hdfio")
test = is_h5df(g, "mydata")
stopifnot(identical(test, FALSE))
