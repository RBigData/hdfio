library(hdfio)

f = system.file("exampledata/from_pandas.h5", package="hdfio")
df = read_h5df(f, "mydata")
stopifnot(identical(dim(df), c(5L, 3L)))

err = tryCatch(read_h5df(f, "asdf"), error=identity)
inherits(err, "simpleError")


g = system.file("exampledata/not_h5df.h5", package="hdfio")
err = tryCatch(read_h5df(g, "mydata"), error=identity)$message
truth = "h5in/dataset does not point to a valid dataset/h5df file"
stopifnot(identical(err, truth))
