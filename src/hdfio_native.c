/* Automatically generated. Do not edit by hand. */

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <stdlib.h>

extern SEXP R_get_max_str_len(SEXP x);

static const R_CallMethodDef CallEntries[] = {
  {"R_get_max_str_len", (DL_FUNC) &R_get_max_str_len, 1},
  {NULL, NULL, 0}
};

void R_init_meanr(DllInfo *dll)
{
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}
