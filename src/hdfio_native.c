/* Automatically generated. Do not edit by hand. */

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <stdlib.h>

extern SEXP R_space_pad_strings(SEXP x);

static const R_CallMethodDef CallEntries[] = {
  {"R_space_pad_strings", (DL_FUNC) &R_space_pad_strings, 1},
  {NULL, NULL, 0}
};

void R_init_meanr(DllInfo *dll)
{
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}
