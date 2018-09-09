#include "data.table.h"

double hsum(double *x, int grp_s, int grp_e) {
  /*
   * we could consider putting extra pragma parallel reduction here
   * to handle cases where there are very few (or even one) groups to utilize all cores
   * and not only as many cores as there are groups
   * that would be nested parallelism, as we already parallelise in outer function by doing single group per core, then use omp_set_nested(1)
   * creating team of workers has probably significant overhead thus this should examined in outer function and extra flag passed here
   * no support for NAs yet
   */
  double s=0;
  for (int i=grp_s-1; i<grp_e; i++) {
    s+=x[i];
  }
  return s;
}

SEXP groupbyR(SEXP x, SEXP y, SEXP by) {
  int protecti=0;
  /*
   * o = forderv(x, by=by, retGrp=TRUE)
   * C forder could be split into forderR.c and forder.c
   * then we can use forder.c and avoid SEXP and other overheads
   */
  SEXP retGrp = PROTECT(allocVector(LGLSXP, 1)); protecti++;
  SEXP sortStrArg = PROTECT(allocVector(LGLSXP, 1)); protecti++;
  SEXP orderArg = PROTECT(allocVector(INTSXP, 1)); protecti++;
  SEXP naArg = PROTECT(allocVector(LGLSXP, 1)); protecti++;
  LOGICAL(retGrp)[0] = TRUE; // we need pointers to groups
  LOGICAL(sortStrArg)[0] = TRUE; //default
  INTEGER(orderArg)[0] = 1; //default
  LOGICAL(naArg)[0] = FALSE; //default
  SEXP o = PROTECT(forder(x, by, retGrp, sortStrArg, orderArg, naArg)); protecti++;
  /*
   * xo = x[o, y, with=FALSE]
   * we should be able to skip this step by referencing
   * using something like x[o][j] instead of xo[j], not sure about perf
   */
  int n = xlength(o);
  double *dx = REAL(VECTOR_ELT(x, INTEGER(y)[0]-1));
  int *iord = INTEGER(o);
  double *drx = malloc((size_t)n * sizeof(double));
#pragma omp parallel for
  for (int i=0; i<n; i++) {
    drx[i] = dx[iord[i]-1];
  }
  
  /*
   * grps = attr(o, "starts", TRUE)
   * grps_end = c((grps-1L)[-1L], nrow(x))
   * this should also avoid operating on SEXP, and there is probably better way to find where groups ends
   */
  SEXP grp_s = PROTECT(getAttrib(o, sym_starts)); protecti++;
  int n_grps = xlength(grp_s);
  SEXP grp_e = PROTECT(allocVector(INTSXP, n_grps)); protecti++;
  for (int i=0; i<n_grps-1; i++) INTEGER(grp_e)[i] = INTEGER(grp_s)[i+1]-1;
  INTEGER(grp_e)[n_grps-1] = n; // last group closes on nrow(x)
  /*
   * ans = rep(0, length(grps))
   * 
   */
  SEXP ans = PROTECT(allocVector(REALSXP, n_grps)); protecti++;
  /*
   * #omp pragma
   * for (i in 1:length(grps)) {
   *   s = 0
   *   for (j in grps[i]:grps_end[i]) s = s + xo[j][[y]]
   *   ans[i] = s
   * }
   */
  int *igrp_s = INTEGER(grp_s);
  int *igrp_e = INTEGER(grp_e);
  double *dans = REAL(ans);
#pragma omp parallel for
  for (int i=0; i<n_grps; i++) {
    dans[i] = hsum(drx, igrp_s[i], igrp_e[i]);
  }
  
  // to make easier to debug everything return also intermediate results for now
  SEXP devans = PROTECT(allocVector(VECSXP, 4)); protecti++;
  SET_VECTOR_ELT(devans, 0, o);
  SET_VECTOR_ELT(devans, 1, grp_s);
  SET_VECTOR_ELT(devans, 2, grp_e);
  SET_VECTOR_ELT(devans, 3, ans);
  UNPROTECT(protecti);
  return devans;
}
