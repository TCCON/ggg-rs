#!/usr/bin/env bash

if [[ $# -lt 1 ]] || [[ $1 == "-h" ]] || [[ $1 == "--help" ]]; then
    echo "USAGE: $0 PA_PRIVATE_CONCAT_FILE"
    exit 2
fi


# usage: subset_netcdf [-h] [-o OUTPUT_FILE] [-c] [--simple-progress] [--tb] [--pdb] input_nc_file start_date [end_date]
MYDIR=$(dirname $0)
"$GGGPATH/bin/subset_netcdf" -o "${MYDIR}/pa_input_test.private.qc.nc.tmp" "$1" 20250604 20250608
rm -v "${MYDIR}/pa_input_test.private.qc.nc"
ncks \
    -v time,flag,lat,long,solzen,pout,prior_index,prior_pressure,ak_pressure,prior_1h2o,prior_1co2,ak_xco2,ak_slant_xco2_bin,xco2_x2019,xco2_error_x2019,aicf_xco2_x2019_scale,o2_7885_am_o2 \
    "${MYDIR}/pa_input_test.private.qc.nc.tmp" \
    "${MYDIR}/pa_input_test.private.qc.nc"
rm "${MYDIR}/pa_input_test.private.qc.nc.tmp"
