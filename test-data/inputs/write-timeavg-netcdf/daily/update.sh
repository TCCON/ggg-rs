#!/usr/bin/env bash

do_update() {
    site_id="$1"
    input_file="../../../inputs/write-timeavg-netcdf/averaging/${site_id}_input_test.private.qc.nc"
    output_file="./${site_id}.private.qc.avg30min.nc"
    make -f ../../../../Makefile run BIN=write_timeavg_netcdf ARGS="avg-site --gas co2 $input_file $output_file"
}

for sid in pa ci; do
    do_update $sid
done
