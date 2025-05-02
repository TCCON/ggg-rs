# In situ correction file format

For backwards compatibility, this file is in the typical GGG input file format
of a line specifying the "shape" of the file, one or more header rows, then
tabular data.
The standard TCCON GGG2020 is show here as an example:

```text
19 4
2017-02-16  GCT
2015-08-11  DW
2021-07-26  JLL
This file contains airmass-independent correction factors (AICFs) determined
offline by comparison against in situ data. CO2, wCO2, lCO2, CH4, and H2O use AICFs
determined as the weighted mean ratio of TCCON to in situ Xgas values. N2O
uses the ratio of TCCON to surface-derived in situ XN2O fit to a mid-tropospheric
potential temperature of 310 K, ensuring that it is fixed to the same temperature
as the ADCFs. CO, H2O, and Luft are not corrected.
For CO2, wCO2, lCO2, CH4, H2O, and CO the AICF_Err (uncertainties) are 2-sigma standard
deviations of bootstrapped mean ratios. For N2O, the error equals the fit vs. potential
temperature multiplied by twice the standard deviation of potential temperatures
experienced by the TCCON network.
The WMO_Scale column gives the WMO scale from the in situ data that the scale
factor ties to. There must be something in this column and must be quoted;
use "N/A" for gases with no WMO scaling. NB: although CO has no WMO scale (because it
is not scaled), the uncertainty was determined from the measurements on the WMO X2014A scale.
 Gas     AICF  AICF_Err  WMO_Scale
"xco2"   1.0101  0.0005  "WMO CO2 X2007"
"xwco2"  1.0008  0.0005  "WMO CO2 X2007"
"xlco2"  1.0014  0.0007  "WMO CO2 X2007"
"xch4"   1.0031  0.0014  "WMO CH4 X2004"
"xn2o"   0.9821  0.0098  "NOAA 2006A"
"xco"    1.0000  0.0526  "N/A"
"xh2o"   0.9883  0.0157  "ARM Radiosondes (Lamont+Darwin)"
"xluft"  1.0000  0.0000  "N/A"
```

The components are as follows:

- The first line specifies the number of header lines and the number of data columns.
  This must be two integers separated by whitespace.
  The number of header lines includes this line and the column headers.
- The next `nhead-2` lines (line numbers 2 to 18 in this case) are free format; these are
  skipped by the program. You can see in the example that these are used to record the
  history of the file and notes about the content of the file.
- The last header line (line number 19 in this case) gives the column names; it must include
  the four columns shown here.

```admonish info
A common error is to add lines to the header without updating the number of header lines
on the first line.
If you get an error running `apply_tccon_insitu_correction` after editing the correction
file's header, double check that you also updated the number of header lines!
```

The data are as follows:

- "Gas" refers to the column in the `.vav.ada` file that the correction applies to
  (along with the associated error, i.e., "xco2" will apply to both the "xco2" and "xco2_error" columns).
- "AICF" is the scaling factor; the Xgas and error will be divided by this.
- "AICF_Err" is the uncertainty on the scaling factor.
- "WMO_Scale" is the metrological scale or other reference to which the scale factor ties the
  These must be quoted strings.
