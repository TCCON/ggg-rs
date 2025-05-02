# Airmass correction file format

For backwards compatibility, this file is in the typical GGG input file format
of a line specifying the "shape" of the file, one or more header rows, then
tabular data.
These files come in two forms.
The first defines airmass corrections for each window, and includes not only
the magnitude of the correction, but two additional parameters that adjust the
form of the equation used for the correction.

## Per-window format

The GGG2020 TCCON standard file in this format is as follows:

```text
15 5
2017-02-16  GCT
2015-08-11  DW
2019-01-14  JLL
2020-12-04  JLL: extrapolated to Xluft = 0.999
2021-02-22  JLL: fit for mid-trop PT = 310 K
2021-07-15  JLL: uncertainties added from 2-sigma std dev of bootstrapped PT = 310 K fits
Contains airmass-dependent correction factors to be applied to the
column-averaged mole fractions. These are determined offline from the
symmetric component of the diurnal variation using derive_airmass_correction.
The ADCF_Err should be the 1-sigma standard deviations which represent day-to-
day variability. This vastly overestimates the uncertainty in the average
value, however the standard error underestimates the uncertainty.
g and p are the zero-SZA and exponent in the ADCF form.
 Gas         ADCF      ADCF_Err  g    p
"xco2_6220"  -0.00903  0.00025   15   4
"xco2_6339"  -0.00512  0.00025   45   5
"xlco2_4852"  0.00008  0.00018  -45   1
"xwco2_6073" -0.00235  0.00016  -45   1
"xwco2_6500" -0.00970  0.00026   45   5
"xch4_5938"  -0.00971  0.00046   25   4
"xch4_6002"  -0.00602  0.00053  -5    2
"xch4_6076"  -0.00594  0.00044   15   3
"xn2o_4395"   0.00523  0.00054  -5    2
"xn2o_4430"   0.00426  0.00042   13   3
"xn2o_4719"  -0.00267  0.00056  -15   2
"xco_4233"    0.00000  0.00000   13   3
"xco_4290"    0.00000  0.00000   13   3
"xluft_6146"  0.00053  0.00017  -45   1
```

The components are as follows:

- The first line specifies the number of header lines and the number of data columns.
  This must be two integers separated by whitespace.
  The number of header lines includes this line and the column headers.
- The next `nhead-2` lines (line numbers 2 to 14 in this case) are free format; these are
  skipped by the program. You can see in the example that thse are used to record the
  history of the file and notes about the content of the file.
- The last header line (line number 15 in this case) gives the column names; it must include
  the five columns shown here.

```admonish info
A common error is to add lines to the header without updating the number of header lines
on the first line.
If you get an error running `apply_tccon_airmass_correction` after editing the correction
file's header, double check that you also updated the number of header lines!
```

The data are as follows:

- "Gas" is the Xgas window name that the correction defined on this line applies to.
  It must be a string that matches a non-error column in the input `.vsw` file with "x" prepended.
  As this is read in as [list directed format data](https://docs.oracle.com/cd/E19957-01/805-4939/6j4m0vnc5/index.html),
  it is recommended to quote the strings.
- "ADCF" is the airmass dependent correction factor, it determines the magnitude of the airmass correction.
- "ADCF_Err" is the uncertainty on the ADCF.
- "g" and "p" are parameters in the airmass correction equation.

Deriving the correction parameters is a complicated process.
For details, along with the definition of the airmass correction equation, please see section 8.1 of the [GGG2020 paper](https://doi.org/10.5194/essd-16-2197-2024).

## Per-gas format

The second format of the airmass correction file is as follows:

```text
13 3
2017-02-16  GCT
2015-08-11  DW
Contains airmass-dependent and airmass-independent (in situ)
correction factors to be applied to the column-averaged mole fractions.
The former (ADCF) is determined offline from the symmetric component
of the diurnal variation using derive_airmass_correction.
The ADCF_Err are the 1-sigma standard deviations which represent day-to-
day variability. This vastly overestimates the uncertainty in the average
value, however the standard error underestimates the uncertainty.
The latter (AICF) is determined offline by comparisons with in situ profiles.
AICF_Err (uncertainties) are 1-sigma standard deviations from the best fit.
 Gas      ADCF  ADCF_Err
"xco2"  -0.0049  0.0009
"xch4"  -0.0045  0.0005
"xn2o"   0.0133  0.0001
"xco"    0.0000  0.0001
"xh2o"  -0.0000  0.0001
"xluft"  0.0027  0.0005
```

This is a simplified version of the per-window format [above](#per-window-format).
As above, the first line defines the number of header lines and data columns.
This file must have three data columns: "Gas", "ADCF", and "ADCF_Err".
These have the same meanings as in the per-window format.
The "g" and "p" columns can be omitted, as shown here.
