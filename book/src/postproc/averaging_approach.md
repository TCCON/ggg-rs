# Averaging approach

## With multiplicative bias

For quantities like the gas columns or VSFs, it is most likely that there is a multiplicative
bias between the windows.
In these cases, this arises because of systematic biases in the spectroscopy between the different
windows (which ofter cover different spectral bands or branches). 
Thus, we want a way to calculate the average across windows that accounts for both the individual
values' errors and the mean multiplicative bias between windows.

This can operate in two modes.
In one mode, the window scaling factors are determined to best match the data being averaged.
This is necessary when first developing a new algorithm version with new spectroscopy.
However, for TCCON, the more common mode is to fix the scale factors (from running the first mode
with enough data to be representative of the network).
This is necessary so that the window-to-window averaging does not depend on how many spectra are
processed at one time.
See [section 8.2 of Laughner et al. (2024)](https://doi.org/10.5194/essd-16-2197-2024) for how
the fixed scaling factors were derived for GGG2020.

### Mathematical form

Solving for scale factors is done by minimizing:

\\[ \chi^2 = \sum\_i \sum\_j \left( \frac{ y\_{ij} - \overline{y}\_i s\_j }{ \epsilon\_{y\_{ij}} } \right)^2 + \sum\_j \left( \frac{ s\_j - s\_{a\_j} }{ \epsilon\_{s\_{a\_j}} } \right)^2 \\]

where

- \\( y\_{ij} \\) is the quantity for spectrum _i_, window _j_,
- \\( \epsilon\_{y\_{ij}} \\) is the uncertainty on \\( y\_{ij} \\),
- \\( \overline{y}\_i \\) is the averaged quantity for spectrum _i_,
- \\( s\_j \\) is the scale factor for window _j_,
- \\( s\_{a\_j} \\) is the _a priori_ scale factor for window _j_, and
- \\( \epsilon\_{s\_{a\_j}} \\) is the _a priori_ scale factor uncertainty.

In this equation, the first term is a cost that is minimized by scale factors that best fit the data,
while the second term is a weak constraint that tries to keep the scale factors close to their _a priori_
values (usually 1).

To find the optimal scale factors, we differentiate the above equation with respect to each \\( s\_j \\) and
set that to 0, giving the equation

\\[ \sum\_i  \overline{y}\_i \frac{ y\_{ij} - \overline{y}\_i s\_j }{ \epsilon\_{y\_{ij}}^2 }  + \frac{ s\_{a\_j} - s\_j }{ \epsilon\_{s\_{a\_j}}^2 } = 0 \\]

Rearranging to solve for \\( s\_j \\) gives

\\[ s\_j = \frac{ s\_{a\_j} / \epsilon\_{s\_{a\_j}}^2 + \sum\_i \overline{y}\_i y\_{ij} / \epsilon\_{ij}^2 }{ 1 / \epsilon\_{s\_{a\_j}}^2 + \sum\_i \overline{y}\_i^2 / \epsilon\_{y\_{ij}}^2 } \\]

Similarly, we can differentiate the \\(\chi^2\\) equation with respect to each \\( \overline{y}\_i \\):


\\[ \sum\_j s\_j \frac{ y\_{ij} - \overline{y}\_i s\_j }{ \epsilon\_{y\_{ij}}^2 } = 0 \\]

and rearrange this to solve for \\( \overline{y}\_i \\):

\\[ \overline{y}\_i = \frac{ \sum\_j y\_{ij} s\_j / \epsilon\_{y\_{ij}}^2  }{ \sum\_j s\_j^2 / \epsilon\_{y\_{ij}}^2 } \\]

Because \\( \overline{y}\_i \\) and \\( s\_j \\) depend on each other, the averaging code must solve these equations iteratively.
In practice, it will iterate until \\( \chi^2 \\) stops decreasing or the maximum number of iterations is reached.
The latter case is an error.
