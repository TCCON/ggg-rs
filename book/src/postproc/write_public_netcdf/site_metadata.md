# Site metadata file

`write_public_netcdf` can take the data latency/release lag from a TOML file that specifies site metadata.
There is an example for the standard TCCON sites [in the repo](https://github.com/TCCON/ggg-rs/blob/main/src/etc/site_info.toml).
This file must have top-level keys that match site two-character IDs, each containing a number of metadata values.
For example:

```toml #notest
[pa]
long_name = "parkfalls01"
release_lag = 120
location = "Park Falls, Wisconsin, USA"
contact = "Paul Wennberg <wennberg@gps.caltech.edu>"
data_revision = "R1"
data_doi = "10.14291/tccon.ggg2014.parkfalls01.R1"
data_reference = "Wennberg, P. O., C. Roehl, D. Wunch, G. C. Toon, J.-F. Blavier, R. Washenfelder, G. Keppel-Aleks, N. Allen, J. Ayers. 2017. TCCON data from Park Falls, Wisconsin, USA, Release GGG2014R1. TCCON data archive, hosted by CaltechDATA, California Institute of Technology, Pasadena, CA, U.S.A. http://doi.org/10.14291/tccon.ggg2014.parkfalls01.R1"
site_reference = "Washenfelder, R. A., G. C. Toon, J.-F. L. Blavier, Z. Yang, N. T. Allen, P. O. Wennberg, S. A. Vay, D. M. Matross, and B. C. Daube (2006), Carbon dioxide column abundances at the Wisconsin Tall Tower site, Journal of Geophysical Research, 111(D22), 1-11, doi:10.1029/2006JD007154. Available from: https://www.agu.org/pubs/crossref/2006/2006JD007154.shtml"

[oc]
long_name = "lamont01"
release_lag = 120
location = "Lamont, Oklahoma, USA"
contact = "Paul Wennberg <wennberg@gps.caltech.edu>"
data_revision = "R1"
data_doi = "10.14291/tccon.ggg2014.lamont01.R1/1255070"
data_reference = "Wennberg, P. O., D. Wunch, C. Roehl, J.-F. Blavier, G. C. Toon, N. Allen, P. Dowell, K. Teske, C. Martin, J. Martin. 2017. TCCON data from Lamont, Oklahoma, USA, Release GGG2014R1. TCCON data archive, hosted by CaltechDATA, California Institute of Technology, Pasadena, CA, U.S.A. https://doi.org/10.14291/tccon.ggg2014.lamont01.R1/1255070"
site_reference = ""
```

Although the public netCDF writer only uses `release_lag`, each site _must_ contain the following keys for this file to be valid:

- `long_name`: the site's location readable name followed by a two-digit number indicating which instrument at that site this is.
- `release_lag`: an integer >= 0 specifying how many days after acquisition data should be kept private.
  TCCON sites are not permitted to set this >366, as data delivery to the public archive within one year is a network requirement.
- `location`: a description of the location where the instrument resides.
  This is usually "city, state/province/country", but can include things such as institution if desired.
- `contact`: the name and email address, formatted as `NAME <EMAIL>` of the person users should contact with questions or concern about this site's data.
- `data_revision`: an "R" followed by a number >= 0 indicating which iteration of GGG2020 reprocessing this data represents.
  This should be incremented whenever previously public data was reprocessed to fix an error.

The following keys may be provided, but are not required:

- `data_doi`: A digital object identifier that points to the public data for this site.
  This should be included if possible; it is optional only so that public files can be created before the first time a DOI is minted.
- `data_reference`: A reference to a persistent repository where the data can be downloaded.
  For TCCON sites, this will be CaltechData.
  For other instruments, this may vary for now.
- `site_reference`: A reference to a publication describing the site location itself (as opposed to the data).

TCCON sites can find the most up-to-date versions of their values for this metadata at https://tccondata.org/metadata/siteinfo/.
Other users should do their best to ensure that the above conventions are followed.