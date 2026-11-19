# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "netcdf4>=1.7.0",
# ]
# ///
from pathlib import Path
from netCDF4 import Dataset

def main() -> None:
    here = Path(__file__).parent
    src = here / '..' / 'new' 
    for ifile, src_file in enumerate(sorted(src.glob('*.nc4'))):
        file = src_file.copy(here / src_file.name)
        print(f'Editing {file}')
        with Dataset(file, 'a') as ds:
            grp = ds.groups['CO2']
            for iobs in range(grp.dimensions['n_obs_instr'].size):
                # Alternate adding and subtracting values to check that the
                # mean absolute bias correctly takes the absolute value, except
                # for num soundings to avoid potential negatives.
                if iobs % 2 == 0:
                    grp['num_soundings'][iobs] += 2 * (ifile + 1)
                    grp['column_mixing'][iobs] += 0.5 * (ifile + 1)
                    grp['sigma_column_mixing'][iobs] += 0.05 * (ifile + 1)
                else:
                    grp['column_mixing'][iobs] -= 0.25 * (ifile + 1)
                    grp['sigma_column_mixing'][iobs] -= 0.025 * (ifile + 1)

if __name__ == "__main__":
    main()
