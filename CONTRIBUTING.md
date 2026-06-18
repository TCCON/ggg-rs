# Contributing to GGG-RS

## Git branch structure

To allow us to have a stable version of GGG-RS and still maintain active development, we use the [Git Flow](https://nvie.com/posts/a-successful-git-branching-model/) model.
The linked post goes into detail, but in short there are 5 types of branches:

- `main`: This is the stable branch. Never commit directly to `main`, and never merge into `main` unless you have the TCCON algorithm co-chair or deputy co-chair's approval.
- `develop`: This is the cutting edge branch. Generally, do not commit directly to this branch either.
- `feature/*`: This is a branch where you develop a significant change to the code, such as a new program or alteration to how a current program or programs work.
- `bugfix/*`: This is a branch where you develop a quick fix for a bug.
- `release/*`: This is a branch where the code is stablized to merge into `main` for a release. Generally only the algorithm co-chair or deputy co-chair will create these branches.

## Types of contributions

**General advice:**

- It's usually best to start by opening an issue rather than immediately starting a PR.
  This lets the algorithm leads give you feedback before you invest time into something that
  they tried and found didn't work or which isn't compatible with all use cases of the code.
- If dealing with a bug, include a minimum reproducible example. For example, if the `write_public_netcdf`
  is crashing when converting a private file, try to create a private file with as few spectra
  in it as possible that still triggers the bug.

**If you want to fix a bug in the current stable version:**

1. Create a `bugfix` branch from the current main branch, e.g. `git switch -c bugfix/netcdf-crash-on-leapyear main`
2. Develop the fix on that branch. If possible, add a test to prevent the bug from recurring in the future (i.e., a regression test)
3. Propose a PR against the `main` branch and tag the algorith co-chair and/or deputy for a review.

**If you want to fix a bug or add a feature in the cutting edge version:**

1. Create a `bugfix` or `feature` branch from the current develop branch, e.g. `git switch -c feature/second-detector-config develop`
2. Develop on that branch. Add tests for your feature or bugfix wherever possible.
3. Propose a PR against the `develop` branch and tag the algorith co-chair and/or deputy for a review.

## Guidelines

In no particular order:

- Any code that relies on the netCDF crate should be gated behind the `netcdf` feature.
    - The netCDF library can be a bit annoying to get to work, so we try to enable users who don't need it to skip it.
- Before adding a new function, check the existing code under `lib` to see if there is a function that does what you need, 
  or close enough that you can wrap it.
- Avoid assumptions about file names in general code; if you need to extract the site ID, runlog name, etc. from a file name,
  use one of the general utility functions for that. This way if filename structures change in the future, only those helper
  functions need updated.
