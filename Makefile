CARGOCMD ?= cargo

ifndef GGGPATH
$(error "Must have GGGPATH set")
endif

ifdef GGGRS_NCDIR

HDF5_DIR := $(GGGRS_NCDIR)
export HDF5_DIR
NETCDF_DIR := $(GGGRS_NCDIR)
export NETCDF_DIR
RUSTFLAGS := -C link-args=-Wl,-rpath,$(HDF5_DIR)/lib
export RUSTFLAGS
CARGOARGS = --features netcdf

else

CARGOARGS = --features netcdf,static

endif

install:
	$(CARGOCMD) install $(CARGOARGS) --path . --root $(GGGPATH)