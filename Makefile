CARGOCMD ?= cargo
GGGRS_FEATURES ?= netcdf

ifndef GGGPATH
$(error "Must have GGGPATH set")
endif

# We have several options for how to get our netCDF/HDF5 library
# directory:
#
# 1. If GGGRS_NCDIR is set to a path, use that
# 2. If GGGRS_NCDIR is set to "AUTO", then we will manage an environment
#    in this directory
# 3. If GGGRS_NCDIR is set to "STATIC", then we will build the netCDF and
#    HDF libraries through cargo
# 4. If GGGRS_NCDIR is not set, we will check for $GGGPATH/install/.condaenv,
#    and if that doesn't exist, then fall back on "AUTO"

ifndef GGGRS_NCDIR
	ifeq ("$(wildcard $(GGGPATH)/install/.condaenv)","")
		GGGRS_NCDIR = AUTO
	else
		GGGRS_NCDIR = $(GGGPATH)/install/.condaenv
	endif
endif

ifeq ("$(GGGRS_NCDIR)", "AUTO")
	BUILD_ENV = 1
	GGGRS_NCDIR = $(CURDIR)/.condaenv

	ifndef GGG_ENV_TOOL
		ifneq ("$(shell which micromamba)", "")
			GGG_ENV_TOOL = micromamba
		else ifneq ("$(shell which mamba)", "")
			GGG_ENV_TOOL = mamba
		else ifneq ("$(shell which conda)", "")
			GGG_ENV_TOOL = conda
		else
			$(error "To automatically manage the netCDF/HDF5 environment one of micromamba, mamba, or conda must be installed")
		endif
	endif

	ifeq ("$(GGG_ENV_TOOL)", "conda")
		ENVCMD = $(GGG_ENV_TOOL) env create
	else
		ENVCMD = $(GGG_ENV_TOOL) create --yes
	endif

else
	BUILD_ENV = 0
endif

ifeq ("$(GGGRS_NCDIR)", "STATIC")
	CARGOARGS = --features $(GGGRS_FEATURES),static
	NC_LIB = 
else ifdef GGGRS_NCDIR
	HDF5_DIR := $(GGGRS_NCDIR)
	export HDF5_DIR
	NETCDF_DIR := $(GGGRS_NCDIR)
	export NETCDF_DIR
	RUSTFLAGS := -C link-args=-Wl,-rpath,$(HDF5_DIR)/lib
	export RUSTFLAGS
	CARGOARGS = --features $(GGGRS_FEATURES)
	ifeq ("$(shell uname -s)", "Darwin")
		NC_LIB = $(GGGRS_NCDIR)/lib/libnetcdf.dylib
	else
		NC_LIB = $(GGGRS_NCDIR)/lib/libnetcdf.so
	endif
else
	$(error "Bad configuration: failed to figure out where to load/build the netCDF and HDF5 libraries")
endif

install: $(NC_LIB)
	$(CARGOCMD) install $(CARGOARGS) --locked --path . --root $(GGGPATH)

debug: $(NC_LIB)
	$(CARGOCMD) build $(CARGOARGS)

release: $(NC_LIB)
	$(CARGOCMD) build $(CARGOARGS) --release

test: $(NC_LIB)
	$(CARGOCMD) test $(CARGOARGS) --bins $(TEST_PATTERN)

check: $(NC_LIB)
	$(CARGOCMD) check $(CARGOARGS)

docs: $(NC_LIB)
	$(CARGOCMD) doc $(CARGOARGS)

ifeq ("$(BUILD_ENV)", "1")
$(NC_LIB):
	rm -rf $(CURDIR)/.condaenv
	$(ENVCMD) --file $(CURDIR)/environment.yml --prefix $(CURDIR)/.condaenv
else
$(NC_LIB):
	@echo "GGGRS_NCDIR = $(GGGRS_NCDIR)"
endif

check-args:
	@echo "CARGOARGS = $(CARGOARGS)"
	@echo "BUILD_ENV = $(BUILD_ENV)"
	@echo "GGGRS_NCDIR = $(GGGRS_NCDIR)"
	@echo "GGG_ENV_TOOL = $(GGG_ENV_TOOL)"
	@echo "GGGRS_FEATURES = $(GGGRS_FEATURES)"
	@echo "ENVCMD = $(ENVCMD)"
	@echo "NC_LIB = $(NC_LIB)"
