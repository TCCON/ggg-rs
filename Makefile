ifndef GGGPATH
$(error "Must have GGGPATH set")
endif

FEATURES ?= static

install:
	cargo install --features $(FEATURES) --path . --root $(GGGPATH)