ifndef GGGPATH
$(error "Must have GGGPATH set")
endif

include $(GGGPATH)/install/.compiler_gggrs.mk

install:
	$(CARGOCMD) install $(CARGOARGS) --path . --root $(GGGPATH)