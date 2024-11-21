include $(GGGPATH)/install/.compiler_ggg

GFIT = $(GGGPATH)/src/gfit
COMN = $(GGGPATH)/src/comn

run_effective_path: run_effective_path.o $(COMN)/vsubs.o $(GFIT)/compute_vertical_paths.o
	$(FC) $(LDFLAGS) -o $@ $^

%.o: %.f
	$(FC) $(CFLAGS) -o $@ $<
