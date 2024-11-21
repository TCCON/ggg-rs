      program run_effective_path

      implicit none

      integer*4 nhead, nprof, nlev, mprof, mlev, mmin,
     & iprof, imin, ilev, i, lunr, lunw
      parameter(mprof=2, mlev=51, mmin=10, lunr=14, lunw=15)

      real*4 z(mlev, mprof), d(mlev, mprof), vpath(mlev), zmin
      logical debug

      debug = .false.
    
c Read in our test data

      open(lunr, file='z_and_d.dat', status='old')
      read(lunr, *) nhead, nprof, nlev

      if(nprof .gt. mprof) then
        write(*,*) 'nprof > mprof, increase mprof'
        stop
      endif

      if(nlev .gt. mlev) then
        write(*,*) 'nlev > mlev, increase mlev'
        stop
      endif

      do i=2,nhead
        read(lunr,*)
      enddo

      do ilev=1,nlev
        read(lunr,*) (z(ilev,iprof), d(ilev,iprof), iprof=1,nprof)
      enddo

      close(lunr)
    
      if (debug) then
        do iprof=1,nprof
            write(*,'(a,i2,a)') 'Profile ', iprof, ' z  d'
            do ilev=1,nlev
              write(*,'(f6.2,1x,1pe10.4)')
     & z(ilev, iprof), d(ilev, iprof)
            enddo
        enddo
      endif

c Calculate vpath for a variety of zmin values for each profile
      open(lunw, file='vpath.dat', status='unknown')

      do iprof=1,nprof
        zmin = -0.1
        do imin=1,mmin
           zmin = zmin + 0.2
           write(lunw, '(a,1x,i2,1x,a,1x,f5.2)') 
     & 'Profile', iprof, 'zmin =', zmin 
           call compute_vertical_paths(
     & 0, zmin, z(:,iprof), d(:,iprof), vpath, nlev)
           write(lunw, '(a6,1x,100f12.4)') 
     & 'z:', (z(ilev,iprof), ilev=1,nlev)
           write(lunw, '(a6,1x,100(1pe12.4))')
     & 'd:', (d(ilev,iprof), ilev=1,nlev)
           write(lunw, '(a6,1x,100f12.4)')
     & 'vpath:', (vpath(ilev), ilev=1,nlev)
        enddo
        write(*,*) ''
      enddo
      
      close(lunw)
      end program
