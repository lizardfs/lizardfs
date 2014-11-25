%define distro @DISTRO@

Summary:        LizardFS - distributed, fault tolerant file system
Name:           lizardfs
Version:        2.5.5
Release:        1%{?distro}
License:        GPL v3
Group:          System Environment/Daemons
URL:            http://www.lizardfs.org/
Source:         lizardfs-%{version}.tar.gz
BuildRequires:  fuse-devel
BuildRequires:  cmake
BuildRequires:  pkgconfig
BuildRequires:  zlib-devel
%if "%{distro}" == "el7"
BuildRequires:  systemd
%endif
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%define         liz_project        lfs
%define         liz_group          %{liz_project}
%define         liz_user           %{liz_project}
%define         liz_datadir        %{_localstatedir}/lib/%{liz_project}
%define         liz_confdir        %{_sysconfdir}/%{liz_project}

%description
LizardFS is an Open Source, easy to deploy and maintain, distributed,
fault tolerant file system for POSIX compliant OSes.
LizardFS is a fork of LizardFS. For more information please visit
http://lizardfs.com

# Packages
############################################################

%package master
Summary:        LizardFS master server
Group:          System Environment/Daemons
%if "%{distro}" == "el7"
Requires(post): systemd-units
Requires(preun): systemd-units
Requires(postun): systemd-units
%endif

%description master
LizardFS master (metadata) server together with metarestore utility.

%package metalogger
Summary:        LizardFS metalogger server
Group:          System Environment/Daemons
%if "%{distro}" == "el7"
Requires(post): systemd-units
Requires(preun): systemd-units
Requires(postun): systemd-units
%endif

%description metalogger
LizardFS metalogger (metadata replication) server.

%package chunkserver
Summary:        LizardFS data server
Group:          System Environment/Daemons
%if "%{distro}" == "el7"
Requires(post): systemd-units
Requires(preun): systemd-units
Requires(postun): systemd-units
%endif

%description chunkserver
LizardFS data server.

%package client
Summary:        LizardFS client
Group:          System Environment/Daemons
Requires:       fuse
Requires:       fuse-libs

%description client
LizardFS client: lfsmount and lfstools.

%package cgi
Summary:        LizardFS CGI Monitor
Group:          System Environment/Daemons
Requires:       python

%description cgi
LizardFS CGI Monitor.

%package cgiserv
Summary:        Simple CGI-capable HTTP server to run LizardFS CGI Monitor
Group:          System Environment/Daemons
Requires:       %{name}-cgi = %{version}-%{release}
%if "%{distro}" == "el7"
Requires(post): systemd-units
Requires(preun): systemd-units
Requires(postun): systemd-units
%endif

%description cgiserv
Simple CGI-capable HTTP server to run LizardFS CGI Monitor.

%package adm
Summary:        LizardFS administration utility
Group:          System Environment/Daemons

%description adm
LizardFS command line administration utility.

# Scriptlets - master
############################################################

%pre master
if ! getent group %{liz_group} > /dev/null 2>&1 ; then
	groupadd --system %{liz_group}
fi
if ! getent passwd %{liz_user} > /dev/null 2>&1 ; then
	adduser --system -g %{liz_group} --no-create-home --home-dir %{liz_datadir} %{liz_user}
fi
exit 0

%post master
%if "%{distro}" == "el6"
/sbin/chkconfig --add lizardfs-master
%endif
%if "%{distro}" == "el7"
%systemd_post lizardfs-master.service
%endif

%preun master
%if "%{distro}" == "el6"
if [ "$1" = 0 ] ; then
	/sbin/service lizardfs-master stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del lizardfs-master
fi
%endif
%if "%{distro}" == "el7"
%systemd_preun lizardfs-master.service
%endif

%postun master
%if "%{distro}" == "el6"
/sbin/service lizardfs-master condrestart > /dev/null 2>&1 || :
%endif
%if "%{distro}" == "el7"
%systemd_postun_with_restart lizardfs-master.service
%endif

# Scriptlets - metalogger
############################################################

%pre metalogger
if ! getent group %{liz_group} > /dev/null 2>&1 ; then
	groupadd --system %{liz_group}
fi
if ! getent passwd %{liz_user} > /dev/null 2>&1 ; then
	adduser --system -g %{liz_group} --no-create-home --home-dir %{liz_datadir} %{liz_user}
fi
exit 0

%post metalogger
%if "%{distro}" == "el6"
/sbin/chkconfig --add lizardfs-metalogger
%endif
%if "%{distro}" == "el7"
%systemd_post lizardfs-metalogger.service
%endif

%preun metalogger
%if "%{distro}" == "el6"
if [ "$1" = 0 ] ; then
	/sbin/service lizardfs-metalogger stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del lizardfs-metalogger
fi
%endif
%if "%{distro}" == "el7"
%systemd_preun lizardfs-metalogger.service
%endif

%postun metalogger
%if "%{distro}" == "el6"
/sbin/service lizardfs-metalogger condrestart > /dev/null 2>&1 || :
%endif
%if "%{distro}" == "el7"
%systemd_postun_with_restart lizardfs-metalogger.service
%endif

# Scriptlets - chunkserver
############################################################

%pre chunkserver
if ! getent group %{liz_group} > /dev/null 2>&1 ; then
	groupadd --system %{liz_group}
fi
if ! getent passwd %{liz_user} > /dev/null 2>&1 ; then
	adduser --system -g %{liz_group} --no-create-home --home-dir %{liz_datadir} %{liz_user}
fi
exit 0

%post chunkserver
%if "%{distro}" == "el6"
/sbin/chkconfig --add lizardfs-chunkserver
%endif
%if "%{distro}" == "el7"
%systemd_post lizardfs-chunkserver.service
%endif

%preun chunkserver
%if "%{distro}" == "el6"
if [ "$1" = 0 ] ; then
	/sbin/service lizardfs-chunkserver stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del lizardfs-chunkserver
fi
%endif
%if "%{distro}" == "el7"
%systemd_preun lizardfs-chunkserver.service
%endif

%postun chunkserver
%if "%{distro}" == "el6"
/sbin/service lizardfs-chunkserver condrestart > /dev/null 2>&1 || :
%endif
%if "%{distro}" == "el7"
%systemd_postun_with_restart lizardfs-chunkserver.service
%endif

# Scriptlets - CGI server
############################################################

%post cgiserv
%if "%{distro}" == "el6"
/sbin/chkconfig --add lizardfs-cgiserv
%endif
%if "%{distro}" == "el7"
%systemd_post lizardfs-cgiserv.service
%endif

%preun cgiserv
%if "%{distro}" == "el6"
if [ "$1" = 0 ] ; then
	/sbin/service lizardfs-cgiserv stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del lizardfs-cgiserv
fi
%endif
%if "%{distro}" == "el7"
%systemd_preun lizardfs-cgiserv.service
%endif

%postun cgiserv
%if "%{distro}" == "el6"
/sbin/service lizardfs-cgiserv condrestart > /dev/null 2>&1 || :
%endif
%if "%{distro}" == "el7"
%systemd_postun_with_restart lizardfs-cgiserv.service
%endif

# Prep, build, install, files...
############################################################

%prep
%setup

%build
%configure
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT
%if "%{distro}" == "el6"
install -d $RPM_BUILD_ROOT%{_initrddir}
for f in rpm/init-scripts/*.init ; do
        sed -e 's,@sysconfdir@,%{_sysconfdir},;
                s,@sbindir@,%{_sbindir},;
                s,@initddir@,%{_initrddir},' $f > $RPM_BUILD_ROOT%{_initrddir}/$(basename $f .init)
done
%endif
%if "%{distro}" == "el7"
install -d -m755 $RPM_BUILD_ROOT/%{_unitdir}
for f in rpm/service-files/*.service ; do
	install -m644 "$f" $RPM_BUILD_ROOT/%{_unitdir}/$(basename "$f")
done
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files master
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_sbindir}/lfsmaster
%attr(755,root,root) %{_sbindir}/lfsrestoremaster
%attr(755,root,root) %{_sbindir}/lfsmetadump
%attr(755,root,root) %{_sbindir}/lfsmetarestore
%attr(755,%{liz_user},%{liz_group}) %dir %{liz_datadir}
%{_mandir}/man5/lfsexports.cfg.5*
%{_mandir}/man5/lfstopology.cfg.5*
%{_mandir}/man5/lfsgoals.cfg.5*
%{_mandir}/man5/lfsmaster.cfg.5*
%{_mandir}/man5/globaliolimits.cfg.5*
%{_mandir}/man7/lfs.7*
%{_mandir}/man7/lizardfs.7*
%{_mandir}/man7/lizardfs.7*
%{_mandir}/man8/lfsmaster.8*
%{_mandir}/man8/lfsmetarestore.8*
%{liz_confdir}/lfsexports.cfg.dist
%{liz_confdir}/lfstopology.cfg.dist
%{liz_confdir}/lfsgoals.cfg.dist
%{liz_confdir}/lfsmaster.cfg.dist
%{liz_confdir}/globaliolimits.cfg.dist
%attr(644,root,root) %{liz_datadir}/metadata.lfs.empty
%if "%{distro}" == "el6"
%attr(754,root,root) %{_initrddir}/lizardfs-master
%endif
%if "%{distro}" == "el7"
%attr(644,root,root) %{_unitdir}/lizardfs-master.service
%endif

%files metalogger
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_sbindir}/lfsmetalogger
%attr(755,%{liz_user},%{liz_group}) %dir %{liz_datadir}
%{_mandir}/man5/lfsmetalogger.cfg.5*
%{_mandir}/man8/lfsmetalogger.8*
%{liz_confdir}/lfsmetalogger.cfg.dist
%if "%{distro}" == "el6"
%attr(754,root,root) %{_initrddir}/lizardfs-metalogger
%endif
%if "%{distro}" == "el7"
%attr(644,root,root) %{_unitdir}/lizardfs-metalogger.service
%endif

%files chunkserver
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_sbindir}/lfschunkserver
%attr(755,%{liz_user},%{liz_group}) %dir %{liz_datadir}
%{_mandir}/man5/lfschunkserver.cfg.5*
%{_mandir}/man5/lfshdd.cfg.5*
%{_mandir}/man8/lfschunkserver.8*
%{liz_confdir}/lfschunkserver.cfg.dist
%{liz_confdir}/lfshdd.cfg.dist
%if "%{distro}" == "el6"
%attr(754,root,root) %{_initrddir}/lizardfs-chunkserver
%endif
%if "%{distro}" == "el7"
%attr(644,root,root) %{_unitdir}/lizardfs-chunkserver.service
%endif

%files client
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_bindir}/lfstools
%attr(755,root,root) %{_bindir}/lfsmount
%attr(755,root,root) %{_bindir}/lfssnapshot
%{_bindir}/lfsappendchunks
%{_bindir}/lfscheckfile
%{_bindir}/lfsdeleattr
%{_bindir}/lfsdirinfo
%{_bindir}/lfsfileinfo
%{_bindir}/lfsfilerepair
%{_bindir}/lfsgeteattr
%{_bindir}/lfsgetgoal
%{_bindir}/lfsgettrashtime
%{_bindir}/lfsmakesnapshot
%{_bindir}/lfsrepquota
%{_bindir}/lfsrgetgoal
%{_bindir}/lfsrgettrashtime
%{_bindir}/lfsrsetgoal
%{_bindir}/lfsrsettrashtime
%{_bindir}/lfsseteattr
%{_bindir}/lfssetgoal
%{_bindir}/lfssetquota
%{_bindir}/lfssettrashtime
%{_mandir}/man1/lfsappendchunks.1*
%{_mandir}/man1/lfscheckfile.1*
%{_mandir}/man1/lfsdeleattr.1*
%{_mandir}/man1/lfsdirinfo.1*
%{_mandir}/man1/lfsfileinfo.1*
%{_mandir}/man1/lfsfilerepair.1*
%{_mandir}/man1/lfsgeteattr.1*
%{_mandir}/man1/lfsgetgoal.1*
%{_mandir}/man1/lfsgettrashtime.1*
%{_mandir}/man1/lfsmakesnapshot.1*
%{_mandir}/man1/lfsrepquota.1*
%{_mandir}/man1/lfsrgetgoal.1*
%{_mandir}/man1/lfsrgettrashtime.1*
%{_mandir}/man1/lfsrsetgoal.1*
%{_mandir}/man1/lfsrsettrashtime.1*
%{_mandir}/man1/lfsseteattr.1*
%{_mandir}/man1/lfssetgoal.1*
%{_mandir}/man1/lfssetquota.1*
%{_mandir}/man1/lfssettrashtime.1*
%{_mandir}/man1/lfstools.1*
%{_mandir}/man5/iolimits.cfg.5*
%{_mandir}/man7/lfs.7*
%{_mandir}/man7/lizardfs.7*
%{_mandir}/man1/lfsmount.1*
%{liz_confdir}/lfsmount.cfg.dist
%{liz_confdir}/iolimits.cfg.dist

%files cgi
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%dir %{_datadir}/lfscgi
%{_datadir}/lfscgi/err.gif
%{_datadir}/lfscgi/favicon.ico
%{_datadir}/lfscgi/index.html
%{_datadir}/lfscgi/logomini.png
%{_datadir}/lfscgi/lfs.css
%attr(755,root,root) %{_datadir}/lfscgi/lfs.cgi
%attr(755,root,root) %{_datadir}/lfscgi/chart.cgi

%files cgiserv
%defattr(644,root,root,755)
%attr(755,root,root) %{_sbindir}/lfscgiserv
%{_mandir}/man8/lfscgiserv.8*
%if "%{distro}" == "el6"
%attr(754,root,root) %{_initrddir}/lizardfs-cgiserv
%endif
%if "%{distro}" == "el7"
%attr(644,root,root) %{_unitdir}/lizardfs-cgiserv.service
%endif

%files adm
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_bindir}/lizardfs-probe
%{_mandir}/man8/lizardfs-probe.8*

%changelog
* Sat Nov 08 2014 Alek Lewandowski <contact@lizardfs.org> - 2.5.5
- (none) None

* Fri Nov 07 2014 Alek Lewandowski <contact@lizardfs.org> - 2.5.4
- (all) Boost is no longer required to build the source code of LizardFS
  or use the binary version
- (all) Added tiering (aka 'custom goal') feature, which allows
  users to label chunkservers and to request chunks to be stored
  on specific groups of servers
- (cgi) "Exports" tabs renamed to "Config", now it also shows goal
  definitions
- (cgi) Added new tab "Chunks"
- (probe) New command "chunks-health" makes it possible to get number of
  missing or endangered chunks
- (master) Fixed reporting memory usage in CGI
- (mount) Fixed caching contents of open directories
- (mount) Add a .lizardfs_tweaks file
- (all) Other minor fixes and improvements

* Mon Sep 15 2014 Alek Lewandowski <contact@lizardfs.org> - 2.5.2
- (master, shadow) Metadata checksum mechanism, allowing to
  find and fix possible metadata inconsistencies between master
  and shadow
- (mount, master) ACL cache in mount, reducing the load of
  the master server
- (packaging) Support packaging for RedHat based systems
- (master) Improved chunkserver deregistration mechanism in
  order to avoid temporary master unresponsiveness
- (polonaise) Add filesystem API for developers allowing to
  use the filesystem without FUSE (and thus working also on
  Windows)
- (all) Minor fixes and improvements

* Tue Jul 15 2014 Marcin Sulikowski <sulik@lizardfs.org> - 2.5.0
- (master) High availability provided by shadow master servers
- (mount, chunkserver) CRC algorithm replaced with a 3 times faster
  implementation
- (mount, master) Support for quotas (for users and groups)
- (mount, master) Support for posix access contol lists (requires
  additional OS support)
- (mount, master) Support for global I/O limiting (bandwidth limiting)
- (mount) Support for per-mountpoint I/O limiting (bandwidth limiting)
- (adm) New package lizardfs-adm with a lizardfs-probe command-line
  tool which can be used to query the installation for variuos
  parameteres
- (master) New mechanism of storing metadata backup files which
  improves performance of the hourly metadata dumps
- (all) A comprehensive test suite added
- (all) Multiple bugfixes


* Wed Oct 16 2013 Peter aNeutrino <contact@lizardfs.org> - 1.6.28-1
- (all) compile with g++ by default
- (deb) fix init scripts for debian packages
- (all) fix build on Mac OS X
- (cgi) introducing LizardFS logo

* Thu Feb 16 2012 Jakub Bogusz <contact@lizardfs.com> - 1.6.27-1
- adjusted to keep configuration files in /etc/lfs
- require just lfsexports.cfg (master) and lfshdd.cfg (chunkserver) in RH-like
  init scripts; for other files defaults are just fine to run services
- moved lfscgiserv to -cgiserv subpackage (-cgi alone can be used with any
  external CGI-capable HTTP server), added lfscgiserv init script

* Fri Nov 19 2010 Jakub Bogusz <contact@lizardfs.com> - 1.6.19-1
- separated lfs-metalogger subpackage (following Debian packaging)

* Fri Oct  8 2010 Jakub Bogusz <contact@lizardfs.com> - 1.6.17-1
- added init scripts based on work of Steve Huff (Dag Apt Repository)
  (included in RPMs when building with --define "distro rh")

* Mon Jul 19 2010 Jakub Kruszona-Zawadzki <contact@lizardfs.com> - 1.6.16-1
- added lfscgiserv man page

* Fri Jun 11 2010 Jakub Bogusz <contact@lizardfs.com> - 1.6.15-1
- initial spec file, based on Debian packaging;
  partially inspired by spec file by Kirby Zhou
