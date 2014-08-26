# build with "--define 'distro XXX' set to:
# "rh" for Fedora/RHEL/CentOS
# ... (other awaiting contribution)

#define distro  rh

Summary:        LizardFS - distributed, fault tolerant file system
Name:           lizardfs
Version:        2.5.0
Release:        1%{?distro}
License:        GPL v3
Group:          System Environment/Daemons
URL:            http://www.lizardfs.org/
Source0:        https://github.com/lizardfs/lizardfs/archive/v%{version}.tar.gz
Patch0:         lizardfs_redhat_fix.patch
BuildRequires:  fuse-devel
BuildRequires:  pkgconfig
BuildRequires:  zlib-devel
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%define         _localstatedir  /var/lib
%define         mfsconfdir      %{_sysconfdir}/mfs

%description
MooseFS is an Open Source, easy to deploy and maintain, distributed,
fault tolerant file system for POSIX compliant OSes.
LizardFS is fork of MooseFS. For more information please visit
http://lizardfs.com

%package master
Summary:        LizardFS master server
Group:          System Environment/Daemons

%description master
LizardFS master (metadata) server together with metarestore utility.

%package metalogger
Summary:        LizardFS metalogger server
Group:          System Environment/Daemons

%description metalogger
LizardFS metalogger (metadata replication) server.

%package chunkserver
Summary:        LizardFS data server
Group:          System Environment/Daemons

%description chunkserver
LizardFS data server.

%package client
Summary:        LizardFS client
Group:          System Environment/Daemons
Requires:       fuse

%description client
LizardFS client: mfsmount and mfstools.

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

%description cgiserv
Simple CGI-capable HTTP server to run LizardFS CGI Monitor.

%package adm
Summary:        LizardFS administration utility
Group:          System Environment/Daemons

%description adm
LizardFS command line administration utility.

%prep
%setup -q

%patch0 -p0 -b .patch0

%build
%configure
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT

make install \
        DESTDIR=$RPM_BUILD_ROOT

%if "%{distro}" == "rh"
install -d $RPM_BUILD_ROOT%{_initrddir}
for f in rpm/rh/*.init ; do
        sed -e 's,@sysconfdir@,%{_sysconfdir},;
                s,@sbindir@,%{_sbindir},;
                s,@initddir@,%{_initrddir},' $f > $RPM_BUILD_ROOT%{_initrddir}/$(basename $f .init)
done
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files master
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_sbindir}/mfsmaster
%attr(755,root,root) %{_sbindir}/mfsrestoremaster
%attr(755,root,root) %{_sbindir}/mfsmetadump
%attr(755,root,root) %{_sbindir}/mfsmetarestore
%attr(755,mfs,mfs) %{_localstatedir}/mfs
%{_mandir}/man5/mfsexports.cfg.5*
%{_mandir}/man5/mfstopology.cfg.5*
%{_mandir}/man5/mfsmaster.cfg.5*
%{_mandir}/man5/globaliolimits.cfg.5*
%{_mandir}/man7/mfs.7*
%{_mandir}/man7/moosefs.7*
%{_mandir}/man7/lizardfs.7*
%{_mandir}/man8/mfsmaster.8*
%{_mandir}/man8/mfsmetarestore.8*
%{mfsconfdir}/mfsexports.cfg.dist
%{mfsconfdir}/mfstopology.cfg.dist
%{mfsconfdir}/mfsmaster.cfg.dist
%{mfsconfdir}/globaliolimits.cfg.dist

%dir %{_localstatedir}/mfs
%attr(640,root,root) %{_localstatedir}/mfs/metadata.mfs.empty
%if "%{distro}" == "rh"
%attr(754,root,root) %{_initrddir}/mfsmaster
%endif

%files metalogger
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_sbindir}/mfsmetalogger
%attr(755,mfs,mfs) %{_localstatedir}/mfs
%{_mandir}/man5/mfsmetalogger.cfg.5*
%{_mandir}/man8/mfsmetalogger.8*
%{mfsconfdir}/mfsmetalogger.cfg.dist
%if "%{distro}" == "rh"
%attr(754,root,root) %{_initrddir}/mfsmetalogger
%endif

%files chunkserver
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_sbindir}/mfschunkserver
%attr(755,mfs,mfs) %{_localstatedir}/mfs
%{_mandir}/man5/mfschunkserver.cfg.5*
%{_mandir}/man5/mfshdd.cfg.5*
%{_mandir}/man8/mfschunkserver.8*
%{mfsconfdir}/mfschunkserver.cfg.dist
%{mfsconfdir}/mfshdd.cfg.dist
%if "%{distro}" == "rh"
%attr(754,root,root) %{_initrddir}/mfschunkserver
%endif

%files client
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_bindir}/mfsappendchunks
%attr(755,root,root) %{_bindir}/mfscheckfile
%attr(755,root,root) %{_bindir}/mfsdeleattr
%attr(755,root,root) %{_bindir}/mfsdirinfo
%attr(755,root,root) %{_bindir}/mfsfileinfo
%attr(755,root,root) %{_bindir}/mfsfilerepair
%attr(755,root,root) %{_bindir}/mfsgeteattr
%attr(755,root,root) %{_bindir}/mfsgetgoal
%attr(755,root,root) %{_bindir}/mfsgettrashtime
%attr(755,root,root) %{_bindir}/mfsmakesnapshot
%attr(755,root,root) %{_bindir}/mfsmount
%attr(755,root,root) %{_bindir}/mfsrepquota
%attr(755,root,root) %{_bindir}/mfsrgetgoal
%attr(755,root,root) %{_bindir}/mfsrgettrashtime
%attr(755,root,root) %{_bindir}/mfsrsetgoal
%attr(755,root,root) %{_bindir}/mfsrsettrashtime
%attr(755,root,root) %{_bindir}/mfsseteattr
%attr(755,root,root) %{_bindir}/mfssetgoal
%attr(755,root,root) %{_bindir}/mfssetquota
%attr(755,root,root) %{_bindir}/mfssettrashtime
%attr(755,root,root) %{_bindir}/mfssnapshot
%attr(755,root,root) %{_bindir}/mfstools
%{_mandir}/man1/mfsappendchunks.1*
%{_mandir}/man1/mfscheckfile.1*
%{_mandir}/man1/mfsdeleattr.1*
%{_mandir}/man1/mfsdirinfo.1*
%{_mandir}/man1/mfsfileinfo.1*
%{_mandir}/man1/mfsfilerepair.1*
%{_mandir}/man1/mfsgeteattr.1*
%{_mandir}/man1/mfsgetgoal.1*
%{_mandir}/man1/mfsgettrashtime.1*
%{_mandir}/man1/mfsmakesnapshot.1*
%{_mandir}/man1/mfsrepquota.1*
%{_mandir}/man1/mfsrgetgoal.1*
%{_mandir}/man1/mfsrgettrashtime.1*
%{_mandir}/man1/mfsrsetgoal.1*
%{_mandir}/man1/mfsrsettrashtime.1*
%{_mandir}/man1/mfsseteattr.1*
%{_mandir}/man1/mfssetgoal.1*
%{_mandir}/man1/mfssetquota.1*
%{_mandir}/man1/mfssettrashtime.1*
%{_mandir}/man1/mfstools.1*
%{_mandir}/man5/iolimits.cfg.5*
%{_mandir}/man7/mfs.7*
%{_mandir}/man7/moosefs.7*
%{_mandir}/man1/mfsmount.1*
%{mfsconfdir}/mfsmount.cfg.dist
%{mfsconfdir}/iolimits.cfg.dist

%files cgi
%defattr(644,root,root,755)
%attr(755,root,root) %{_datadir}/mfscgi/mfs.cgi
%attr(755,root,root) %{_datadir}/mfscgi/chart.cgi
%doc NEWS README UPGRADE
%{_datadir}/mfscgi

%files cgiserv
%defattr(644,root,root,755)
%attr(755,root,root) %{_sbindir}/mfscgiserv
%{_mandir}/man8/mfscgiserv.8*
%if "%{distro}" == "rh"
%attr(754,root,root) %{_initrddir}/mfscgiserv
%endif

%files adm
%defattr(644,root,root,755)
%doc NEWS README UPGRADE
%attr(755,root,root) %{_bindir}/lizardfs-probe
%{_mandir}/man8/lizardfs-probe.8*

%changelog
* Wed Jul 23 2014 George Lucan <george.lucan@yahoo.com> - 2.5.0
- (all) lizardfs rebranding
- (adm) introduce adm package
- (master) add globaliolimits.cfg and manuals
- (client) add iolimits.cfg and manuals


* Wed Oct 16 2013 Peter aNeutrino <contact@lizardfs.org> - 1.6.28-1
- (all) compile with g++ by default
- (deb) fix init scripts for debian packages
- (all) fix build on Mac OS X
- (cgi) introducing LizardFS logo

* Thu Feb 16 2012 Jakub Bogusz <contact@moosefs.com> - 1.6.27-1
- adjusted to keep configuration files in /etc/mfs
- require just mfsexports.cfg (master) and mfshdd.cfg (chunkserver) in RH-like
  init scripts; for other files defaults are just fine to run services
- moved mfscgiserv to -cgiserv subpackage (-cgi alone can be used with any
  external CGI-capable HTTP server), added mfscgiserv init script

* Fri Nov 19 2010 Jakub Bogusz <contact@moosefs.com> - 1.6.19-1
- separated mfs-metalogger subpackage (following Debian packaging)

* Fri Oct  8 2010 Jakub Bogusz <contact@moosefs.com> - 1.6.17-1
- added init scripts based on work of Steve Huff (Dag Apt Repository)
  (included in RPMs when building with --define "distro rh")

* Mon Jul 19 2010 Jakub Kruszona-Zawadzki <contact@moosefs.com> - 1.6.16-1
- added mfscgiserv man page

* Fri Jun 11 2010 Jakub Bogusz <contact@moosefs.com> - 1.6.15-1
- initial spec file, based on Debian packaging;
  partially inspired by spec file by Kirby Zhou
