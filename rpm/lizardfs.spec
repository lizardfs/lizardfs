%define distro @DISTRO@

Summary:        LizardFS - distributed, fault tolerant file system
Name:           lizardfs
Version:        3.13.0
Release:        0%{?distro}
License:        GPL v3
Group:          System Environment/Daemons
URL:            http://www.lizardfs.org/
Source:         lizardfs-%{version}.tar.gz
BuildRequires:  fuse-devel
BuildRequires:  cmake
BuildRequires:  pkgconfig
BuildRequires:  zlib-devel
BuildRequires:  asciidoc
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
BuildRequires:  systemd
%endif
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%define         liz_project        lizardfs
%define         liz_group          %{liz_project}
%define         liz_user           %{liz_project}
%define         liz_datadir        %{_localstatedir}/lib/%{liz_project}
%define         liz_confdir        %{_sysconfdir}/%{liz_project}
%define         liz_limits_conf    /etc/security/limits.d/10-lizardfs.conf
%define         liz_pam_d          /etc/pam.d/lizardfs
%define         _unpackaged_files_terminate_build 0

%description
LizardFS is an Open Source, easy to deploy and maintain, distributed,
fault tolerant file system for POSIX compliant OSes.
LizardFS is a fork of MooseFS. For more information please visit
http://lizardfs.com

# Packages
############################################################

%package master
Summary:        LizardFS master server
Group:          System Environment/Daemons
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
Requires(post): systemd-units
Requires(preun): systemd-units
Requires(postun): systemd-units
%endif

%description master
LizardFS master (metadata) server together with metarestore utility.

%package metalogger
Summary:        LizardFS metalogger server
Group:          System Environment/Daemons
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
Requires(post): systemd-units
Requires(preun): systemd-units
Requires(postun): systemd-units
%endif

%description metalogger
LizardFS metalogger (metadata replication) server.

%package chunkserver
Summary:        LizardFS data server
Group:          System Environment/Daemons
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
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
Requires:       bash-completion

%description client
LizardFS client: mfsmount and mfstools.

%package client3
Summary:        LizardFS client using FUSE3
Group:          System Environment/Daemons
Requires:       lizardfs-client

%description client3
LizardFS client: mfsmount and mfstools.

%package lib-client
Summary:        LizardFS client C/C++ library
Group:          Development/Libraries

%description lib-client
LizardFS client library for C/C++ bindings.

%package nfs-ganesha
Summary:        LizardFS plugin for nfs-ganesha
Group:          System Environment/Libraries
Requires:       lizardfs-lib-client

%description nfs-ganesha
LizardFS fsal plugin for nfs-ganesha.

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
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
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

%package uraft
Summary:        LizardFS cluster management tool
Group:          System Environment/Daemons
Requires:       lizardfs-master
Requires:       lizardfs-adm
Requires:       boost-system
Requires:       boost-program-options

%description uraft
LizardFS cluster management tool.

# Scriptlets - master
############################################################

%pre master
if ! getent group %{liz_group} > /dev/null 2>&1 ; then
	groupadd --system %{liz_group}
fi
if ! getent passwd %{liz_user} > /dev/null 2>&1 ; then
	adduser --system -g %{liz_group} --no-create-home --home-dir %{liz_datadir} %{liz_user}
fi
if [ ! -f %{liz_limits_conf} ]; then
	echo "%{liz_user} soft nofile 10000" > %{liz_limits_conf}
	echo "%{liz_user} hard nofile 10000" >> %{liz_limits_conf}
	chmod 0644 %{liz_limits_conf}
fi
if [ ! -f %{liz_pam_d} ]; then
	echo "session	required	pam_limits.so" > %{liz_pam_d}
fi
exit 0

%post master
%if "%{distro}" == "el6"
/sbin/chkconfig --add lizardfs-master
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_post lizardfs-master.service
%endif

%preun master
%if "%{distro}" == "el6"
if [ "$1" = 0 ] ; then
	/sbin/service lizardfs-master stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del lizardfs-master
fi
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_preun lizardfs-master.service
%endif

%postun master
%if "%{distro}" == "el6"
/sbin/service lizardfs-master condrestart > /dev/null 2>&1 || :
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
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
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_post lizardfs-metalogger.service
%endif

%preun metalogger
%if "%{distro}" == "el6"
if [ "$1" = 0 ] ; then
	/sbin/service lizardfs-metalogger stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del lizardfs-metalogger
fi
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_preun lizardfs-metalogger.service
%endif

%postun metalogger
%if "%{distro}" == "el6"
/sbin/service lizardfs-metalogger condrestart > /dev/null 2>&1 || :
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
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
if [ ! -f %{liz_limits_conf} ]; then
	echo "%{liz_user} soft nofile 10000" > %{liz_limits_conf}
	echo "%{liz_user} hard nofile 10000" >> %{liz_limits_conf}
	chmod 0644 %{liz_limits_conf}
fi
if [ ! -f %{liz_pam_d} ]; then
	echo "session	required	pam_limits.so" > %{liz_pam_d}
fi
exit 0

%post chunkserver
%if "%{distro}" == "el6"
/sbin/chkconfig --add lizardfs-chunkserver
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_post lizardfs-chunkserver.service
%endif

%preun chunkserver
%if "%{distro}" == "el6"
if [ "$1" = 0 ] ; then
	/sbin/service lizardfs-chunkserver stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del lizardfs-chunkserver
fi
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_preun lizardfs-chunkserver.service
%endif

%postun chunkserver
%if "%{distro}" == "el6"
/sbin/service lizardfs-chunkserver condrestart > /dev/null 2>&1 || :
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_postun_with_restart lizardfs-chunkserver.service
%endif

# Scriptlets - CGI server
############################################################

%post cgiserv
%if "%{distro}" == "el6"
/sbin/chkconfig --add lizardfs-cgiserv
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_post lizardfs-cgiserv.service
%endif

%preun cgiserv
%if "%{distro}" == "el6"
if [ "$1" = 0 ] ; then
	/sbin/service lizardfs-cgiserv stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del lizardfs-cgiserv
fi
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_preun lizardfs-cgiserv.service
%endif

%postun cgiserv
%if "%{distro}" == "el6"
/sbin/service lizardfs-cgiserv condrestart > /dev/null 2>&1 || :
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%systemd_postun_with_restart lizardfs-cgiserv.service
%endif

# Scriptlets - client3
############################################################

%post client3
/bin/ln -s %{_mandir}/man1/mfsmount.1 %{_mandir}/man1/mfsmount3.1

# Scriptlets - uraft
############################################################

%post uraft
echo "net.ipv4.conf.all.arp_accept = 1" > /etc/sysctl.d/10-lizardfs-uraft-arp.conf
chmod 0664 /etc/sysctl.d/10-lizardfs-uraft-arp.conf
sysctl -p /etc/sysctl.d/10-lizardfs-uraft-arp.conf
echo "# Allow lizardfs user to set floating ip" > /etc/sudoers.d/lizardfs-uraft
echo "lizardfs    ALL=NOPASSWD:/sbin/ip" >> /etc/sudoers.d/lizardfs-uraft
echo 'Defaults !requiretty' >> /etc/sudoers

# Prep, build, install, files...
############################################################

%prep
%setup

%build
./configure --with-doc
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
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
install -d -m755 $RPM_BUILD_ROOT/%{_unitdir}
for f in rpm/service-files/*.service ; do
	install -m644 "$f" $RPM_BUILD_ROOT/%{_unitdir}/$(basename "$f")
done
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files master
%defattr(644,root,root,755)
%doc NEWS README.md UPGRADE
%attr(755,root,root) %{_sbindir}/mfsmaster
%attr(755,root,root) %{_sbindir}/mfsrestoremaster
%attr(755,root,root) %{_sbindir}/mfsmetadump
%attr(755,root,root) %{_sbindir}/mfsmetarestore
%attr(755,%{liz_user},%{liz_group}) %dir %{liz_datadir}
%{_mandir}/man5/mfsexports.cfg.5*
%{_mandir}/man5/mfstopology.cfg.5*
%{_mandir}/man5/mfsgoals.cfg.5*
%{_mandir}/man5/mfsmaster.cfg.5*
%{_mandir}/man5/globaliolimits.cfg.5*
%{_mandir}/man7/mfs.7*
%{_mandir}/man7/moosefs.7*
%{_mandir}/man7/lizardfs.7*
%{_mandir}/man8/mfsmaster.8*
%{_mandir}/man8/mfsmetadump.8*
%{_mandir}/man8/mfsmetarestore.8*
%{_mandir}/man8/mfsrestoremaster.8*
%{liz_confdir}/mfsexports.cfg
%{liz_confdir}/mfstopology.cfg
%{liz_confdir}/mfsgoals.cfg
%{liz_confdir}/mfsmaster.cfg
%{liz_confdir}/globaliolimits.cfg
%attr(644,root,root) %{liz_datadir}/metadata.mfs.empty
%if "%{distro}" == "el6"
%attr(754,root,root) %{_initrddir}/lizardfs-master
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%attr(644,root,root) %{_unitdir}/lizardfs-master.service
%endif

%files metalogger
%defattr(644,root,root,755)
%doc NEWS README.md UPGRADE
%attr(755,root,root) %{_sbindir}/mfsmetalogger
%attr(755,%{liz_user},%{liz_group}) %dir %{liz_datadir}
%{_mandir}/man5/mfsmetalogger.cfg.5*
%{_mandir}/man8/mfsmetalogger.8*
%{liz_confdir}/mfsmetalogger.cfg
%if "%{distro}" == "el6"
%attr(754,root,root) %{_initrddir}/lizardfs-metalogger
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%attr(644,root,root) %{_unitdir}/lizardfs-metalogger.service
%endif

%files chunkserver
%defattr(644,root,root,755)
%doc NEWS README.md UPGRADE
%attr(755,root,root) %{_sbindir}/mfschunkserver
%attr(755,%{liz_user},%{liz_group}) %dir %{liz_datadir}
%{_mandir}/man5/mfschunkserver.cfg.5*
%{_mandir}/man5/mfshdd.cfg.5*
%{_mandir}/man8/mfschunkserver.8*
%{liz_confdir}/mfschunkserver.cfg
%{liz_confdir}/mfshdd.cfg
%if "%{distro}" == "el6"
%attr(754,root,root) %{_initrddir}/lizardfs-chunkserver
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%attr(644,root,root) %{_unitdir}/lizardfs-chunkserver.service
%endif

%files client
%defattr(644,root,root,755)
%doc NEWS README.md UPGRADE
%attr(755,root,root) %{_bindir}/lizardfs
%attr(755,root,root) %{_bindir}/mfsmount
%attr(755,root,root) %{_bindir}/mfstools.sh
%{_bindir}/mfsappendchunks
%{_bindir}/mfscheckfile
%{_bindir}/mfsdeleattr
%{_bindir}/mfsdirinfo
%{_bindir}/mfsfileinfo
%{_bindir}/mfsfilerepair
%{_bindir}/mfsgeteattr
%{_bindir}/mfsgetgoal
%{_bindir}/mfsgettrashtime
%{_bindir}/mfsmakesnapshot
%{_bindir}/mfsrepquota
%{_bindir}/mfsrgetgoal
%{_bindir}/mfsrgettrashtime
%{_bindir}/mfsrsetgoal
%{_bindir}/mfsrsettrashtime
%{_bindir}/mfsseteattr
%{_bindir}/mfssetgoal
%{_bindir}/mfssetquota
%{_bindir}/mfssettrashtime
%{_mandir}/man1/lizardfs-appendchunks.1*
%{_mandir}/man1/lizardfs-checkfile.1*
%{_mandir}/man1/lizardfs-deleattr.1*
%{_mandir}/man1/lizardfs-dirinfo.1*
%{_mandir}/man1/lizardfs-fileinfo.1*
%{_mandir}/man1/lizardfs-filerepair.1*
%{_mandir}/man1/lizardfs-geteattr.1*
%{_mandir}/man1/lizardfs-getgoal.1*
%{_mandir}/man1/lizardfs-gettrashtime.1*
%{_mandir}/man1/lizardfs-makesnapshot.1*
%{_mandir}/man1/lizardfs-repquota.1*
%{_mandir}/man1/lizardfs-rgetgoal.1*
%{_mandir}/man1/lizardfs-rgettrashtime.1*
%{_mandir}/man1/lizardfs-rsetgoal.1*
%{_mandir}/man1/lizardfs-rsettrashtime.1*
%{_mandir}/man1/lizardfs-seteattr.1*
%{_mandir}/man1/lizardfs-setgoal.1*
%{_mandir}/man1/lizardfs-setquota.1*
%{_mandir}/man1/lizardfs-settrashtime.1*
%{_mandir}/man1/lizardfs-rremove.1*
%{_mandir}/man1/lizardfs.1*
%{_mandir}/man5/iolimits.cfg.5*
%{_mandir}/man7/mfs.7*
%{_mandir}/man7/moosefs.7*
%{_mandir}/man1/mfsmount.1*
%{_mandir}/man5/mfsmount.cfg.5*
%{liz_confdir}/mfsmount.cfg
%{liz_confdir}/iolimits.cfg
%{_sysconfdir}/bash_completion.d/lizardfs

%files client3
%attr(755,root,root) %{_bindir}/mfsmount3
%{_mandir}/man1/mfsmount3.1*

%files lib-client
%{_libdir}/liblizardfsmount_shared.so
%{_libdir}/liblizardfs-client.so
%{_libdir}/liblizardfs-client-cpp.a
%{_libdir}/liblizardfs-client-cpp_pic.a
%{_libdir}/liblizardfs-client.a
%{_libdir}/liblizardfs-client_pic.a
%{_includedir}/lizardfs/lizardfs_c_api.h
%{_includedir}/lizardfs/lizardfs_error_codes.h

%files nfs-ganesha
%{_libdir}/ganesha/libfsallizardfs.so
%{_libdir}/ganesha/libfsallizardfs.so.4
%{_libdir}/ganesha/libfsallizardfs.so.4.2.0

%files cgi
%defattr(644,root,root,755)
%doc NEWS README.md UPGRADE
%dir %{_datadir}/mfscgi
%{_datadir}/mfscgi/err.gif
%{_datadir}/mfscgi/favicon.ico
%{_datadir}/mfscgi/index.html
%{_datadir}/mfscgi/logomini.png
%{_datadir}/mfscgi/mfs.css
%attr(755,root,root) %{_datadir}/mfscgi/mfs.cgi
%attr(755,root,root) %{_datadir}/mfscgi/chart.cgi

%files cgiserv
%defattr(644,root,root,755)
%attr(755,root,root) %{_sbindir}/lizardfs-cgiserver
%attr(755,root,root) %{_sbindir}/mfscgiserv
%{_mandir}/man8/lizardfs-cgiserver.8*
%{_mandir}/man8/mfscgiserv.8*
%if "%{distro}" == "el6"
%attr(754,root,root) %{_initrddir}/lizardfs-cgiserv
%endif
%if "%{distro}" == "el7" || "%{distro}" == "fc24"
%attr(644,root,root) %{_unitdir}/lizardfs-cgiserv.service
%endif

%files adm
%defattr(644,root,root,755)
%doc NEWS README.md UPGRADE
%attr(755,root,root) %{_bindir}/lizardfs-admin
%{_mandir}/man8/lizardfs-admin.8*
%{_bindir}/lizardfs-probe
%{_mandir}/man8/lizardfs-probe.8*

%files uraft
%defattr(644,root,root,755)
%attr(755,root,root) %{_sbindir}/lizardfs-uraft
%attr(755,root,root) %{_sbindir}/lizardfs-uraft-helper
%doc NEWS README.md UPGRADE
%{_mandir}/man8/lizardfs-uraft.8*
%{_mandir}/man8/lizardfs-uraft-helper.8*
%{_mandir}/man5/lizardfs-uraft.cfg.5*
%{liz_confdir}/lizardfs-uraft.cfg
%if "%{distro}" == "el6"
%attr(754,root,root) %{_initrddir}/lizardfs-uraft
%endif
%if "%{distro}" == "el7"
%attr(644,root,root) %{_unitdir}/lizardfs-uraft.service
%attr(644,root,root) %{_unitdir}/lizardfs-ha-master.service
%endif

%changelog
* Thu Jun 28 2018 Pawel Kalinowski <contact@lizardfs.org> - 3.13.0
- (all) uRaft HA
- (all) fixes to EC handling
- (all) nfs-ganesha plugin changed to use only C code
- (mount) reduced number of secondary groups retrievals (better performance)
- (mount) add fuse3 client (better performance, writeback cache)
- (all) many fixes

* Wed Nov 22 2017 Pawel Kalinowski <contact@lizardfs.org> - 3.12.0
- (all) C API
- (all) nfs-ganesha plugin
- (all) RichACL support (which includes NFSv4)
- (all) OSX ACL support
- (master, mount) file lock fixes
- (mount) client readahead enabled by default
- (mount) AVX2 extensions support for erasure code goals
- (chunkserver) more flexible options
- (all) many fixes

* Tue May 9 2017 Piotr Sarna <contact@lizardfs.org> - 3.11.0
- (master) improve ACL implementation
- (master) add option to avoid same-ip chunkserver replication
- (master) add minimal goal configuration option
- (master) reimplement directory entry cache for faster lookups
- (master) add whole-path lookups
- (master, chunkserver) add chunkserver load awareness
- (mount) add readahead to improve sequential read perfromance
- (mount) add secondary groups support
- (tools) add correct-only flag to filerepair
- (tools) add -s and -i options to snapshot command
- (tools) add recursive remove operations (for removing large directories and snapshots)
- (tools) add tool for stopping execution of tasks (snapshot, recursive remove, etc.)
- (all) change to semantic versioning system
- (all) many fixes

* Fri Oct 7 2016 Piotr Sarna <contact@lizardfs.org> - 3.10.4
- (master) task manager performance improvements
- (master) trash fixes

* Tue Aug 30 2016 Piotr Sarna <contact@lizardfs.org> - 3.10.2
- (master) redesign in-memory representation of file system objects - at least 30% reduction in RAM usage
- (master) name storage - a possibility to keep all file names in BerkeleyDB, thus saving even more RAM
- (master) redesign of trash - increased performance, reduced RAM usage and CPU pressure
- (master) huge boost of responsiveness - lengthy operations split into asynchronous bits
- (master) OPERATIONS_DELAY* config entries, which allow postponing metadata operations on restart/disconnect
- (master) fix improper handling of endangered chunks
- (chunkserver) memory optimizations - at least 60% reduction in RAM usage
- (chunkserver) introduce smart descriptor management
- (tools) brand new `lizardfs` command, a unified replacement for mfs* tools with prompt and bash completion
- (all) various fixes and community requests

* Mon Mar 14 2016 Piotr Sarna <contact@lizardfs.org> - 3.10.0
- (all) Added erasure code goals
- (all) Added per directory quotas
- (all) Improved interaction with legacy version (chunkservers, mounts)
- (all) Ports for OSX and FreeBSD
- (all) Many fixes

* Wed Dec 2 2015 Piotr Sarna <contact@lizardfs.org> - 3.9.4
- (master) Removed master server overload on restarting chunkservers
- (master) Improved global file locks engine
- (chunkserver) Fixed leaking descriptors problem
- (chunkserver) Improved mechanism of moving chunks to new directory layout
- (chunkserver) Fixed issues related to scanning directories with new chunk format present
- (mount) Removed hang in mount when chunkserver reported no valid copies of a file
- (master) Changed handling of legacy (pre-3.9.2) chunkservers in new installations
- (cgi) Added XOR replication to statistics
- (all) Removed default linking to tcmalloc library due to performance drop

* Fri Oct 23 2015 Piotr Sarna <contact@lizardfs.org> - 3.9.2
- (all) Introduced XOR goal types
- (all) Added file locks (flock & fcntl)
- (all) Increased max number of files from 500 million to over 4 billion
- (all) Introduced managing open file limits by PAM
- (master) Improved consistency of applying changelogs by shadow masters
- (master) Redesigned snapshot execution in master
- (master) Redesigned chunk loop logic
- (master) Added option to limit chunk loop's CPU usage
- (master) Removed hard coded connection limit
- (chunkserver) Added new network threads responsible for handling requests
  sent by chunkserver's clients
- (chunkserver) Introduced new more efficient directory layout
- (chunkserver) Added option to choose if fsync should be performed after each write
  for increased safety
- (chunkserver) Removed hard coded connection limit
- (chunkserver) Added replication network bandwidth limiting
- (mount) Improved symlink cache and added configurable timeout value
- (all) Minor bug fixes and improvements

* Mon Feb 09 2015 Adam Ochmanski <contact@lizardfs.org> - 2.6.0
- (all) Added comments in all config files
- (all) Improve messages printed by daemons when starting
- (cgi) A new chunkserver's chart: number of chunk tests
- (cgi) Fixed paths to static content
- (cgi) New implementation of the CGI server; mfscgiserv is now deprecated.
- (cgi) New table: 'Metadata Servers' in the 'Servers' tab
- (chunkserver) Allowed starts with damaged disks
- (chunkserver) A new option: HDD_ADVISE_NO_CACHE
- (chunkserver) Improved handling of disk read errors
- (chunkserver) Removed 'testing chunk: xxxxxx' log messages
- (master) A new feature: disabling atime updates (globally)
- (master) Fixed rotating changelogs and downloading files in shadow mode
- (probe) New commands
- (probe) Renamed to lizardfs-admin
- (all) Minor bug fixes and improvements

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
