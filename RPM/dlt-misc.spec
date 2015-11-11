%define default_release 1

Name: dlt-tools
Version: 1.0
Release: %{?release}%{!?release:%{default_release}}
Summary: Dlt tools


Group:	        Application/System
License:	http://www.apache.org/licenses/LICENSE-2.0
URL:	        https://github.com/datalogistics/dlt-misc
Source0:	%{name}.tar.gz
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}

BuildRequires: tcl
Requires: tcl

%description
Bunch of dlt scripts to listen for landsat scenes and upload/download stuff

%prep
%setup -n dlt-tools

# %prep
# %setup -n ibp_server

%build
mkdir -p dlt-tools

# cmake -DCMAKE_INSTALL_PREFIX:PATH=%{buildroot} .
# make

%install
install -m 755 landsat_listener.py ${RPM_BUILD_ROOT}/usr/bin
# install -m 755 landsat_listener.py ${RPM_BUILD_ROOT}/usr/dlt-misc
# rm -rf ${RPM_BUILD_ROOT}/bin

%files
%defattr(-,root,root)
%{_bindir}/*
%{_libdir}/*
%{_includedir}/*.h
%{_datadir}/lors/*.tcl
%{_datadir}/lors/*.gif
%{_datadir}/lors/*.pl
%{_datadir}/lors/pkill

%clean
rm -rf %{buildroot}

# %post
# rpmconf --owner=accre-ibp-server

%files
*
# %changelog
# * Tue Nov 03 2015 <jayaajay@indiana.edu> 1.0-9-accre-ibp-server
# - Updated the paths to executables and sysconf files.
