%define name dlt-tools
%define version 0.0.1
%define unmangled_version 0.0.1
%define unmangled_version 0.0.1
%define release 1

Summary: DLT tools - basically a landsat listener and downloader
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: BSD
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Prakash <prakraja@umail.iu.edu>
Requires: python python-requests python-setuptools>=10
Url: https://github.com/datalogistics/dlt-misc

%description
Dlt tools

%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}


%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT
rm -f INSTALLED_FILES

%files -f INSTALLED_FILES
%defattr(-,root,root)
