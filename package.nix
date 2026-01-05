{
  stdenv,
  lib,
  cmake,
  libfds,
  docutils,
  libxml2,
  rdkafka,
  zlib,
  lz4,
  nemea-framework,
  git,
  cacert
}:

stdenv.mkDerivation rec {
  pname = "ipfixcol2";
  version = "2.7.1";

  src = ./.;

  nativeBuildInputs = [ cmake git cacert ];
  buildInputs = [ libfds docutils libxml2 rdkafka zlib lz4 nemea-framework ];

  postInstall = ''
    cd ../extra_plugins/output/unirec
    mkdir build
    cd build
    echo "Building Unirec plugin"
    cmake .. -DCMAKE_INSTALL_PREFIX=$out -DCMAKE_C_FLAGS="$CFLAGS -Wno-error=implicit-function-declaration" -DCMAKE_INSTALL_LIBDIR=lib 
    make
    echo "Make install unirec"
    make install
    cd ../../clickhouse
    mkdir build
    cd build
    echo "Building Clickhouse plugin"
    cmake .. -DCMAKE_INSTALL_PREFIX=$out -DCMAKE_C_FLAGS="$CFLAGS -Wno-error=implicit-function-declaration" -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    make
    echo "Make install clickhouse"
    make install
  '';

  meta = {
    description = "Flexible, high-performance NetFlow v5/v9 and IPFIX flow data collector designed to be extensible by plugins";
    homepage = "https://github.com/CESNET/ipfixcol2";
    license = lib.licenses.gpl2Plus;
    platforms = lib.platforms.linux;
  };
}