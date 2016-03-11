# spark-thrift-test
Example of reading writing thrift using spark

Argument:

-Djava.library.path="/path/to/hadoop/lib/native"
example: -Djava.library.path="/usr/local/Cellar/hadoop-2.7.1-src/hadoop-dist/target/hadoop-2.7.1/lib/native/"
File included:

$ls /usr/local/Cellar/hadoop-2.7.1-src/hadoop-dist/target/hadoop-2.7.1/lib/native/
libgplcompression.dylib libhadoop.1.0.0.dylib   libhadoop.dylib         libhadooputils.a        libhdfs.a               liblzo2.2.dylib         liblzo2.dylib
libgplcompression.la    libhadoop.a             libhadooppipes.a        libhdfs.0.0.0.dylib     libhdfs.dylib           liblzo2.a

Native LZO should be installed. It will create following directory in MAC
$ls /usr/local/Cellar/lzo/2.09/lib/
    liblzo2.2.dylib liblzo2.a       liblzo2.dylib

$ls ls /usr/local/Cellar/lzop/1.03/bin/
   lzop

Add hadoop-lzo-lib.jar in the class path.






