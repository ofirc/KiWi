#!/bin/bash
set -e
jdb -sourcepath src -classpath ./hamcrest-core-1.3.jar:./junit.jar:target/kiwi-1.0-SNAPSHOT.jar kiwi.KiWiMap
