#!/bin/bash
set -e
java -ea -cp ./hamcrest-core-1.3.jar:./junit.jar:target/kiwi-1.0-SNAPSHOT.jar kiwi.KiWiMap
