#!/bin/bash

export LD_LIBRARY_PATH="$JAVA_HOME:/usr/lib/hadoop/lib/native:/usr/lib/jvm/default/jre/lib/amd64/server/"
export CLASSPATH="$(hadoop classpath --glob )"
