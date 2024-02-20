#!/bin/bash
#
# Copyright 2024 asyncer.io projects
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -e

if [ "$#" -ne 1 ] || [ "${1##*.}" != "log" ]; then
    echo "Please provide a single log file with a .log extension."
    exit 1
fi

if grep -q 'LEAK' "$1" ; then
    echo "LEAK FOUND: The log file $1 contains a memory leak."
    exit 1
fi

echo "No Leak: The log file $1 does not contain any memory leaks."
exit 0