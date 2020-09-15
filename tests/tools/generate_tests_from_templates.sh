#!/bin/bash

CMAKE_DIRECTORY="$(pwd)"

for n in {2..21}; do
	path=${CMAKE_DIRECTORY}/test_suites/LongSystemTests/test_brute_force_lost_chunks_ec_${n}_k.sh
	echo "Generating ${path}"
	cat > ${path} <<-EOF
	# @generator
	# callback="@callback@"
	# source test_suites/TestTemplates/test_brute_force_lost_chunks_ec_n_k.wrapper
	# @endgenerator
	n=${n} source test_suites/TestTemplates/test_brute_force_lost_chunks_ec_n_k.inc
	unset callback

EOF
done
