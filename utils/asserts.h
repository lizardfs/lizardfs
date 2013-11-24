#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>

/* Check if condition if true; if no -- abort */
#define utils_massert(condition) do { \
			if(!(condition)) { \
				fprintf(stderr, "Assert failure: %s\n", #condition); \
				fflush(stdout); \
				abort(); \
			} \
		} while(0)

/* Unrecoverable failure */
#define utils_mabort(msg) do { \
			fprintf(stderr, "Failure: %s\n", #msg); \
			fflush(stdout); \
			abort(); \
		} while(0)

/* Check if condition if true; if no -- perror and abort */
#define utils_passert(condition) do { \
			if(!(condition)) { \
				perror(#condition); \
				fflush(stdout); \
				abort(); \
			} \
	} while(0)

/* Check if expression is equal to 0; if no -- perror and abort */
#define utils_zassert(expression) do { \
			if((expression) != 0) { \
				perror(#expression); \
				fflush(stdout); \
				abort(); \
			} \
	} while(0)
