#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>

/* Check if condition if true; if no -- exit */
#define utils_massert(condition) do { \
			if(!(condition)) { \
				fprintf(stderr, "Assertion %s failed.\n", #condition); \
				exit(1); \
			} \
		} while(0)

/* Unrecoverable failure */
#define utils_mabort(msg) do { \
			fprintf(stderr, "Failure: %s\n", #msg); \
			exit(1); \
		} while(0)

/* Check if condition if true; if no -- perror and exit */
#define utils_passert(condition) do { \
			if(!(condition)) { \
				perror("Assertion " #condition " failed"); \
				exit(1); \
			} \
	} while(0)

/* Check if expression is equal to 0; if no -- perror and exit */
#define utils_zassert(expression) do { \
			if((expression) != 0) { \
				perror(#expression); \
				exit(1);\
			} \
	} while(0)
