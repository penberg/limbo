#ifndef CHECK_H

#define CHECK_EQUAL(expected, actual) \
	do { \
		if ((expected) != (actual)) { \
			fprintf(stderr, "%s:%d: Assertion failed: %d != %d\n", __FILE__, __LINE__, (expected), (actual)); \
			exit(1); \
		} \
	} while (0)

#endif
