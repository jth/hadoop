#include <stdio.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>

unsigned long get_iterations(char *arg) {
    if (strncasecmp(arg, "uintmax", sizeof("uintmax")) == 0) {
        return UINT_MAX;
    } else if (strncasecmp(arg, "ulongmax", sizeof("ulongmax")) == 0) {
        return ULONG_MAX;
    }
    return strtoul(arg, NULL, 10);
}

void usage(char *argv0) {
    printf("Usage: %s <iters>\n", argv0);
}

int main(int argc, char **argv) {
	const double denom = 4;
	double divisor = 1;
	double pi = 0.0;

    if (argc != 2) {
        usage(argv[0]);
        return 0;
    }

    const unsigned long iterations = get_iterations(argv[1]);

    if (iterations == 0) {
        usage(argv[0]);
        return -1;
    }

	printf("Starting Pi calculation YARN app using infinite series\n");
	
	for (unsigned long i = 0; i < iterations; ++i) {
		pi -= denom / divisor;
		divisor+=2;
		pi += denom / divisor; 
		divisor+=2;
	}
	
    printf("Pi: %1.10f\n", pi*-1);
	
    return 0;
}
