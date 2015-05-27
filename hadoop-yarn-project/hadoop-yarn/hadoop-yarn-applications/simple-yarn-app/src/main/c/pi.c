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

void writeProgress(FILE *f, unsigned long curr_i, unsigned long max_i) {
    const double percent = (100.0/max_i) * curr_i;
    const int p_len = snprintf(NULL, 0, "%0.2f", percent);
    char p_str[p_len+1];
    
    snprintf(p_str, p_len+1, "%f", percent);
    rewind(f);
    fprintf(f, "%s\n", p_str);
}

int main(int argc, char **argv) {
	const double denom = 4;
	double divisor = 1;
	double pi = 0.0;

    if (argc != 3) {
        usage(argv[0]);
        return 0;
    }

    const unsigned long iterations = get_iterations(argv[1]);
    const int container = atoi(argv[2]);

    if (iterations == 0) {
        usage(argv[0]);
        return -1;
    }

	printf("Starting Pi calculation YARN app using infinite series\n");
    printf("Using %lu iterations\n", iterations);

    FILE *f = fopen(argv[2], "w");

	for (unsigned long i = 0; i < iterations; ++i) {
		pi -= denom / divisor;
		divisor+=2;
		pi += denom / divisor; 
		divisor+=2;
        writeProgress(f, i, iterations);
    }

    fclose(f);

    printf("Pi: %1.10f\n", pi*-1);
	
    return 0;
}
