/* Test-only GNU loader audit. Hide cuFile or redirect it to the existing CPU shim. */
#define _GNU_SOURCE
#include <link.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

unsigned int la_version(unsigned int version) {
    write(2, "storage audit active\n", sizeof("storage audit active\n") - 1);
    return version < LAV_CURRENT ? 0 : LAV_CURRENT;
}

char *la_objsearch(const char *name, uintptr_t *cookie, unsigned int flag) {
    (void)cookie;
    const char *base = strrchr(name, '/');
    base = base ? base + 1 : name;
    if (flag == LA_SER_ORIG && strncmp(base, "libcufile.so", sizeof("libcufile.so") - 1) == 0) {
        write(2, "cuFile lookup\n", sizeof("cuFile lookup\n") - 1);
        return getenv("ORBITE_TEST_CUFILE"); /* NULL makes the SONAME unavailable. */
    }
    if (flag == LA_SER_ORIG && strncmp(base, "liburing.so", sizeof("liburing.so") - 1) == 0)
        write(2, "liburing lookup\n", sizeof("liburing lookup\n") - 1);
    return (char *)name;
}
