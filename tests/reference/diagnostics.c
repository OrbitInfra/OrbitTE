#include "orbitte/diagnostics.h"
#include <stdio.h>
#include <string.h>

static int32_t write_json(const uint8_t *bytes, size_t len, void *file) {
    return fwrite(bytes, 1, len, (FILE *)file) == len ? 0 : 1;
}
static int32_t verify(const uint8_t *bytes, size_t len, void *expected) {
    char copy[4096];
    if (!len || len >= sizeof(copy)) return 1;
    memcpy(copy, bytes, len);
    copy[len] = 0;
    return strstr(copy, (const char *)expected) && !strstr(copy, "private") ? 0 : 1;
}
static int32_t reject(const uint8_t *bytes, size_t len, void *context) {
    (void)bytes;
    (void)len;
    (void)context;
    return 1;
}
int main(void) {
    if (orbitte_diagnostics_version(99, write_json, stdout) != 2 ||
        orbitte_diagnostics_version(ORBITTE_DIAGNOSTIC_JSON, NULL, NULL) != 2 ||
        orbitte_diagnostics_version(ORBITTE_DIAGNOSTIC_TEXT, reject, NULL) != 4)
        return 1;
    char redacted[] = "[redacted]";
    char failed[] = "[fail] tool.internal";
    const char *redact = "{\"mode\":\"strict\",\"fields\":[{\"field\":\"token\",\"value\":\"private\"}],\"text\":\"key\"}";
    const char *snapshot = "{\"schema_version\":1,\"command\":\"test\",\"build\":{\"version\":\"1\",\"commit\":\"a\",\"compiler\":\"test\",\"build_type\":\"test\",\"features\":{}},\"sections\":[],\"checks\":[{\"id\":\"tool.internal\",\"outcome\":\"fail\",\"summary\":\"fault\",\"evidence\":[],\"remediation\":[]}],\"redaction\":{\"mode\":\"default\",\"fields_redacted\":0}}";
    if (orbitte_diagnostics_render((const uint8_t *)snapshot, strlen(snapshot), ORBITTE_DIAGNOSTIC_TEXT, false, verify, failed) != 4 ||
        orbitte_diagnostics_redact((const uint8_t *)redact, strlen(redact), verify, redacted) != 0 ||
        orbitte_diagnostics_render(NULL, 0, ORBITTE_DIAGNOSTIC_JSON, false, verify, NULL) != 2)
        return 1;
    return orbitte_diagnostics_version(ORBITTE_DIAGNOSTIC_JSON, write_json, stdout);
}
