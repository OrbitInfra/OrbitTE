#ifndef ORBITTE_DIAGNOSTICS_H
#define ORBITTE_DIAGNOSTICS_H
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

enum orbitte_diagnostic_format { ORBITTE_DIAGNOSTIC_TEXT, ORBITTE_DIAGNOSTIC_JSON };
/* Borrowed UTF-8 bytes, valid only inside the callback. Return zero on success. */
typedef int32_t (*orbitte_diagnostic_emit)(const uint8_t *, size_t, void *);
/* Offline build metadata. Returns 0 on success, 2 on misuse, 4 on output failure. */
int32_t orbitte_diagnostics_version(uint32_t format, orbitte_diagnostic_emit emit, void *context);

/* Borrowed UTF-8 JSON, at most 1 MiB. Render a sanitized diagnostic snapshot.
 * Returns its check exit code, 2 for invalid input, or 4 on output failure. */
int32_t orbitte_diagnostics_render(const uint8_t *data, size_t len, uint32_t format,
                                   bool warnings_are_errors, orbitte_diagnostic_emit emit, void *context);
/* One snapshot of {mode, fields: [{field, value, sensitivity}], text}.
 * Emits {fields, text, redaction}; deployment pseudonyms are shared within the call. */
int32_t orbitte_diagnostics_redact(const uint8_t *data, size_t len, orbitte_diagnostic_emit emit, void *context);

#ifdef __cplusplus
}
#endif
#endif
