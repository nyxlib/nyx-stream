/* NyxStream
 * Author: Jérôme ODIER <jerome.odier@lpsc.in2p3.fr>
 * SPDX-License-Identifier: GPL-2.0-only
 */

/*--------------------------------------------------------------------------------------------------------------------*/

#ifndef NYX_STREAM_H
#define NYX_STREAM_H

/*--------------------------------------------------------------------------------------------------------------------*/

#include <stddef.h>
#include <stdint.h>

/*--------------------------------------------------------------------------------------------------------------------*/

#define __NYX_NOTNULL__ \
            /* do nothing */

#define __NYX_NULLABLE__ \
            /* do nothing */

#define __NYX_ZEROABLE__ \
            /* do nothing */

#define __NYX_UNUSED__ \
            __attribute__ ((unused))

#define __NYX_INLINE__ \
            __attribute__ ((always_inline)) static inline

/*--------------------------------------------------------------------------------------------------------------------*/

#define str_t /*-*/ char *
#define STR_t const char *

#define buff_t /*-*/ void *
#define BUFF_t const void *

/*--------------------------------------------------------------------------------------------------------------------*/

__NYX_INLINE__ uint32_t nyx_read_u32_le(const uint8_t *buff)
{
    return ((uint32_t) buff[0] << 0)
           |
           ((uint32_t) buff[1] << 8)
           |
           ((uint32_t) buff[2] << 16)
           |
           ((uint32_t) buff[3] << 24)
    ;
}

/*--------------------------------------------------------------------------------------------------------------------*/

size_t nyx_memory_free(__NYX_NULLABLE__ buff_t buff);

buff_t nyx_memory_alloc(__NYX_ZEROABLE__ size_t size);

buff_t nyx_memory_realloc(__NYX_NULLABLE__ buff_t buff, __NYX_ZEROABLE__ size_t size);

/*--------------------------------------------------------------------------------------------------------------------*/

uint32_t nyx_hash32(__NYX_ZEROABLE__ size_t size, __NYX_NULLABLE__ BUFF_t buff, uint32_t seed);

/*--------------------------------------------------------------------------------------------------------------------*/

#endif /* NYX_STREAM_H */

/*--------------------------------------------------------------------------------------------------------------------*/
