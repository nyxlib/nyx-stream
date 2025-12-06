/* NyxStream
 * Author: Jérôme ODIER <jerome.odier@lpsc.in2p3.fr>
 * SPDX-License-Identifier: GPL-2.0-only
 */

/*--------------------------------------------------------------------------------------------------------------------*/

#include <stdlib.h>

#include "nyx-stream.h"

#include "external/mongoose.h"

/*--------------------------------------------------------------------------------------------------------------------*/

#ifdef HAVE_MALLOC_SIZE
size_t malloc_size(void *);
#endif

#ifdef HAVE_MALLOC_USABLE_SIZE
size_t malloc_usable_size(void *);
#endif

/*--------------------------------------------------------------------------------------------------------------------*/

size_t nyx_memory_free(buff_t buff)
{
    if(buff == NULL)
    {
        return 0x00;
    }

    /*----------------------------------------------------------------------------------------------------------------*/

#ifdef HAVE_MALLOC_SIZE
    size_t result = malloc_size(buff);
#endif

#ifdef HAVE_MALLOC_USABLE_SIZE
    size_t result = malloc_usable_size(buff);
#endif

    /*----------------------------------------------------------------------------------------------------------------*/

    free(buff);

    /*----------------------------------------------------------------------------------------------------------------*/

#if defined(HAVE_MALLOC_SIZE) || defined(HAVE_MALLOC_USABLE_SIZE)
    return result;
#else
    return 0x0000;
#endif
}

/*--------------------------------------------------------------------------------------------------------------------*/

buff_t nyx_memory_alloc(size_t size)
{
    if(size == 0x00)
    {
        return NULL;
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    buff_t result = malloc(size);

    if(result == NULL)
    {
        MG_ERROR(("Out of memory"));

        exit(1);
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    return result;
}

/*--------------------------------------------------------------------------------------------------------------------*/

buff_t nyx_memory_realloc(buff_t buff, size_t size)
{
    if(buff == NULL) {
        return nyx_memory_alloc(size);
    }

    if(size == 0x00) {
        nyx_memory_free(buff); return NULL;
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    buff_t result = realloc(buff, size);

    if(result == NULL)
    {
        MG_ERROR(("Out of memory"));

        exit(1);
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    return result;
}

/*--------------------------------------------------------------------------------------------------------------------*/
