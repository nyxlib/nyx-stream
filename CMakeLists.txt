########################################################################################################################

cmake_minimum_required(VERSION 3.5)

########################################################################################################################

project(nyx_node
    VERSION 1.0.0
    LANGUAGES C
)

########################################################################################################################

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_C_STANDARD_REQUIRED ON)

set(CMAKE_C_FLAGS_DEBUG "-O0 -g")
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")

add_compile_options(-D_GNU_SOURCE -DMG_ENABLE_LOG=0 -DMG_ENABLE_DIRLIST=0 -DMG_ENABLE_POLL=0 -DMG_ENABLE_EPOLL=1 -DMG_ENABLE_SSI=0 -Wall -Wextra -Wconversion -Wdouble-promotion -Wno-unknown-pragmas -Wno-unused-function)

########################################################################################################################
# LIBS                                                                                                                 #
########################################################################################################################

include(CheckFunctionExists)

check_function_exists(malloc_size HAVE_MALLOC_SIZE)

check_function_exists(malloc_usable_size HAVE_MALLOC_USABLE_SIZE)

########################################################################################################################

set(SOURCE_FILES
    src/nyx-stream.h
    #
    src/external/mongoose.c
    #
    src/hash.c
    src/memory.c
    src/nyx-stream.c
)

########################################################################################################################

add_executable(nyx-stream-exec ${SOURCE_FILES})

if(HAVE_MALLOC_SIZE)
    target_compile_definitions(nyx-stream-exec PRIVATE HAVE_MALLOC_SIZE)
endif()

if(HAVE_MALLOC_USABLE_SIZE)
    target_compile_definitions(nyx-stream-exec PRIVATE HAVE_MALLOC_USABLE_SIZE)
endif()

set_target_properties(nyx-stream-exec PROPERTIES
    OUTPUT_NAME "nyx-stream"
)

########################################################################################################################
# INSTALLATION                                                                                                         #
########################################################################################################################

install(TARGETS nyx-stream-exec
    RUNTIME DESTINATION bin
)

########################################################################################################################
