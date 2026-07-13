function(kv_enable_sanitizers target)
    if(NOT KV_ENABLE_SANITIZERS)
        return()
    endif()

    if(MSVC)
        message(FATAL_ERROR "KV_ENABLE_SANITIZERS is not supported with MSVC")
    endif()

    target_compile_options(${target} PUBLIC
        -fsanitize=${KV_SANITIZERS}
        -fno-omit-frame-pointer
    )
    target_link_options(${target} PUBLIC -fsanitize=${KV_SANITIZERS})
endfunction()
