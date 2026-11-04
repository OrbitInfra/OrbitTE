# Configure the parent TE's public CPU header as the frozen project does.
cmake_minimum_required(VERSION 3.16)
set(te "${REFERENCE}/mooncake-transfer-engine")
file(MAKE_DIRECTORY "${OUTPUT}")
configure_file("${te}/include/mooncake_te_build_config.h.in"
               "${OUTPUT}/mooncake_te_build_config.h" @ONLY)
execute_process(COMMAND c++ -std=c++20 -DUSE_TENT -DENABLE_MULTI_PROTOCOL -fsyntax-only
    "-I${te}/include" "-I${te}/tent/include" "-I${REFERENCE}/mooncake-common/include"
    "-I${OUTPUT}" -I/usr/include/jsoncpp "${CMAKE_CURRENT_LIST_DIR}/frozen_contract.cpp"
    COMMAND_ERROR_IS_FATAL ANY)
