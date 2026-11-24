cmake_minimum_required(VERSION 3.16)

# Link the single source of truth shared across Node and Gateway
set(EXTRA_COMPONENT_DIRS "../../shared_components")

include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(BhumiPulse_Node)