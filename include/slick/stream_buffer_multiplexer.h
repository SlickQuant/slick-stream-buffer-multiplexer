/********************************************************************************
 * Copyright (c) 2026 Slick Quant LLC
 * All rights reserved
 *
 * This file is part of the slick-stream-buffer-multiplexer. Redistribution and 
 * use in source and binary forms, with or without modification, are permitted
 * exclusively under the terms of the MIT license which is available at
 * https://github.com/SlickQuant/slick-stream-buffer-multiplexer/blob/main/LICENSE
 *
 ********************************************************************************/

#pragma once

// Backward-compatibility shim. The canonical header is <slick/stream_buffer_multiplexer.hpp>.
#pragma once
#ifdef _MSC_VER
#  pragma message("warning: <slick/stream_buffer_multiplexer.h> is deprecated; use <slick/stream_buffer_multiplexer.hpp>")
#else
#  warning "<slick/stream_buffer_multiplexer.h> is deprecated; use <slick/stream_buffer_multiplexer.hpp>"
#endif
#include "stream_buffer_multiplexer.hpp"
