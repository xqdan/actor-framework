/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2017                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#ifndef CAF_IO_REMOTE_GROUP_HPP
#define CAF_IO_REMOTE_GROUP_HPP

#include <string>
#include <cstdint>

#include "caf/actor_system.hpp"

#include "caf/io/middleman.hpp"

namespace caf {
namespace io {

inline expected<group> remote_group(actor_system& sys,
                                    const std::string& group_uri) {
  return sys.middleman().remote_group(group_uri);
}

inline expected<group> remote_group(actor_system& sys,
                                    const std::string& group_identifier,
                                    const std::string& host, uint16_t port) {
  return sys.middleman().remote_group(group_identifier, host, port);
}

} // namespace io
} // namespace caf

#endif // CAF_IO_REMOTE_GROUP_HPP
