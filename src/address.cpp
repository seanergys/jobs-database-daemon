/*
 * SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
 *
 * SPDX-License-Identifier: Apache-2.0
 */
 #include "address.hpp"

#include <seanerbus/msg/message.hpp>

namespace hpc {

AddressParts parse_address(const std::string& text)
{
    const seanerbus::msg::Uuid uuid{text};
    return {uuid.upper, uuid.lower};
}

std::string format_address(std::uint64_t upper, std::uint64_t lower)
{
    return seanerbus::msg::Uuid{upper, lower}.as_string();
}

} // namespace hpc
