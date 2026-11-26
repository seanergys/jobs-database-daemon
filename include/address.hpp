/*
 * SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include <cstdint>
#include <string>

namespace hpc {

struct AddressParts {
    std::uint64_t upper{};
    std::uint64_t lower{};
};

AddressParts parse_address(const std::string& text);
std::string format_address(std::uint64_t upper, std::uint64_t lower);

} // namespace hpc
