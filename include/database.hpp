/*
 * SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
 *
 * SPDX-License-Identifier: LGPL-3.0-only
 */
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace hpc {

struct FluxLifecycleRLiteNodeData {
    std::string ranks;
    std::string child_cores;
    std::string child_gpus;
};

struct FluxJobDataV2 {
    std::uint64_t job_id{};
    std::string jobid_f58;

    std::string state;
    std::string prev_state;

    std::uint64_t nslots{};
    std::uint64_t nnodes{};
    std::uint64_t ngpus{};
    std::uint64_t ncpus{};

    std::uint64_t owner_id{};
    std::string owner_name;
    std::string job_name;
    std::string job_display_name;
    std::string budget;
    std::string queue;

    std::uint64_t submit_time_us{};
    std::uint64_t requested_walltime_us{};

    std::uint64_t priority{};
    bool has_priority{false};

    std::vector<std::string> node_list;
    std::vector<FluxLifecycleRLiteNodeData> r_lite;

    std::uint64_t start_time_us{};
    bool has_start_time{false};

    std::string rc_state;
    std::uint64_t finish_time_us{};
    bool has_finish_time{false};

    std::string error_type;
    std::uint64_t error_severity{};
    std::string error_note;
    std::uint64_t error_user_id{};
    std::uint64_t error_wait_status{};
    bool has_error{false};
};

class Database {
public:
    explicit Database(const std::string& conninfo);
    ~Database();

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    void initialize(const std::string& sql_file = "sql/001_schema.sql");

    void store_flux_event(const FluxJobDataV2& data,
                          const std::string& address,
                          std::uint64_t payload_type_id,
                          const std::vector<std::uint8_t>& raw_capnp_payload);

    void subscribe(const std::string& address);
    void print_recent_messages(const std::string& address, int limit = 20) const;
    void print_job(std::uint64_t job_id) const;

private:
    struct Impl;
    Impl* impl_;
};

} // namespace hpc
