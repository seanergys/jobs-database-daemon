/*
 * SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#include "database.hpp"

#include <seanerbus/client/connection.hpp>
#include <seanerbus/client/simple_event_loop.hpp>
#include <seanerbus/msg/message.hpp>
#include <seanerbus/msg/msg.capnp.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

constexpr const char* kFluxAddress = "834d23e1-fd27-4537-b467-9702ef9ca8a2";
constexpr std::uint64_t kFluxPayloadType =
    static_cast<std::uint64_t>(MessageTypes::FLUX_LIFECYCLE_EVENT_V2);

auto state_to_string(FluxLifecycleState state) -> std::string
{
    switch (state) {
        case FluxLifecycleState::DEPEND: return "depend";
        case FluxLifecycleState::PRIORITY: return "priority";
        case FluxLifecycleState::SCHED: return "sched";
        case FluxLifecycleState::RUN: return "run";
        case FluxLifecycleState::NEW: return "new";
        case FluxLifecycleState::CLEANUP: return "cleanup";
        case FluxLifecycleState::INACTIVE: return "inactive";
    }
    throw std::runtime_error("Unknown FluxLifecycleState");
}

auto finish_to_string(FluxFinishState state) -> std::string
{
    switch (state) {
        case FluxFinishState::COMPLETED: return "COMPLETED";
        case FluxFinishState::CANCELLED: return "CANCELLED";
        case FluxFinishState::FAILED: return "FAILED";
        case FluxFinishState::TIMEOUT: return "TIMEOUT";
    }
    throw std::runtime_error("Unknown FluxFinishState");
}

hpc::FluxJobDataV2 decode_flux_event(const seanerbus::msg::Message& message)
{
    auto rr = seanerbus::msg::MessageReader<FluxLifecycleEventV2>::from_msg(message);
    if (!rr.ok())
        throw std::runtime_error(
    	    std::string{"Invalid FluxLifecycleEventV2: "} +
    	    rr.unwrap_error().what()
        );

    const auto reader = rr.unwrap_ok().reader();
    hpc::FluxJobDataV2 d;

    d.job_id = reader.getJobid();
    d.jobid_f58 = reader.getJobidF58().cStr();
    d.state = state_to_string(reader.getState());
    d.prev_state = state_to_string(reader.getPrevState());

    d.nslots = reader.getNSlots();
    d.nnodes = reader.getNNodes();
    d.ngpus = reader.getNGpus();
    d.ncpus = reader.getNCpus();

    d.owner_id = reader.getOwner();
    d.owner_name = reader.getOwnerName().cStr();
    d.job_name = reader.getJobName().cStr();
    d.job_display_name = reader.getJobDisplayName().cStr();
    d.budget = reader.getBudget().cStr();
    d.queue = reader.getQueue().cStr();

    d.submit_time_us = reader.getSubmitTime();
    d.requested_walltime_us = reader.getRequestedWalltime();

    d.has_priority = d.state == "sched";
    if (d.has_priority)
        d.priority = reader.getPriority();

    if (reader.hasNodeList()) {
        const auto list = reader.getNodeList();
        d.node_list.reserve(list.size());
        for (auto node : list)
            d.node_list.emplace_back(node.cStr());
    }

    if (reader.hasRLite()) {
        const auto list = reader.getRLite();
        d.r_lite.reserve(list.size());
        for (auto node : list) {
            hpc::FluxLifecycleRLiteNodeData item;
            item.ranks = node.getRanks().cStr();
            item.child_cores = node.getChildCores().cStr();
            item.child_gpus = node.getChildGpus().cStr();
            d.r_lite.push_back(std::move(item));
        }
    }

    d.has_start_time = d.state == "run" || d.state == "cleanup" || d.state == "inactive";
    if (d.has_start_time)
        d.start_time_us = reader.getStartTime();

    d.has_finish_time = d.state == "cleanup" || d.state == "inactive";
    if (d.has_finish_time) {
        d.rc_state = finish_to_string(reader.getRcState());
        d.finish_time_us = reader.getFinishTime();
    }

    if (reader.hasErrObj()) {
        const auto error = reader.getErrObj();
        d.has_error = true;
        d.error_type = error.getType().cStr();
        d.error_severity = error.getSeverity();
        d.error_note = error.getNote().cStr();
        d.error_user_id = error.getUserId();
        d.error_wait_status = error.getWaitStatus();
    }

    return d;
}

void handle_message(hpc::Database& db,
                    const std::string& address,
                    const seanerbus::msg::Message& message)
{
    if (message.payload_type_as_int() != kFluxPayloadType) {
        std::cerr << "[flux V2 daemon] ignoring payload type "
                  << message.payload_type_to_str() << " (id="
                  << message.payload_type_as_int() << ")\n";
        return;
    }

    const auto parsed = decode_flux_event(message);
    const auto& bytes = message.payload();
    const std::vector<std::uint8_t> raw(
        reinterpret_cast<const std::uint8_t*>(bytes.data()),
        reinterpret_cast<const std::uint8_t*>(bytes.data()) + bytes.size());

    db.store_flux_event(parsed, address, kFluxPayloadType, raw);

    std::cout << "[flux V2 daemon] persisted job=" << parsed.job_id
              << " f58=" << parsed.jobid_f58
              << " " << parsed.prev_state << " -> " << parsed.state
              << " slots=" << parsed.nslots
              << " nodes=" << parsed.nnodes
              << " cpus=" << parsed.ncpus
              << " gpus=" << parsed.ngpus;
    if (parsed.has_priority) std::cout << " priority=" << parsed.priority;
    if (parsed.has_start_time) std::cout << " start_us=" << parsed.start_time_us;
    if (parsed.has_finish_time) std::cout << " rc=" << parsed.rc_state
                                          << " finish_us=" << parsed.finish_time_us;
    std::cout << " rlite=" << parsed.r_lite.size() << '\n';
}

} // namespace

int main()
{
    const char* env_db = std::getenv("HPC_JOB_DB");
    const std::string db_conn = env_db ? env_db :
        "host=hpc-postgres port=5432 dbname=hpcjobs_v2 user=hpc password=hpc";

    const char* env_pubsub = std::getenv("SEANERBUS_PUBSUB_ENDPOINT");
    const char* env_reqres = std::getenv("SEANERBUS_REQRES_ENDPOINT");
    const std::string pubsub_endpoint = env_pubsub ? env_pubsub : "demobus://localhost:5398";
    const std::string reqres_endpoint = env_reqres ? env_reqres : "demobus://localhost:5398";

    try {
        hpc::Database db(db_conn);
        db.initialize();
        db.subscribe(kFluxAddress);

        seanerbus::client::Connection conn(pubsub_endpoint, reqres_endpoint);
        const seanerbus::msg::Entity entity{
    	    seanerbus::msg::Uuid{kFluxAddress}
        };

        conn.subscribe(entity, [&db](auto& c, auto result) {
            if (!result.ok()) {
                std::cerr << "[flux V2 daemon] receive failed: "
                          << result.unwrap_error().what() << '\n';
                c.shutdown(seanerbus::Unexpected(result.unwrap_error()));
                return;
            }
            try {
                handle_message(db, kFluxAddress, result.unwrap_ok());
            } catch (const std::exception& e) {
                std::cerr << "[flux V2 daemon] rejected/persistence error: " << e.what() << '\n';
            }
        });

        std::cout << "[flux V2 daemon] subscribed to " << kFluxAddress << '\n';
        std::cout << "[flux V2 daemon] expecting FluxLifecycleEventV2 / MessageTypes ordinal "
                  << kFluxPayloadType << '\n';
        std::cout << "[flux V2 daemon] waiting for the V2 Flux jobtap publisher...\n";

        seanerbus::client::SimpleEventLoop(conn).run();
    } catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << '\n';
        return 1;
    }

    return 0;
}
