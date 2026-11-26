/*
 * SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#include "database.hpp"
#include "address.hpp"

#include <libpq-fe.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

struct PGResult {
    PGresult* p{nullptr};
    ~PGResult() { if (p) PQclear(p); }
    PGresult* get() const { return p; }
};

void check(PGconn* c, PGresult* r)
{
    if (!r || (PQresultStatus(r) != PGRES_COMMAND_OK && PQresultStatus(r) != PGRES_TUPLES_OK)) {
        throw std::runtime_error(
            std::string("PostgreSQL error: ") + (r ? PQresultErrorMessage(r) : PQerrorMessage(c)));
    }
}

std::string read_sql(const std::string& path)
{
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Cannot open SQL file: " + path);
    std::ostringstream s;
    s << in.rdbuf();
    return s.str();
}

std::string u64(std::uint64_t value) { return std::to_string(value); }

std::string to_pg_array(const std::vector<std::string>& values)
{
    std::string out{"{"};
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (i) out += ',';
        out += '"';
        for (char c : values[i]) {
            if (c == '"' || c == '\\') out += '\\';
            out += c;
        }
        out += '"';
    }
    out += '}';
    return out;
}

std::string rlite_to_json(const std::vector<hpc::FluxLifecycleRLiteNodeData>& values)
{
    std::ostringstream out;
    out << '[';
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (i) out << ',';
        auto escape = [](const std::string& in) {
            std::string r;
            r.reserve(in.size() + 8);
            for (char c : in) {
                switch (c) {
                    case '"': r += "\\\""; break;
                    case '\\': r += "\\\\"; break;
                    case '\n': r += "\\n"; break;
                    case '\r': r += "\\r"; break;
                    case '\t': r += "\\t"; break;
                    default: r += c; break;
                }
            }
            return r;
        };
        out << "{\"ranks\":\"" << escape(values[i].ranks)
            << "\",\"childCores\":\"" << escape(values[i].child_cores)
            << "\",\"childGpus\":\"" << escape(values[i].child_gpus) << "\"}";
    }
    out << ']';
    return out.str();
}

std::string normalized_state(const hpc::FluxJobDataV2& d)
{
    if (d.has_finish_time) return d.rc_state;
    if (d.state == "depend") return "SUBMITTED";
    if (d.state == "priority" || d.state == "sched") return "PENDING";
    if (d.state == "run") return "RUNNING";
    return d.state;
}

std::uint64_t event_ts_ns(const hpc::FluxJobDataV2& d)
{
    const auto to_ns = [](std::uint64_t value_us) {
        if (value_us > UINT64_MAX / 1000ULL)
            throw std::overflow_error("microsecond timestamp overflows nanoseconds");
        return value_us * 1000ULL;
    };
    if (d.has_finish_time) return to_ns(d.finish_time_us);
    if (d.has_start_time) return to_ns(d.start_time_us);
    return to_ns(d.submit_time_us);
}

} // namespace

namespace hpc {

struct Database::Impl {
    PGconn* conn{nullptr};
    explicit Impl(const std::string& conninfo)
    {
        conn = PQconnectdb(conninfo.c_str());
        if (!conn || PQstatus(conn) != CONNECTION_OK) {
            const std::string message = conn ? PQerrorMessage(conn) : "PQconnectdb failed";
            if (conn) PQfinish(conn);
            throw std::runtime_error(message);
        }
    }
    ~Impl() { if (conn) PQfinish(conn); }
};

Database::Database(const std::string& conninfo) : impl_(new Impl(conninfo)) {}
Database::~Database() { delete impl_; }

void Database::initialize(const std::string& path)
{
    PGResult result{PQexec(impl_->conn, read_sql(path).c_str())};
    check(impl_->conn, result.get());
}

void Database::store_flux_event(const FluxJobDataV2& d,
                                const std::string& address,
                                std::uint64_t payload_type_id,
                                const std::vector<std::uint8_t>& raw)
{
    const auto a = parse_address(address);
    const std::string au = u64(a.upper);
    const std::string al = u64(a.lower);
    const std::string jid = u64(d.job_id);
    const std::string tid = u64(payload_type_id);
    const std::uint64_t timestamp_ns_value = event_ts_ns(d);
    const std::string ts = std::to_string(timestamp_ns_value);
    const std::string normalized = normalized_state(d);
    const std::string reason = d.has_error ? d.error_type + ": " + d.error_note : d.state;
    const std::string nodes = to_pg_array(d.node_list);
    const std::string rlite = rlite_to_json(d.r_lite);

    PGResult begin{PQexec(impl_->conn, "BEGIN")};
    check(impl_->conn, begin.get());
    try {
        const char* msql = R"SQL(
INSERT INTO messages(
 address_upper,address_lower,timestamp_ns,message_type,payload_type_id,job_id,capnp_payload)
VALUES($1::numeric,$2::numeric,$3::bigint,'FluxLifecycleEventV2',$4::numeric,$5::bigint,$6)
RETURNING message_id)SQL";

        const char* mv[6] = {
            au.c_str(), al.c_str(), ts.c_str(), tid.c_str(), jid.c_str(),
            reinterpret_cast<const char*>(raw.data())
        };
        int ml[6]{};
        int mf[6]{};
        ml[5] = static_cast<int>(raw.size());
        mf[5] = 1;
        PGResult mr{PQexecParams(impl_->conn, msql, 6, nullptr, mv, ml, mf, 0)};
        check(impl_->conn, mr.get());
        const std::string mid = PQgetvalue(mr.get(), 0, 0);

        const std::string priority = d.has_priority ? u64(d.priority) : "";
        const std::string start = d.has_start_time ? u64(d.start_time_us) : "";
        const std::string finish = d.has_finish_time ? u64(d.finish_time_us) : "";
        const std::string error_type = d.has_error ? d.error_type : "";
        const std::string error_sev = d.has_error ? u64(d.error_severity) : "";
        const std::string error_note = d.has_error ? d.error_note : "";
        const std::string error_uid = d.has_error ? u64(d.error_user_id) : "";
        const std::string error_wait = d.has_error ? u64(d.error_wait_status) : "";

        const char* jsql = R"SQL(
INSERT INTO jobs(
 job_id,jobid_f58,state,prev_state,owner_id,owner_name,job_name,job_display_name,budget,queue,
 submit_time_us,requested_walltime_us,nslots,nnodes,ncpus,ngpus,priority,node_list,r_lite,start_time_us,
 finish_time_us,rc_state,error_type,error_severity,error_note,error_user_id,error_wait_status,
 current_state,last_event_ts_ns,last_message_id,last_capnp_payload)
VALUES(
 $1::bigint,$2,$3,$4,$5::bigint,$6,$7,$8,$9,$10,
 $11::bigint,$12::bigint,$13::bigint,$14::bigint,$15::bigint,$16::bigint,NULLIF($17,'')::bigint,
 NULLIF($18,'')::text[],NULLIF($19,'')::jsonb,NULLIF($20,'')::bigint,
 NULLIF($21,'')::bigint,NULLIF($22,''),NULLIF($23,''),NULLIF($24,'')::bigint,NULLIF($25,''),
 NULLIF($26,'')::bigint,NULLIF($27,'')::bigint,
 $28,$29::bigint,$30::bigint,$31)
ON CONFLICT(job_id) DO UPDATE SET
 jobid_f58=EXCLUDED.jobid_f58,state=EXCLUDED.state,prev_state=EXCLUDED.prev_state,
 owner_id=EXCLUDED.owner_id,owner_name=EXCLUDED.owner_name,job_name=EXCLUDED.job_name,
 job_display_name=EXCLUDED.job_display_name,budget=EXCLUDED.budget,queue=EXCLUDED.queue,
 submit_time_us=EXCLUDED.submit_time_us,requested_walltime_us=EXCLUDED.requested_walltime_us,
 nslots=EXCLUDED.nslots,nnodes=EXCLUDED.nnodes,ncpus=EXCLUDED.ncpus,ngpus=EXCLUDED.ngpus,
 priority=COALESCE(EXCLUDED.priority,jobs.priority),
 node_list=CASE WHEN EXCLUDED.node_list IS NOT NULL THEN EXCLUDED.node_list ELSE jobs.node_list END,
 r_lite=CASE WHEN EXCLUDED.r_lite IS NOT NULL THEN EXCLUDED.r_lite ELSE jobs.r_lite END,
 start_time_us=COALESCE(EXCLUDED.start_time_us,jobs.start_time_us),
 finish_time_us=COALESCE(EXCLUDED.finish_time_us,jobs.finish_time_us),
 rc_state=COALESCE(EXCLUDED.rc_state,jobs.rc_state),
 error_type=COALESCE(EXCLUDED.error_type,jobs.error_type),
 error_severity=COALESCE(EXCLUDED.error_severity,jobs.error_severity),
 error_note=COALESCE(EXCLUDED.error_note,jobs.error_note),
 error_user_id=COALESCE(EXCLUDED.error_user_id,jobs.error_user_id),
 error_wait_status=COALESCE(EXCLUDED.error_wait_status,jobs.error_wait_status),
 current_state=EXCLUDED.current_state,last_event_ts_ns=EXCLUDED.last_event_ts_ns,
 last_message_id=EXCLUDED.last_message_id,last_capnp_payload=EXCLUDED.last_capnp_payload,
 updated_at=clock_timestamp()
WHERE (EXCLUDED.last_event_ts_ns,EXCLUDED.last_message_id) >= (jobs.last_event_ts_ns,COALESCE(jobs.last_message_id,0)) )SQL";

        const std::string owner = u64(d.owner_id);
        const std::string submit_us = u64(d.submit_time_us);
        const std::string wall_us = u64(d.requested_walltime_us);
        const std::string slots = u64(d.nslots);
        const std::string nnodes = u64(d.nnodes);
        const std::string ncpus = u64(d.ncpus);
        const std::string ngpus = u64(d.ngpus);

        const char* jv[31] = {
            jid.c_str(), d.jobid_f58.c_str(), d.state.c_str(), d.prev_state.c_str(), owner.c_str(),
            d.owner_name.c_str(), d.job_name.c_str(), d.job_display_name.c_str(), d.budget.c_str(), d.queue.c_str(),
            submit_us.c_str(), wall_us.c_str(), slots.c_str(), nnodes.c_str(), ncpus.c_str(), ngpus.c_str(),
            priority.c_str(), nodes.c_str(), rlite.c_str(), start.c_str(), finish.c_str(), d.rc_state.c_str(),
            error_type.c_str(), error_sev.c_str(), error_note.c_str(), error_uid.c_str(), error_wait.c_str(),
            normalized.c_str(), ts.c_str(), mid.c_str(), reinterpret_cast<const char*>(raw.data())
        };
        int jl[31]{};
        int jf[31]{};
        jl[30] = static_cast<int>(raw.size());
        jf[30] = 1;
        PGResult jr{PQexecParams(impl_->conn, jsql, 31, nullptr, jv, jl, jf, 0)};
        check(impl_->conn, jr.get());

        const char* esql = R"SQL(
INSERT INTO job_events(
 message_id,job_id,event_id,event_ts_ns,flux_state,flux_prev_state,owner_id,owner_name,job_name,job_display_name,
 budget,queue,submit_time_us,requested_walltime_us,nslots,nnodes,ncpus,ngpus,priority,node_list,r_lite,start_time_us,
 finish_time_us,rc_state,error_type,error_severity,error_note,error_user_id,error_wait_status,normalized_state,reason)
VALUES(
 $1::bigint,$2::bigint,$3::bigint,$4::bigint,$5,$6,$7::bigint,$8,$9,$10,
 $11,$12,$13::bigint,$14::bigint,$15::bigint,$16::bigint,$17::bigint,$18::bigint,NULLIF($19,'')::bigint,
 NULLIF($20,'')::text[],NULLIF($21,'')::jsonb,NULLIF($22,'')::bigint,
 NULLIF($23,'')::bigint,NULLIF($24,''),NULLIF($25,''),NULLIF($26,'')::bigint,NULLIF($27,''),
 NULLIF($28,'')::bigint,NULLIF($29,'')::bigint,$30,$31)
ON CONFLICT(job_id,event_id) DO NOTHING)SQL";

        const char* ev[31] = {
            mid.c_str(), jid.c_str(), mid.c_str(), ts.c_str(), d.state.c_str(), d.prev_state.c_str(), owner.c_str(),
            d.owner_name.c_str(), d.job_name.c_str(), d.job_display_name.c_str(), d.budget.c_str(), d.queue.c_str(),
            submit_us.c_str(), wall_us.c_str(), slots.c_str(), nnodes.c_str(), ncpus.c_str(), ngpus.c_str(), priority.c_str(),
            nodes.c_str(), rlite.c_str(), start.c_str(), finish.c_str(), d.rc_state.c_str(), error_type.c_str(),
            error_sev.c_str(), error_note.c_str(), error_uid.c_str(), error_wait.c_str(), normalized.c_str(), reason.c_str()
        };
        PGResult er{PQexecParams(impl_->conn, esql, 31, nullptr, ev, nullptr, nullptr, 0)};
        check(impl_->conn, er.get());

        PGResult commit{PQexec(impl_->conn, "COMMIT")};
        check(impl_->conn, commit.get());
    } catch (...) {
        PGResult rollback{PQexec(impl_->conn, "ROLLBACK")};
        (void)rollback;
        throw;
    }
}

void Database::subscribe(const std::string& address)
{
    const auto a = parse_address(address);
    const std::string u = u64(a.upper);
    const std::string l = u64(a.lower);
    const char* values[2] = {u.c_str(), l.c_str()};
    const char* sql = "INSERT INTO subscriptions(address_upper,address_lower) VALUES($1::numeric,$2::numeric) ON CONFLICT(address_upper,address_lower) DO UPDATE SET enabled=TRUE";
    PGResult r{PQexecParams(impl_->conn, sql, 2, nullptr, values, nullptr, nullptr, 0)};
    check(impl_->conn, r.get());
}

void Database::print_recent_messages(const std::string& address, int limit) const
{
    const auto a = parse_address(address);
    const std::string u = u64(a.upper);
    const std::string l = u64(a.lower);
    const std::string lim = std::to_string(limit);
    const char* values[3] = {u.c_str(), l.c_str(), lim.c_str()};
    const char* sql = "SELECT message_id,job_id,timestamp_ns,message_type,payload_type_id,octet_length(capnp_payload) FROM messages WHERE address_upper=$1::numeric AND address_lower=$2::numeric ORDER BY message_id DESC LIMIT $3::integer";
    PGResult r{PQexecParams(impl_->conn, sql, 3, nullptr, values, nullptr, nullptr, 0)};
    check(impl_->conn, r.get());
    for (int i = 0; i < PQntuples(r.get()); ++i)
        std::cout << PQgetvalue(r.get(), i, 0) << " job=" << PQgetvalue(r.get(), i, 1)
                  << " ts_ns=" << PQgetvalue(r.get(), i, 2)
                  << " type=" << PQgetvalue(r.get(), i, 3)
                  << " type_id=" << PQgetvalue(r.get(), i, 4)
                  << " bytes=" << PQgetvalue(r.get(), i, 5) << '\n';
}

void Database::print_job(std::uint64_t job_id) const
{
    const std::string id = u64(job_id);
    const char* values[1] = {id.c_str()};
    const char* sql = "SELECT job_id,jobid_f58,state,prev_state,current_state,rc_state,last_message_id FROM jobs WHERE job_id=$1::bigint";
    PGResult r{PQexecParams(impl_->conn, sql, 1, nullptr, values, nullptr, nullptr, 0)};
    check(impl_->conn, r.get());
    if (PQntuples(r.get()) == 0) {
        std::cout << "Job " << job_id << " not found\n";
        return;
    }
    for (int i = 0; i < PQnfields(r.get()); ++i)
        std::cout << PQfname(r.get(), i) << ": " << PQgetvalue(r.get(), 0, i) << '\n';
}

} // namespace hpc
