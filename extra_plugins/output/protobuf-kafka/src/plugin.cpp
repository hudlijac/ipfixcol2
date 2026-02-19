/**
 * \file plugin.cpp
 * \brief IPFIXcol2 protobuf-kafka output plugin
 * \author Jaroslav Pesek
 * \date 2026
 *
 * This plugin serializes IPFIX flow records into Protocol Buffers and sends
 * them to a Kafka topic. It uses dynamic schema loading.
 */

#include <ipfixcol2.h>
#include <libfds.h>

#include <memory>
#include <stdexcept>
#include <vector>
#include <atomic>
#include <string>
#include <pthread.h>
#include <cstring>

#include "Config.hpp"
#include "ProtoSchema.hpp"
#include "TranslationTable.hpp"
#include "KafkaProducer.hpp"
#include "FlowConverter.hpp"

namespace {

struct PluginContext;

/**
 * \brief Worker thread context
 */
struct WorkerContext {
    PluginContext* owner = nullptr;
    pthread_t thread{};
    bool started = false;
    std::unique_ptr<protobuf_kafka::FlowConverter> converter;
};

static void stopWorkerPool(PluginContext& data) noexcept;
static void processRecord(PluginContext& data,
                          protobuf_kafka::FlowConverter& converter,
                          struct ipx_ipfix_record* rec);

/**
 * \brief Plugin instance context
 */
struct PluginContext {
    std::unique_ptr<protobuf_kafka::Config> config;
    std::unique_ptr<protobuf_kafka::ProtoSchema> schema;
    std::unique_ptr<protobuf_kafka::TranslationTable> table;
    std::unique_ptr<protobuf_kafka::KafkaProducer> kafka;
    std::unique_ptr<protobuf_kafka::FlowConverter> converter;

    uint32_t effective_workers = 1;
    std::vector<WorkerContext> workers;

    pthread_mutex_t work_lock = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t work_ready = PTHREAD_COND_INITIALIZER;
    pthread_cond_t work_done = PTHREAD_COND_INITIALIZER;

    bool stop_workers = false;
    uint64_t batch_generation = 0;
    uint32_t workers_completed = 0;
    std::atomic<uint32_t> next_record{0};
    ipx_msg_ipfix_t* current_msg = nullptr;
    uint32_t current_rec_cnt = 0;

    ~PluginContext()
    {
        stopWorkerPool(*this);
        pthread_cond_destroy(&work_done);
        pthread_cond_destroy(&work_ready);
        pthread_mutex_destroy(&work_lock);
    }
};

static int32_t
computePartition(const PluginContext& data, const protobuf_kafka::PartitionKey& key)
{
    if (data.config->partition_mode != protobuf_kafka::PartitionMode::RSS || !key.valid) {
        return RD_KAFKA_PARTITION_UA;
    }

    if (key.has_flow_id) {
        return protobuf_kafka::KafkaProducer::computeRssPartitionFromFlowId(
            key.flow_id,
            data.kafka->partitionCount());
    }

    return protobuf_kafka::KafkaProducer::computeRssPartition(
        key.src_ip, key.src_ip_len,
        key.dst_ip, key.dst_ip_len,
        key.src_port, key.dst_port,
        key.protocol,
        data.kafka->partitionCount());
}

static void
processRecord(PluginContext& data,
              protobuf_kafka::FlowConverter& converter,
              struct ipx_ipfix_record* rec)
{
    if (!rec || rec->rec.tmplt == nullptr || rec->rec.tmplt->type == FDS_TYPE_TEMPLATE_OPTS) {
        return;
    }

    const char* buf = nullptr;
    size_t len = 0;
    protobuf_kafka::PartitionKey key{};

    if (!converter.convert(&rec->rec, &buf, &len, &key)) {
        return;
    }

    data.kafka->produce(buf, len, computePartition(data, key));
}

static void*
workerMain(void* arg)
{
    auto* worker = static_cast<WorkerContext*>(arg);
    PluginContext& data = *worker->owner;

    uint64_t seen_generation = 0;
    while (true) {
        pthread_mutex_lock(&data.work_lock);
        while (!data.stop_workers && data.batch_generation == seen_generation) {
            pthread_cond_wait(&data.work_ready, &data.work_lock);
        }

        if (data.stop_workers) {
            pthread_mutex_unlock(&data.work_lock);
            break;
        }

        seen_generation = data.batch_generation;
        ipx_msg_ipfix_t* msg = data.current_msg;
        const uint32_t rec_cnt = data.current_rec_cnt;
        pthread_mutex_unlock(&data.work_lock);

        while (true) {
            const uint32_t rec_idx = data.next_record.fetch_add(1, std::memory_order_relaxed);
            if (rec_idx >= rec_cnt) {
                break;
            }

            struct ipx_ipfix_record* rec = ipx_msg_ipfix_get_drec(msg, rec_idx);
            processRecord(data, *worker->converter, rec);
        }

        pthread_mutex_lock(&data.work_lock);
        ++data.workers_completed;
        if (data.workers_completed >= data.effective_workers) {
            pthread_cond_signal(&data.work_done);
        }
        pthread_mutex_unlock(&data.work_lock);
    }

    return nullptr;
}

static void
startWorkerPool(PluginContext& data, ipx_ctx_t* ctx)
{
    if (data.effective_workers <= 1) {
        return;
    }

    data.workers.reserve(data.effective_workers);
    for (uint32_t idx = 0; idx < data.effective_workers; ++idx) {
        data.workers.emplace_back();
        WorkerContext& worker = data.workers.back();
        worker.owner = &data;
        worker.converter = std::make_unique<protobuf_kafka::FlowConverter>(
            *data.schema,
            *data.table,
            data.config->partition_mode);

        const int rc = pthread_create(&worker.thread, nullptr, &workerMain, &worker);
        if (rc != 0) {
            throw std::runtime_error(
                std::string("Failed to start worker thread: ") + std::strerror(rc));
        }
        worker.started = true;
    }

    IPX_CTX_INFO(ctx, "Parallel workers enabled: %u (parallel_min_records=%u)",
                 data.effective_workers,
                 data.config->parallel_min_records);
}

static void
stopWorkerPool(PluginContext& data) noexcept
{
    if (data.workers.empty()) {
        return;
    }

    pthread_mutex_lock(&data.work_lock);
    data.stop_workers = true;
    pthread_cond_broadcast(&data.work_ready);
    pthread_mutex_unlock(&data.work_lock);

    for (auto& worker : data.workers) {
        if (worker.started) {
            pthread_join(worker.thread, nullptr);
            worker.started = false;
        }
    }

    data.workers.clear();
    data.current_msg = nullptr;
    data.current_rec_cnt = 0;
}

static void
processParallelMessage(PluginContext& data, ipx_msg_ipfix_t* ipfix_msg, uint32_t rec_cnt)
{
    pthread_mutex_lock(&data.work_lock);
    data.current_msg = ipfix_msg;
    data.current_rec_cnt = rec_cnt;
    data.next_record.store(0, std::memory_order_relaxed);
    data.workers_completed = 0;
    ++data.batch_generation;

    pthread_cond_broadcast(&data.work_ready);
    while (data.workers_completed < data.effective_workers) {
        pthread_cond_wait(&data.work_done, &data.work_lock);
    }

    data.current_msg = nullptr;
    data.current_rec_cnt = 0;
    pthread_mutex_unlock(&data.work_lock);
}

} // anonymous namespace

/// Plugin identification structure
IPX_API struct ipx_plugin_info ipx_plugin_info = {
    // Plugin identification name
    "protobuf-kafka",
    // Brief description of plugin
    "Serializes IPFIX records to Protocol Buffers and sends to Kafka",
    // Plugin type
    IPX_PT_OUTPUT,
    // Configuration flags - use DEEPBIND for protobuf library compatibility
    IPX_PF_DEEPBIND,
    // Plugin version string
    "1.0.0",
    // Minimal IPFIXcol version string
    "2.2.0"
};

/**
 * \brief Plugin initialization
 */
extern "C" IPX_API int
ipx_plugin_init(ipx_ctx_t* ctx, const char* params)
{
    try {
        IPX_CTX_INFO(ctx, "Initializing protobuf-kafka output plugin...");

        auto data = std::make_unique<PluginContext>();

        const fds_iemgr_t* iemgr = ipx_ctx_iemgr_get(ctx);
        if (!iemgr) {
            IPX_CTX_ERROR(ctx, "Failed to get Information Element manager");
            return IPX_ERR_DENIED;
        }

        data->config = std::make_unique<protobuf_kafka::Config>(
            protobuf_kafka::parse_config(params, iemgr, ctx));
        data->effective_workers = data->config->workers;

        IPX_CTX_INFO(ctx, "Loading proto file: %s", data->config->proto_file.c_str());
        data->schema = std::make_unique<protobuf_kafka::ProtoSchema>(
            data->config->proto_file, data->config->message_type);

        data->table = std::make_unique<protobuf_kafka::TranslationTable>();
        data->table->build(data->config->mappings, *data->schema, iemgr, ctx);

        data->kafka = std::make_unique<protobuf_kafka::KafkaProducer>(
            *data->config, ctx);

        data->converter = std::make_unique<protobuf_kafka::FlowConverter>(
            *data->schema, *data->table, data->config->partition_mode);

        startWorkerPool(*data, ctx);
        if (data->effective_workers <= 1) {
            IPX_CTX_INFO(ctx, "Parallel workers disabled (workers=%u)",
                         data->effective_workers);
        }

        IPX_CTX_INFO(ctx, "Protobuf-kafka plugin initialized successfully");

        ipx_ctx_private_set(ctx, data.release());
        return IPX_OK;

    } catch (const std::exception& ex) {
        IPX_CTX_ERROR(ctx, "Initialization failed: %s", ex.what());
        return IPX_ERR_DENIED;
    } catch (...) {
        IPX_CTX_ERROR(ctx, "Initialization failed: unknown exception");
        return IPX_ERR_DENIED;
    }
}

/**
 * \brief Plugin destruction
 */
extern "C" IPX_API void
ipx_plugin_destroy(ipx_ctx_t* ctx, void* cfg)
{
    if (cfg == nullptr) {
        return;
    }

    auto* data = static_cast<PluginContext*>(cfg);
    delete data;

    IPX_CTX_INFO(ctx, "Protobuf-kafka plugin destroyed");
}

/**
 * \brief Process IPFIX message
 */
extern "C" IPX_API int
ipx_plugin_process(ipx_ctx_t* ctx, void* cfg, ipx_msg_t* msg)
{
    (void)ctx;

    auto* data = static_cast<PluginContext*>(cfg);
    ipx_msg_ipfix_t* ipfix_msg = ipx_msg_base2ipfix(msg);

    const uint32_t rec_cnt = ipx_msg_ipfix_get_drec_cnt(ipfix_msg);

    const bool use_parallel = data->effective_workers > 1 &&
        rec_cnt >= data->config->parallel_min_records;

    if (use_parallel) {
        processParallelMessage(*data, ipfix_msg, rec_cnt);
        return IPX_OK;
    }

    for (uint32_t i = 0; i < rec_cnt; ++i) {
        struct ipx_ipfix_record* rec = ipx_msg_ipfix_get_drec(ipfix_msg, i);
        processRecord(*data, *data->converter, rec);
    }

    return IPX_OK;
}
