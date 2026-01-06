/**
 * \file plugin.cpp
 * \brief IPFIXcol2 protobuf-kafka output plugin
 * \author Generated
 * \date 2026
 *
 * This plugin serializes IPFIX flow records into Protocol Buffers and sends
 * them to a Kafka topic. It uses dynamic schema loading (no compile-time
 * .proto generation) and is designed for resource-aware, high-throughput
 * processing with zero allocations in the hot path.
 */

#include <ipfixcol2.h>
#include <libfds.h>

#include <memory>
#include <stdexcept>

#include "Config.hpp"
#include "ProtoSchema.hpp"
#include "TranslationTable.hpp"
#include "KafkaProducer.hpp"
#include "FlowConverter.hpp"

namespace {

/**
 * \brief Plugin instance context
 */
struct PluginContext {
    std::unique_ptr<protobuf_kafka::Config> config;
    std::unique_ptr<protobuf_kafka::ProtoSchema> schema;
    std::unique_ptr<protobuf_kafka::TranslationTable> table;
    std::unique_ptr<protobuf_kafka::KafkaProducer> kafka;
    std::unique_ptr<protobuf_kafka::FlowConverter> converter;
};

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

        // Get IE manager for resolving IPFIX element names
        const fds_iemgr_t* iemgr = ipx_ctx_iemgr_get(ctx);
        if (!iemgr) {
            IPX_CTX_ERROR(ctx, "Failed to get Information Element manager");
            return IPX_ERR_DENIED;
        }

        // Parse configuration
        data->config = std::make_unique<protobuf_kafka::Config>(
            protobuf_kafka::parse_config(params, iemgr, ctx));

        // Load protobuf schema
        IPX_CTX_INFO(ctx, "Loading proto file: %s", data->config->proto_file.c_str());
        data->schema = std::make_unique<protobuf_kafka::ProtoSchema>(
            data->config->proto_file, data->config->message_type);

        // Build translation table
        data->table = std::make_unique<protobuf_kafka::TranslationTable>();
        data->table->build(data->config->mappings, *data->schema, iemgr, ctx);

        // Initialize Kafka producer
        data->kafka = std::make_unique<protobuf_kafka::KafkaProducer>(
            *data->config, ctx);

        // Create flow converter
        data->converter = std::make_unique<protobuf_kafka::FlowConverter>(
            *data->schema, *data->table, data->config->partition_mode);

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
    (void)ctx;

    auto* data = static_cast<PluginContext*>(cfg);
    delete data;

    IPX_CTX_INFO(ctx, "Protobuf-kafka plugin destroyed");
}

/**
 * \brief Process IPFIX message (HOT PATH)
 *
 * This function is called for each IPFIX message. It iterates through
 * all data records, converts them to Protobuf, and sends to Kafka.
 *
 * Design: Zero allocations in this function. All objects are reused.
 */
extern "C" IPX_API int
ipx_plugin_process(ipx_ctx_t* ctx, void* cfg, ipx_msg_t* msg)
{
    (void)ctx;

    auto* data = static_cast<PluginContext*>(cfg);
    ipx_msg_ipfix_t* ipfix_msg = ipx_msg_base2ipfix(msg);

    const uint32_t rec_cnt = ipx_msg_ipfix_get_drec_cnt(ipfix_msg);

    for (uint32_t i = 0; i < rec_cnt; ++i) {
        struct ipx_ipfix_record* rec = ipx_msg_ipfix_get_drec(ipfix_msg, i);

        // Skip Options Template records
        if (rec->rec.tmplt->type == FDS_TYPE_TEMPLATE_OPTS) {
            continue;
        }

        const char* buf = nullptr;
        size_t len = 0;
        protobuf_kafka::PartitionKey pk{};

        // Convert record to Protobuf
        if (!data->converter->convert(&rec->rec, &buf, &len, &pk)) {
            // Conversion failed - skip this record
            continue;
        }

        // Determine partition
        int32_t partition = RD_KAFKA_PARTITION_UA;
        if (data->config->partition_mode == protobuf_kafka::PartitionMode::RSS && pk.valid) {
            partition = protobuf_kafka::KafkaProducer::computeRssPartition(
                pk.src_ip, pk.src_ip_len,
                pk.dst_ip, pk.dst_ip_len,
                pk.src_port, pk.dst_port,
                pk.protocol,
                data->kafka->partitionCount());
        }

        // Send to Kafka (copies data internally)
        data->kafka->produce(buf, len, partition);
    }

    return IPX_OK;
}
