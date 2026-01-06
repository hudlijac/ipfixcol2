/**
 * \file Config.hpp
 * \brief Configuration parser for protobuf-kafka output plugin
 * \author Generated
 * \date 2026
 */

#ifndef PROTOBUF_KAFKA_CONFIG_HPP
#define PROTOBUF_KAFKA_CONFIG_HPP

#include <string>
#include <vector>
#include <cstdint>

#include <ipfixcol2.h>
#include <libfds.h>

namespace protobuf_kafka {

/**
 * \brief Partition mode for Kafka producer
 */
enum class PartitionMode {
    RANDOM,  ///< Random partition assignment (RD_KAFKA_PARTITION_UA)
    RSS      ///< Receiver-side scaling based on 5-tuple hash
};

/**
 * \brief Mapping between IPFIX field and Protobuf field
 */
struct FieldMapping {
    uint32_t ipfix_pen;      ///< IPFIX Private Enterprise Number
    uint16_t ipfix_id;       ///< IPFIX Information Element ID
    std::string proto_name;  ///< Protobuf field name
    std::string ipfix_spec;  ///< Original IPFIX specification (for error messages)
};

/**
 * \brief Plugin configuration
 */
struct Config {
    // Kafka configuration
    std::string brokers;                  ///< Kafka broker list (comma-separated)
    std::string topic;                    ///< Kafka topic name
    PartitionMode partition_mode;         ///< Partition assignment mode
    uint32_t batch_size = 10000;          ///< batch.num.messages
    uint32_t linger_ms = 100;             ///< queue.buffering.max.ms
    std::string compression = "lz4";      ///< compression.codec
    bool blocking = false;                ///< Block when queue is full

    // Protobuf configuration
    std::string proto_file;               ///< Path to .proto file
    std::string message_type;             ///< Fully qualified message type name

    // Field mappings
    std::vector<FieldMapping> mappings;   ///< IPFIX to Protobuf field mappings
};

/**
 * \brief Parse XML configuration
 *
 * \param[in] params  XML configuration string
 * \param[in] iemgr   Information Element manager for resolving IPFIX names
 * \param[in] ctx     Plugin context for logging
 * \return Parsed configuration
 * \throws std::runtime_error on parse error
 */
Config
parse_config(const char* params, const fds_iemgr_t* iemgr, ipx_ctx_t* ctx);

} // namespace protobuf_kafka

#endif // PROTOBUF_KAFKA_CONFIG_HPP
