/**
 * \file FlowConverter.hpp
 * \brief Zero-allocation IPFIX to Protobuf converter for hot path
 * \author Generated
 * \date 2026
 */

#ifndef PROTOBUF_KAFKA_FLOWCONVERTER_HPP
#define PROTOBUF_KAFKA_FLOWCONVERTER_HPP

#include "ProtoSchema.hpp"
#include "TranslationTable.hpp"
#include "Config.hpp"

#include <string>
#include <cstdint>

#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <libfds.h>

namespace protobuf_kafka {

/**
 * \brief Key for RSS partition computation
 */
struct PartitionKey {
    const uint8_t* src_ip = nullptr;
    size_t src_ip_len = 0;
    const uint8_t* dst_ip = nullptr;
    size_t dst_ip_len = 0;
    uint16_t src_port = 0;
    uint16_t dst_port = 0;
    uint8_t protocol = 0;
    bool valid = false;
};

/**
 * \brief Zero-allocation IPFIX to Protobuf flow converter
 *
 * This class is designed for the hot processing path. It:
 * - Reuses a single DynamicMessage instance (created once)
 * - Caches the Reflection pointer
 * - Uses a persistent serialization buffer
 * - Uses pre-resolved FieldDescriptor* from TranslationTable
 *
 * NO allocations occur in the convert() method.
 */
class FlowConverter {
public:
    /**
     * \brief Create a converter
     *
     * \param schema  Protobuf schema (must outlive this object)
     * \param table   Translation table (must outlive this object)
     * \param mode    Partition mode
     */
    FlowConverter(ProtoSchema& schema,
                  const TranslationTable& table,
                  PartitionMode mode);

    ~FlowConverter();

    // Non-copyable
    FlowConverter(const FlowConverter&) = delete;
    FlowConverter& operator=(const FlowConverter&) = delete;

    /**
     * \brief Convert an IPFIX record to serialized Protobuf
     *
     * This is the HOT PATH. Zero allocations occur here.
     *
     * \param[in]  rec            IPFIX data record
     * \param[out] out_data       Pointer to serialized data (valid until next call)
     * \param[out] out_len        Length of serialized data
     * \param[out] partition_key  Partition key for RSS (if mode is RSS)
     * \return true on success, false if conversion failed
     */
    bool convert(const fds_drec* rec,
                 const char** out_data,
                 size_t* out_len,
                 PartitionKey* partition_key);

private:
    const TranslationTable& m_table;
    PartitionMode m_partition_mode;

    // Pre-allocated, reused resources
    google::protobuf::Message* m_message;              ///< Reused message instance
    const google::protobuf::Reflection* m_reflection;  ///< Cached reflection
    std::string m_buffer;                               ///< Serialization buffer

    // Well-known IPFIX element IDs for RSS partitioning
    static constexpr uint32_t IANA_PEN = 0;
    static constexpr uint16_t ID_SRC_IPV4 = 8;    // sourceIPv4Address
    static constexpr uint16_t ID_DST_IPV4 = 12;   // destinationIPv4Address
    static constexpr uint16_t ID_SRC_IPV6 = 27;   // sourceIPv6Address
    static constexpr uint16_t ID_DST_IPV6 = 28;   // destinationIPv6Address
    static constexpr uint16_t ID_SRC_PORT = 7;    // sourceTransportPort
    static constexpr uint16_t ID_DST_PORT = 11;   // destinationTransportPort
    static constexpr uint16_t ID_PROTOCOL = 4;    // protocolIdentifier

    /**
     * \brief Set a protobuf field from IPFIX field data
     *
     * \param entry  Translation table entry with cached FieldDescriptor
     * \param data   Raw IPFIX field data
     * \param size   Size of data
     */
    void setField(const FieldEntry& entry, const uint8_t* data, size_t size);

    /**
     * \brief Extract partition key from IPFIX record
     */
    void extractPartitionKey(const fds_drec* rec, PartitionKey* key);
};

} // namespace protobuf_kafka

#endif // PROTOBUF_KAFKA_FLOWCONVERTER_HPP
