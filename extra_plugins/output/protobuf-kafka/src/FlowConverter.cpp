/**
 * \file FlowConverter.cpp
 * \brief Zero-allocation IPFIX to Protobuf converter for hot path
 * \author Generated
 * \date 2026
 */

#include "FlowConverter.hpp"

#include <cstring>
#include <arpa/inet.h>

namespace protobuf_kafka {

FlowConverter::FlowConverter(ProtoSchema& schema,
                             const TranslationTable& table,
                             PartitionMode mode)
    : m_table(table)
    , m_partition_mode(mode)
    , m_message(nullptr)
    , m_reflection(nullptr)
{
    // Create the message prototype once - this is the ONLY allocation
    m_message = schema.createMessage();
    m_reflection = m_message->GetReflection();

    // Pre-reserve buffer to avoid reallocations during hot path
    m_buffer.reserve(4096);
}

FlowConverter::~FlowConverter()
{
    delete m_message;
}

bool
FlowConverter::convert(const fds_drec* rec,
                       const char** out_data,
                       size_t* out_len,
                       PartitionKey* partition_key)
{
    // Clear message for reuse - NO allocation
    m_message->Clear();

    // Extract partition key if needed
    if (m_partition_mode == PartitionMode::RSS && partition_key) {
        extractPartitionKey(rec, partition_key);
    }

    // Iterate through configured field mappings
    const auto& ipfix_ids = m_table.ipfixIds();
    const auto& entries = m_table.entries();

    for (size_t i = 0; i < ipfix_ids.size(); ++i) {
        const auto& [pen, id] = ipfix_ids[i];
        const FieldEntry& entry = entries[i];

        // Find field in IPFIX record
        struct fds_drec_field field;
        if (fds_drec_find(const_cast<fds_drec*>(rec), pen, id, &field) == FDS_EOC) {
            // Field not present in this record - skip
            continue;
        }

        // Set the protobuf field
        setField(entry, field.data, field.size);
    }

    // Serialize to reusable buffer - NO allocation (buffer is pre-reserved)
    m_buffer.clear();
    if (!m_message->SerializeToString(&m_buffer)) {
        return false;
    }

    *out_data = m_buffer.data();
    *out_len = m_buffer.size();
    return true;
}

void
FlowConverter::setField(const FieldEntry& entry, const uint8_t* data, size_t size)
{
    const google::protobuf::FieldDescriptor* fd = entry.fd;

    switch (fd->type()) {
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
        if (size == 8) {
            double val;
            memcpy(&val, data, 8);
            m_reflection->SetDouble(m_message, fd, val);
        }
        break;

    case google::protobuf::FieldDescriptor::TYPE_FLOAT:
        if (size == 4) {
            float val;
            memcpy(&val, data, 4);
            m_reflection->SetFloat(m_message, fd, val);
        }
        break;

    case google::protobuf::FieldDescriptor::TYPE_INT64:
    case google::protobuf::FieldDescriptor::TYPE_SINT64:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED64: {
        int64_t val = 0;
        if (fds_get_int_be(data, size, &val) == FDS_OK) {
            m_reflection->SetInt64(m_message, fd, val);
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT64:
    case google::protobuf::FieldDescriptor::TYPE_FIXED64: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) == FDS_OK) {
            m_reflection->SetUInt64(m_message, fd, val);
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_INT32:
    case google::protobuf::FieldDescriptor::TYPE_SINT32:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED32: {
        int64_t val = 0;
        if (fds_get_int_be(data, size, &val) == FDS_OK) {
            m_reflection->SetInt32(m_message, fd, static_cast<int32_t>(val));
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT32:
    case google::protobuf::FieldDescriptor::TYPE_FIXED32: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) == FDS_OK) {
            m_reflection->SetUInt32(m_message, fd, static_cast<uint32_t>(val));
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_BOOL: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) == FDS_OK) {
            m_reflection->SetBool(m_message, fd, val != 0);
        }
        break;
    }

    case google::protobuf::FieldDescriptor::TYPE_STRING:
        // For strings, set directly from bytes
        m_reflection->SetString(m_message, fd,
                                std::string(reinterpret_cast<const char*>(data), size));
        break;

    case google::protobuf::FieldDescriptor::TYPE_BYTES:
        // For bytes, set directly
        m_reflection->SetString(m_message, fd,
                                std::string(reinterpret_cast<const char*>(data), size));
        break;

    case google::protobuf::FieldDescriptor::TYPE_ENUM: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) == FDS_OK) {
            m_reflection->SetEnumValue(m_message, fd, static_cast<int>(val));
        }
        break;
    }

    default:
        // Unsupported type - skip silently
        break;
    }
}

void
FlowConverter::extractPartitionKey(const fds_drec* rec, PartitionKey* key)
{
    key->valid = false;
    key->src_ip = nullptr;
    key->dst_ip = nullptr;
    key->src_port = 0;
    key->dst_port = 0;
    key->protocol = 0;

    struct fds_drec_field field;

    // Try IPv4 first
    if (fds_drec_find(const_cast<fds_drec*>(rec), IANA_PEN, ID_SRC_IPV4, &field) != FDS_EOC) {
        key->src_ip = field.data;
        key->src_ip_len = field.size;
    }
    if (fds_drec_find(const_cast<fds_drec*>(rec), IANA_PEN, ID_DST_IPV4, &field) != FDS_EOC) {
        key->dst_ip = field.data;
        key->dst_ip_len = field.size;
    }

    // Try IPv6 if IPv4 not found
    if (!key->src_ip) {
        if (fds_drec_find(const_cast<fds_drec*>(rec), IANA_PEN, ID_SRC_IPV6, &field) != FDS_EOC) {
            key->src_ip = field.data;
            key->src_ip_len = field.size;
        }
    }
    if (!key->dst_ip) {
        if (fds_drec_find(const_cast<fds_drec*>(rec), IANA_PEN, ID_DST_IPV6, &field) != FDS_EOC) {
            key->dst_ip = field.data;
            key->dst_ip_len = field.size;
        }
    }

    // Get ports
    if (fds_drec_find(const_cast<fds_drec*>(rec), IANA_PEN, ID_SRC_PORT, &field) != FDS_EOC) {
        if (field.size == 2) {
            key->src_port = ntohs(*reinterpret_cast<const uint16_t*>(field.data));
        }
    }
    if (fds_drec_find(const_cast<fds_drec*>(rec), IANA_PEN, ID_DST_PORT, &field) != FDS_EOC) {
        if (field.size == 2) {
            key->dst_port = ntohs(*reinterpret_cast<const uint16_t*>(field.data));
        }
    }

    // Get protocol
    if (fds_drec_find(const_cast<fds_drec*>(rec), IANA_PEN, ID_PROTOCOL, &field) != FDS_EOC) {
        if (field.size >= 1) {
            key->protocol = field.data[0];
        }
    }

    // Valid if we have at least one IP address
    key->valid = (key->src_ip != nullptr || key->dst_ip != nullptr);
}

} // namespace protobuf_kafka
