/**
 * \file FlowConverter.cpp
 * \brief To Protobuf converter
 * \author Jaroslav Pesek
 * \date 2026
 */

#include "FlowConverter.hpp"

#include <arpa/inet.h>
#include <cstring>

namespace protobuf_kafka {

FlowConverter::FlowConverter(ProtoSchema& schema,
                             const TranslationTable& table,
                             PartitionMode mode)
    : m_table(table)
    , m_partition_mode(mode)
    , m_message(nullptr)
    , m_reflection(nullptr)
{
    m_message = schema.createMessage();
    m_reflection = m_message->GetReflection();
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
    m_message->Clear();
    PartitionKey pk_local{};
    bool need_partition_key = (m_partition_mode == PartitionMode::RSS && partition_key);

    struct fds_drec_iter it;
    fds_drec_iter_init(&it, const_cast<struct fds_drec*>(rec), 0);

    int rc = FDS_OK;
    while ((rc = fds_drec_iter_next(&it)) != FDS_EOC) {
        if (rc != FDS_OK || it.field.info == nullptr) {
            continue;
        }

        const struct fds_tfield* info = it.field.info;
        MappingKey key;
        key.root_pen = info->en;
        key.root_id = info->id;

        const bool is_basic_list = (info->def != nullptr && info->def->data_type == FDS_ET_BASIC_LIST);
        if (is_basic_list) {
            struct fds_blist_iter list_it;
            fds_blist_iter_init(&list_it, &it.field, NULL);
            int list_rc = fds_blist_iter_next(&list_it);
            if (list_rc == FDS_OK && list_it.field.info != nullptr) {
                key.has_list_elem = true;
                key.list_pen = list_it.field.info->en;
                key.list_id = list_it.field.info->id;
            } else if (list_rc == FDS_ERR_FORMAT) {
                continue;
            }
        }

        const FieldEntry* entry = m_table.lookup(key);
        if (!entry && key.has_list_elem) {
            key.has_list_elem = false;
            key.list_pen = 0;
            key.list_id = 0;
            entry = m_table.lookup(key);
        }

        if (entry != nullptr) {
            bool converted = false;
            if (entry->is_list) {
                converted = setBasicListField(*entry, it.field);
            } else {
                converted = setFieldValue(*entry, it.field.data, it.field.size, false);
            }
            if (!converted) {
                continue;
            }
        }

        if (need_partition_key && info->en == IANA_PEN && !is_basic_list) {
            extractPartitionField(info->id, it.field.data, it.field.size, &pk_local);
        }
    }

    if (need_partition_key) {
        pk_local.valid = (pk_local.src_ip != nullptr || pk_local.dst_ip != nullptr);
        *partition_key = pk_local;
    }

    m_buffer.clear();
    if (!m_message->SerializeToString(&m_buffer)) {
        return false;
    }

    *out_data = m_buffer.data();
    *out_len = m_buffer.size();
    return true;
}

bool
FlowConverter::setFieldValue(const FieldEntry& entry, const uint8_t* data, size_t size, bool append)
{
    const google::protobuf::FieldDescriptor* fd = entry.fd;
    const bool use_append = append || fd->is_repeated();

    bool is_datetime = (entry.ipfix_type == FDS_ET_DATE_TIME_SECONDS ||
                        entry.ipfix_type == FDS_ET_DATE_TIME_MILLISECONDS ||
                        entry.ipfix_type == FDS_ET_DATE_TIME_MICROSECONDS ||
                        entry.ipfix_type == FDS_ET_DATE_TIME_NANOSECONDS);

    switch (fd->type()) {
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
        if (size == 8) {
            double val;
            memcpy(&val, data, 8);
            if (use_append) {
                m_reflection->AddDouble(m_message, fd, val);
            } else {
                m_reflection->SetDouble(m_message, fd, val);
            }
        }
        return true;

    case google::protobuf::FieldDescriptor::TYPE_FLOAT:
        if (size == 4) {
            float val;
            memcpy(&val, data, 4);
            if (use_append) {
                m_reflection->AddFloat(m_message, fd, val);
            } else {
                m_reflection->SetFloat(m_message, fd, val);
            }
        }
        return true;

    case google::protobuf::FieldDescriptor::TYPE_INT64:
    case google::protobuf::FieldDescriptor::TYPE_SINT64:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED64: {
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) == FDS_OK) {
                if (use_append) {
                    m_reflection->AddInt64(m_message, fd, static_cast<int64_t>(ts_ms));
                } else {
                    m_reflection->SetInt64(m_message, fd, static_cast<int64_t>(ts_ms));
                }
                return true;
            }
        } else {
            int64_t val = 0;
            if (fds_get_int_be(data, size, &val) == FDS_OK) {
                if (use_append) {
                    m_reflection->AddInt64(m_message, fd, val);
                } else {
                    m_reflection->SetInt64(m_message, fd, val);
                }
                return true;
            }
        }
        return false;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT64:
    case google::protobuf::FieldDescriptor::TYPE_FIXED64: {
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) == FDS_OK) {
                if (use_append) {
                    m_reflection->AddUInt64(m_message, fd, ts_ms);
                } else {
                    m_reflection->SetUInt64(m_message, fd, ts_ms);
                }
                return true;
            }
        } else {
            uint64_t val = 0;
            if (fds_get_uint_be(data, size, &val) == FDS_OK) {
                if (use_append) {
                    m_reflection->AddUInt64(m_message, fd, val);
                } else {
                    m_reflection->SetUInt64(m_message, fd, val);
                }
                return true;
            }
        }
        return false;
    }

    case google::protobuf::FieldDescriptor::TYPE_INT32:
    case google::protobuf::FieldDescriptor::TYPE_SINT32:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED32: {
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) == FDS_OK) {
                const int32_t value = static_cast<int32_t>(ts_ms / 1000);
                if (use_append) {
                    m_reflection->AddInt32(m_message, fd, value);
                } else {
                    m_reflection->SetInt32(m_message, fd, value);
                }
                return true;
            }
        } else {
            int64_t val = 0;
            if (fds_get_int_be(data, size, &val) == FDS_OK) {
                const int32_t value = static_cast<int32_t>(val);
                if (use_append) {
                    m_reflection->AddInt32(m_message, fd, value);
                } else {
                    m_reflection->SetInt32(m_message, fd, value);
                }
                return true;
            }
        }
        return false;
    }

    case google::protobuf::FieldDescriptor::TYPE_UINT32:
    case google::protobuf::FieldDescriptor::TYPE_FIXED32: {
        if (is_datetime) {
            uint64_t ts_ms = 0;
            if (fds_get_datetime_lp_be(data, size, entry.ipfix_type, &ts_ms) == FDS_OK) {
                const uint32_t value = static_cast<uint32_t>(ts_ms / 1000);
                if (use_append) {
                    m_reflection->AddUInt32(m_message, fd, value);
                } else {
                    m_reflection->SetUInt32(m_message, fd, value);
                }
                return true;
            }
        } else {
            uint64_t val = 0;
            if (fds_get_uint_be(data, size, &val) == FDS_OK) {
                const uint32_t value = static_cast<uint32_t>(val);
                if (use_append) {
                    m_reflection->AddUInt32(m_message, fd, value);
                } else {
                    m_reflection->SetUInt32(m_message, fd, value);
                }
                return true;
            }
        }
        return false;
    }

    case google::protobuf::FieldDescriptor::TYPE_BOOL: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) == FDS_OK) {
            const bool value = (val != 0);
            if (use_append) {
                m_reflection->AddBool(m_message, fd, value);
            } else {
                m_reflection->SetBool(m_message, fd, value);
            }
            return true;
        }
        return false;
    }

    case google::protobuf::FieldDescriptor::TYPE_STRING:
    case google::protobuf::FieldDescriptor::TYPE_BYTES:
    {
        std::string value(reinterpret_cast<const char*>(data), size);
        if (use_append) {
            m_reflection->AddString(m_message, fd, value);
        } else {
            m_reflection->SetString(m_message, fd, value);
        }
        return true;
    }

    case google::protobuf::FieldDescriptor::TYPE_ENUM: {
        uint64_t val = 0;
        if (fds_get_uint_be(data, size, &val) == FDS_OK) {
            const int enum_value = static_cast<int>(val);
            if (use_append) {
                m_reflection->AddEnumValue(m_message, fd, enum_value);
            } else {
                m_reflection->SetEnumValue(m_message, fd, enum_value);
            }
            return true;
        }
        return false;
    }

    default:
        return false;
    }

    return false;
}

bool
FlowConverter::setBasicListField(const FieldEntry& entry, const struct fds_drec_field& field)
{
    if (!entry.is_list || !entry.fd->is_repeated()) {
        return false;
    }

    struct fds_blist_iter list_it;
    fds_blist_iter_init(&list_it, const_cast<struct fds_drec_field*>(&field), NULL);

    int rc = FDS_OK;
    while ((rc = fds_blist_iter_next(&list_it)) == FDS_OK) {
        if (!setFieldValue(entry, list_it.field.data, list_it.field.size, true)) {
            return false;
        }
    }

    return rc == FDS_EOC;
}

void
FlowConverter::extractPartitionField(uint16_t id, const uint8_t* data, size_t size, PartitionKey* key)
{
    switch (id) {
    case ID_SRC_IPV4:
        if (!key->src_ip) {
            key->src_ip = data;
            key->src_ip_len = size;
        }
        break;
    case ID_DST_IPV4:
        if (!key->dst_ip) {
            key->dst_ip = data;
            key->dst_ip_len = size;
        }
        break;
    case ID_SRC_IPV6:
        if (!key->src_ip) {
            key->src_ip = data;
            key->src_ip_len = size;
        }
        break;
    case ID_DST_IPV6:
        if (!key->dst_ip) {
            key->dst_ip = data;
            key->dst_ip_len = size;
        }
        break;
    case ID_SRC_PORT:
        if (size == 2) {
            key->src_port = ntohs(*reinterpret_cast<const uint16_t*>(data));
        }
        break;
    case ID_DST_PORT:
        if (size == 2) {
            key->dst_port = ntohs(*reinterpret_cast<const uint16_t*>(data));
        }
        break;
    case ID_PROTOCOL:
        if (size >= 1) {
            key->protocol = data[0];
        }
        break;
    default:
        break;
    }
}

} // namespace protobuf_kafka
