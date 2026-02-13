/**
 * \file TranslationTable.hpp
 * \brief Pre-computed IPFIX to Protobuf field mapping table
 * \author Jaroslav Pesek
 * \date 2026
 */

#ifndef PROTOBUF_KAFKA_TRANSLATIONTABLE_HPP
#define PROTOBUF_KAFKA_TRANSLATIONTABLE_HPP

#include "Config.hpp"
#include "ProtoSchema.hpp"

#include <vector>
#include <unordered_map>
#include <cstdint>
#include <functional>

#include <google/protobuf/descriptor.h>
#include <libfds.h>

namespace protobuf_kafka {

/**
 * \brief Entry in the translation table
 */
struct FieldEntry {
    const google::protobuf::FieldDescriptor* fd;  ///< Protobuf field descriptor
    std::string proto_name;                       ///< Field name (for debugging)
    std::string ipfix_spec;                       ///< Original IPFIX specification
    bool is_list = false;                         ///< True when mapping from basicList element
    fds_iemgr_element_type ipfix_type;            ///< Value element type (root for scalar, child for list)
};

/**
 * \brief Mapping lookup key
 *
 * For scalar fields, has_list_elem=false and child fields are ignored.
 * For basicList fields, child fields identify the list element definition.
 */
struct MappingKey {
    uint32_t root_pen = 0;
    uint16_t root_id = 0;
    bool has_list_elem = false;
    uint32_t list_pen = 0;
    uint16_t list_id = 0;

    bool operator==(const MappingKey& other) const noexcept {
        return root_pen == other.root_pen &&
               root_id == other.root_id &&
               has_list_elem == other.has_list_elem &&
               list_pen == other.list_pen &&
               list_id == other.list_id;
    }
};

/**
 * \brief Hash function for MappingKey
 */
struct MappingKeyHash {
    std::size_t operator()(const MappingKey& key) const noexcept {
        std::size_t h = static_cast<std::size_t>(key.root_pen);
        h ^= (static_cast<std::size_t>(key.root_id) << 1);
        h ^= (static_cast<std::size_t>(key.has_list_elem ? 0x9e37 : 0x79b9) << 1);
        h ^= (static_cast<std::size_t>(key.list_pen) << 2);
        h ^= (static_cast<std::size_t>(key.list_id) << 3);
        return h;
    }
};

/**
 * \brief Pre-computed translation table for IPFIX to Protobuf mapping
 */
class TranslationTable {
public:
    TranslationTable() = default;

    /**
     * \brief Build the translation table
     *
     * Resolves all field mappings from IPFIX IDs to Protobuf FieldDescriptors.
     *
     * \param mappings  Field mappings from configuration
     * \param schema    Loaded protobuf schema
     * \param iemgr     Information Element manager
     * \param ctx       Plugin context for logging
     * \throws std::runtime_error if a proto field is not found
     */
    void build(const std::vector<FieldMapping>& mappings,
               ProtoSchema& schema,
               const fds_iemgr_t* iemgr,
               ipx_ctx_t* ctx);

    /// Fast lookup by mapping key
    const FieldEntry* lookup(const MappingKey& key) const;

    /**
     * \brief Get all field entries
     * \return Vector of field entries
     */
    const std::vector<FieldEntry>& entries() const { return m_entries; }

private:
    std::unordered_map<MappingKey, size_t, MappingKeyHash> m_lookup;
    std::vector<FieldEntry> m_entries;
};

} // namespace protobuf_kafka

#endif // PROTOBUF_KAFKA_TRANSLATIONTABLE_HPP
