/**
 * \file TranslationTable.hpp
 * \brief Pre-computed IPFIX to Protobuf field mapping table
 * \author Generated
 * \date 2026
 */

#ifndef PROTOBUF_KAFKA_TRANSLATIONTABLE_HPP
#define PROTOBUF_KAFKA_TRANSLATIONTABLE_HPP

#include "Config.hpp"
#include "ProtoSchema.hpp"

#include <vector>
#include <unordered_map>
#include <cstdint>

#include <google/protobuf/descriptor.h>
#include <libfds.h>

namespace protobuf_kafka {

/**
 * \brief Entry in the translation table
 *
 * Contains pre-resolved field descriptor for zero-allocation hot path.
 */
struct FieldEntry {
    const google::protobuf::FieldDescriptor* fd;  ///< Protobuf field descriptor
    std::string proto_name;                        ///< Field name (for debugging)
    fds_iemgr_element_type ipfix_type;            ///< IPFIX data type
};

/**
 * \brief Pre-computed translation table for IPFIX to Protobuf mapping
 *
 * Built during initialization phase. Provides O(1) lookup during processing.
 */
class TranslationTable {
public:
    TranslationTable() = default;

    /**
     * \brief Build the translation table
     *
     * Resolves all field mappings from IPFIX IDs to Protobuf FieldDescriptors.
     * Call this during plugin initialization (cold path).
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

    /**
     * \brief Fast lookup by IPFIX PEN and ID (hot path)
     *
     * \param pen  Private Enterprise Number
     * \param id   Information Element ID
     * \return Pointer to field entry, or nullptr if no mapping exists
     */
    const FieldEntry* lookup(uint32_t pen, uint16_t id) const;

    /**
     * \brief Get all configured IPFIX field identifiers
     *
     * Used for iterating over fields in the hot path.
     *
     * \return Vector of (PEN, ID) pairs
     */
    const std::vector<std::pair<uint32_t, uint16_t>>& ipfixIds() const {
        return m_ipfix_ids;
    }

    /**
     * \brief Get all field entries
     * \return Vector of field entries
     */
    const std::vector<FieldEntry>& entries() const { return m_entries; }

private:
    /// Create hash key from PEN and ID
    static uint64_t makeKey(uint32_t pen, uint16_t id) {
        return (static_cast<uint64_t>(pen) << 16) | id;
    }

    std::unordered_map<uint64_t, size_t> m_lookup;  ///< Key -> index in m_entries
    std::vector<FieldEntry> m_entries;               ///< Field entries
    std::vector<std::pair<uint32_t, uint16_t>> m_ipfix_ids;  ///< Ordered list of IDs
};

} // namespace protobuf_kafka

#endif // PROTOBUF_KAFKA_TRANSLATIONTABLE_HPP
