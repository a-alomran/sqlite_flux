// src/UpdateBuilder.cpp
#include "UpdateBuilder.h"
#include "ValueVisitor.h"
#include <sstream>
#include <stdexcept>
#include <algorithm>

namespace sqlite_flux
{

	// ============================================================================
	// Helper function to convert ColumnValue to SQL string (reuse from InsertBuilder)
	// ============================================================================

	static std::string valueToSql(const ColumnValue& value_)
	{
		std::ostringstream oss;

		std::visit(overloaded{
			[&oss](std::monostate) { oss << "NULL"; },
			[&oss](int64_t v) { oss << v; },
			[&oss](double v) { oss << v; },
			[&oss](const std::string& v) {
				oss << "'";
				// SQL escape - replace ' with ''
				for (char c : v)
				{
					if (c == '\'')
						oss << "''";
					else
						oss << c;
				} // end of for
				oss << "'";
			},
			[&oss](const std::vector<uint8_t>&) {
				oss << "?";  // BLOB needs parameter binding
			}
			}, value_);

		return oss.str();
	} // end of valueToSql

	// ============================================================================
	// Helper function to convert CompareOp to SQL string (reuse from QueryBuilder)
	// ============================================================================

	static std::string compareOpToString(CompareOp op_)
	{
		switch (op_)
		{
		case CompareOp::Equal:              return "=";
		case CompareOp::NotEqual:           return "!=";
		case CompareOp::LessThan:           return "<";
		case CompareOp::LessThanOrEqual:    return "<=";
		case CompareOp::GreaterThan:        return ">";
		case CompareOp::GreaterThanOrEqual: return ">=";
		case CompareOp::Like:               return "LIKE";
		case CompareOp::In:                 return "IN";
		default:                            return "=";
		} // end of switch
	} // end of compareOpToString

	// ============================================================================
	// PreparedUpdate Implementation
	// ============================================================================

	PreparedUpdate::PreparedUpdate(Analyzer& analyzer, const std::string& tableName,
		const std::unordered_map<std::string, ColumnValue>& updates)
		: analyzer_(analyzer), tableName_(tableName), updates_(updates)
	{
		// Begin transaction for batch operations
		if (!analyzer_.beginTransaction())
		{
			throw std::runtime_error("Failed to begin transaction for batch update");
		} // end of if
		inTransaction_ = true;
	} // end of PreparedUpdate constructor

	PreparedUpdate::~PreparedUpdate()
	{
		cleanup();
	} // end of PreparedUpdate destructor

	PreparedUpdate::PreparedUpdate(PreparedUpdate&& other) noexcept
		: analyzer_(other.analyzer_)
		, tableName_(std::move(other.tableName_))
		, updates_(std::move(other.updates_))
		, currentFilters_(std::move(other.currentFilters_))
		, inTransaction_(other.inTransaction_)
		, updateCount_(other.updateCount_)
		, batchSize_(other.batchSize_)
	{
		other.inTransaction_ = false;
	} // end of PreparedUpdate move constructor

	PreparedUpdate& PreparedUpdate::Where(const std::string& column_, const ColumnValue& value_,
		CompareOp op_)
	{
		currentFilters_.emplace_back(column_, value_, op_);
		return *this;
	} // end of Where

	void PreparedUpdate::ExecuteBatch()
	{
		// Build UPDATE statement
		std::ostringstream sql;
		sql << "UPDATE " << tableName_ << " SET ";

		// SET clause
		size_t index = 0;
		for (const auto& [col, val] : updates_)
		{
			if (index++ > 0) sql << ", ";
			sql << col << " = " << valueToSql(val);
		} // end of for

		// WHERE clause
		if (!currentFilters_.empty())
		{
			sql << " WHERE ";
			for (size_t i = 0; i < currentFilters_.size(); ++i)
			{
				if (i > 0) sql << " AND ";
				sql << currentFilters_[i].toSql();
			} // end of for
		} // end of if

		// Execute the update
		if (!analyzer_.execute(sql.str()))
		{
			throw std::runtime_error("Batch update failed: " + analyzer_.getLastError());
		} // end of if

		// Get rows affected (requires adding helper to Analyzer)
		auto result = analyzer_.query("SELECT changes() as affected");
		if (!result.empty())
		{
			auto affected = getValue<int64_t>(result[0], "affected");
			updateCount_ += affected.value_or(0);
		} // end of if

		// Clear current filters for next batch item
		currentFilters_.clear();

		// Auto-commit every N operations for better performance
		if (updateCount_ % batchSize_ == 0)
		{
			analyzer_.commit();
			analyzer_.beginTransaction();
		} // end of if
	} // end of ExecuteBatch

	int64_t PreparedUpdate::Finalize()
	{
		if (inTransaction_)
		{
			if (!analyzer_.commit())
			{
				analyzer_.rollback();
				throw std::runtime_error("Failed to commit batch update transaction");
			} // end of if
			inTransaction_ = false;
		} // end of if

		return updateCount_;
	} // end of Finalize

	void PreparedUpdate::cleanup()
	{
		if (inTransaction_)
		{
			analyzer_.rollback();
			inTransaction_ = false;
		} // end of if
	} // end of cleanup

	// ============================================================================
	// UpdateBuilder Implementation
	// ============================================================================

	UpdateBuilder::UpdateBuilder(Analyzer& analyzer, const std::string& tableName)
		: analyzer_(analyzer), tableName_(tableName)
	{
		// Get and cache table schema
		schema_ = analyzer_.getTableSchema(tableName_);

		if (schema_.empty())
		{
			throw std::runtime_error("Table not found or has no columns: " + tableName_);
		} // end of if
	} // end of UpdateBuilder constructor

	UpdateBuilder& UpdateBuilder::Set(const std::string& column_, const ColumnValue& value_)
	{
		// Validate column exists
		validateColumn(column_);

		// Validate type compatibility
		validateColumnType(column_, value_);

		// Add to updates
		updates_[column_] = value_;

		return *this;
	} // end of Set

	UpdateBuilder& UpdateBuilder::Where(const std::string& column_, const ColumnValue& value_,
		CompareOp op_)
	{
		// Validate column exists
		validateColumn(column_);

		// Validate type compatibility
		validateColumnType(column_, value_);

		// Add filter condition
		filters_.emplace_back(column_, value_, op_);

		return *this;
	} // end of Where

	UpdateBuilder& UpdateBuilder::Unsafe()
	{
		allowUnsafe_ = true;
		return *this;
	} // end of Unsafe

	int64_t UpdateBuilder::Execute()
	{
		if (updates_.empty())
		{
			throw std::runtime_error("No columns set for update");
		} // end of if

		// Safety check: prevent UPDATE without WHERE
		validateSafeExecution();

		std::string sql = buildSql();

		if (!analyzer_.execute(sql))
		{
			throw std::runtime_error("Update failed: " + analyzer_.getLastError());
		} // end of if

		// Get rows affected
		auto result = analyzer_.query("SELECT changes() as affected");
		if (!result.empty())
		{
			auto affected = getValue<int64_t>(result[0], "affected");
			return affected.value_or(0);
		} // end of if

		return 0;
	} // end of Execute

	PreparedUpdate UpdateBuilder::Prepare()
	{
		if (updates_.empty())
		{
			throw std::runtime_error("No columns set for prepared update");
		} // end of if

		return PreparedUpdate(analyzer_, tableName_, updates_);
	} // end of Prepare

	std::string UpdateBuilder::buildSql() const
	{
		std::ostringstream sql;

		sql << "UPDATE " << tableName_ << " SET ";

		// SET clause
		size_t index = 0;
		for (const auto& [col, val] : updates_)
		{
			if (index++ > 0) sql << ", ";
			sql << col << " = " << valueToSql(val);
		} // end of for

		// WHERE clause
		if (!filters_.empty())
		{
			sql << " WHERE ";
			for (size_t i = 0; i < filters_.size(); ++i)
			{
				if (i > 0) sql << " AND ";
				sql << filters_[i].toSql();
			} // end of for
		} // end of if

		return sql.str();
	} // end of buildSql

	void UpdateBuilder::validateColumn(const std::string& column_) const
	{
		auto it = std::find_if(schema_.begin(), schema_.end(),
			[&column_](const ColumnInfo& info) { return info.name == column_; });

		if (it == schema_.end())
		{
			throw std::runtime_error("Column '" + column_ + "' not found in table '" + tableName_ + "'");
		} // end of if
	} // end of validateColumn

	void UpdateBuilder::validateColumnType(const std::string& column_, const ColumnValue& value_) const
	{
		// Allow NULL for any column type
		if (std::holds_alternative<std::monostate>(value_))
		{
			return;
		} // end of if

		std::string sqliteType = getSqliteType(column_);

		if (!isTypeCompatible(sqliteType, value_))
		{
			std::string valueType;
			std::visit(overloaded{
				[&valueType](std::monostate) { valueType = "NULL"; },
				[&valueType](int64_t) { valueType = "int64_t"; },
				[&valueType](double) { valueType = "double"; },
				[&valueType](const std::string&) { valueType = "string"; },
				[&valueType](const std::vector<uint8_t>&) { valueType = "blob"; }
				}, value_);

			throw std::runtime_error(
				"Type mismatch for column_ '" + column_ + "': " +
				"expected " + sqliteType + ", got " + valueType
			);
		} // end of if
	} // end of validateColumnType

	std::string UpdateBuilder::getSqliteType(const std::string& column_) const
	{
		auto it = std::find_if(schema_.begin(), schema_.end(),
			[&column_](const ColumnInfo& info) { return info.name == column_; });

		if (it != schema_.end())
		{
			return it->type;
		} // end of if

		return "";
	} // end of getSqliteType

	bool UpdateBuilder::isTypeCompatible(const std::string& sqliteType, const ColumnValue& value_) const
	{
		// SQLite type affinity rules (same as InsertBuilder)
		// INTEGER affinity
		if (sqliteType.find("INT") != std::string::npos)
		{
			return std::holds_alternative<int64_t>(value_);
		} // end of if

		// TEXT affinity
		if (sqliteType.find("CHAR") != std::string::npos ||
			sqliteType.find("CLOB") != std::string::npos ||
			sqliteType.find("TEXT") != std::string::npos)
		{
			return std::holds_alternative<std::string>(value_);
		} // end of if

		// BLOB affinity
		if (sqliteType.find("BLOB") != std::string::npos)
		{
			return std::holds_alternative<std::vector<uint8_t>>(value_);
		} // end of if

		// REAL affinity
		if (sqliteType.find("REAL") != std::string::npos ||
			sqliteType.find("FLOA") != std::string::npos ||
			sqliteType.find("DOUB") != std::string::npos)
		{
			return std::holds_alternative<double>(value_);
		} // end of if

		// NUMERIC affinity - accepts both int and real
		return std::holds_alternative<int64_t>(value_) ||
			std::holds_alternative<double>(value_);
	} // end of isTypeCompatible

	void UpdateBuilder::validateSafeExecution() const
	{
		// Prevent UPDATE without WHERE clause unless explicitly allowed
		if (filters_.empty() && !allowUnsafe_)
		{
			throw std::runtime_error(
				"UPDATE without WHERE clause requires explicit .Unsafe() call to prevent accidental data loss. "
				"Use .Unsafe() if you really want to update all rows, or add .Where() conditions."
			);
		} // end of if
	} // end of validateSafeExecution

} // namespace sqlite_flux