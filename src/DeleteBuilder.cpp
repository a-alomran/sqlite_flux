// src/DeleteBuilder.cpp
#include "DeleteBuilder.h"
#include "ValueVisitor.h"
#include <sstream>
#include <stdexcept>
#include <algorithm>

namespace sqlite_flux
{

	// ============================================================================
	// Helper function to convert CompareOp to SQL string
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
	// DeleteBuilder Implementation
	// ============================================================================

	DeleteBuilder::DeleteBuilder(Analyzer& analyzer, const std::string& tableName)
		: analyzer_(analyzer), tableName_(tableName)
	{
		// Get and cache table schema
		schema_ = analyzer_.getTableSchema(tableName_);

		if (schema_.empty())
		{
			throw std::runtime_error("Table not found or has no columns: " + tableName_);
		} // end of if
	} // end of DeleteBuilder constructor

	DeleteBuilder& DeleteBuilder::Where(const std::string& column_, const ColumnValue& value_,
		CompareOp op_)
	{
		// Validate column exists
		validateColumn(column_);

		// Add filter condition
		filters_.emplace_back(column_, value_, op_);

		return *this;
	} // end of Where

	DeleteBuilder& DeleteBuilder::OrderBy(const std::string& column_, bool ascending)
	{
		// Validate column exists
		validateColumn(column_);

		orderByColumn_ = column_;
		orderAscending_ = ascending;

		return *this;
	} // end of OrderBy

	DeleteBuilder& DeleteBuilder::Limit(int limit)
	{
		if (limit < 0)
		{
			throw std::invalid_argument("Limit must be non-negative");
		} // end of if

		limitValue_ = limit;
		return *this;
	} // end of Limit

	DeleteBuilder& DeleteBuilder::Unsafe()
	{
		allowUnsafe_ = true;
		return *this;
	} // end of Unsafe

	int64_t DeleteBuilder::Execute()
	{
		// Safety check: prevent DELETE without WHERE
		validateSafeExecution();

		std::string sql = buildSql();

		if (!analyzer_.execute(sql))
		{
			throw std::runtime_error("Delete failed: " + analyzer_.getLastError());
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

	std::string DeleteBuilder::buildSql() const
	{
		std::ostringstream sql;

		sql << "DELETE FROM " << tableName_;

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

		// ORDER BY clause (useful with LIMIT)
		if (!orderByColumn_.empty())
		{
			sql << " ORDER BY " << orderByColumn_;
			sql << (orderAscending_ ? " ASC" : " DESC");
		} // end of if

		// LIMIT clause
		if (limitValue_ > 0)
		{
			sql << " LIMIT " << limitValue_;
		} // end of if

		return sql.str();
	} // end of buildSql

	void DeleteBuilder::validateColumn(const std::string& column_) const
	{
		auto it = std::find_if(schema_.begin(), schema_.end(),
			[&column_](const ColumnInfo& info) { return info.name == column_; });

		if (it == schema_.end())
		{
			throw std::runtime_error("Column '" + column_ + "' not found in table '" + tableName_ + "'");
		} // end of if
	} // end of validateColumn

	void DeleteBuilder::validateSafeExecution() const
	{
		// Prevent DELETE without WHERE clause unless explicitly allowed
		if (filters_.empty() && !allowUnsafe_)
		{
			throw std::runtime_error(
				"DELETE without WHERE clause requires explicit .Unsafe() call to prevent accidental data loss. "
				"Use .Unsafe() if you really want to delete all rows, or add .Where() conditions."
			);
		} // end of if
	} // end of validateSafeExecution

} // namespace sqlite_flux